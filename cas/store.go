// Package cas stores data in in a backing blob store with
// content-addressed storage and convergent encryption. That is,
// objects with with identical contents (using the same secret) have
// identical blob key and ciphertext.
//
// Stored data is identified by a keyed hash of the plaintext
// data. Contents are encrypted with AEAD. All keys used are derived
// from the user-controlled secret by Argon2 KDFs.
//
// Limitations
//
// - No key rotation (at this level)
// - No garbage collection (at this level)
package cas

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"bazil.org/plop/internal/multierr"
	"bazil.org/plop/internal/multiflight"
	"cloud.google.com/go/storage"
	"github.com/dgryski/go-s4lru"
	"github.com/klauspost/compress/zstd"
	"github.com/restic/chunker"
	"github.com/tv42/zbase32"
	"github.com/zeebo/blake3"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

var (
	ErrBadKey      = errors.New("bad key")
	ErrNotExist    = errors.New("object does not exist")
	ErrCorruptBlob = errors.New("blob is corrupted")
)

type UnexpectedContentTypeError struct {
	ContentType string
}

var _ error = (*UnexpectedContentTypeError)(nil)

func (u *UnexpectedContentTypeError) Error() string {
	return fmt.Sprintf("unexpected Content-Type: %q", u.ContentType)
}

// Length of keys before encoding.
const dataHashSize = 32

const (
	// All of our stored objects are superficially the same type, you
	// only get to know the real type after opening the crypto.
	//
	// The version number at the end controls crypto algorithm and
	// plaintext content format.
	contentTypeV1 = "application/x.org.bazil.plop.v1"
)

const (
	prefixExtents = "bazil.org/plop#type/extents/v1\x00\x00"
	prefixBlob    = "bazil.org/plop#type/blob/v1\x00\x00\x00\x00\x00"
)

const extentSize = 8 + 32

func init() {
	// make sure we have nice 8-byte alignment
	if len(prefixExtents) != 32 {
		panic("bad definition of prefixExtents")
	}
	if len(prefixBlob) != 32 {
		panic("bad definition of prefixBlob")
	}
}

func kdf(secret, salt []byte, keySize uint32) []byte {
	return argon2.IDKey(secret, salt, 1, 64*1024, 4, keySize)
}

func newCipher(secret []byte) cipher.AEAD {
	c, err := chacha20poly1305.NewX(secret)
	if err != nil {
		panic("programmer error: chacha20poly1305.NewX: " + err.Error())
	}
	return c
}

type alternativeBucket struct {
	delay  time.Duration
	bucket *blob.Bucket
}

type config struct {
	chunkMin     uint32
	chunkMax     uint32
	chunkAvgBits int
	buckets      []alternativeBucket
}

type Store struct {
	config            config
	nameSecret        []byte
	hashSecret        []byte
	nonceSecret       []byte
	dataCipher        cipher.AEAD
	chunkerPolynomial chunker.Pol

	cacheMu sync.Mutex
	cache   *s4lru.Cache
}

func mustBlake3NewKeyed(key []byte) *blake3.Hasher {
	h, err := blake3.NewKeyed(key)
	if err != nil {
		panic(fmt.Errorf("programmer error: blake3.NewKeyed: %w", err))
	}
	return h
}

func blake3DeriveKeySized(context constantString, key []byte, size int) []byte {
	out := make([]byte, size)
	blake3.DeriveKey(string(context), key, out)
	return out
}

func NewStore(sharingPassphrase string, opts ...Option) *Store {
	// Salt for argon2 key derivation. This is obviously not secret
	// (and cannot be), but it does force any attackers to attack this
	// software specifically and not rely on existing rainbow tables.
	const sharingSalt = "bazil.org/plop 2020-04-07 sharing salt"
	sharingSecret := kdf(
		[]byte(sharingPassphrase),
		[]byte(sharingSalt),
		32,
	)
	blobSecret := blake3DeriveKeySized(
		"bazil.org/plop 2020-04-07 blob cipher",
		sharingSecret,
		chacha20poly1305.KeySize,
	)
	// same chunker for everything using this sharing secret, to
	// actually enable deduplication
	chunkerPolHash := blake3.NewDeriveKey("bazil.org/plop 2020-04-07 rolling hash polynomial")
	_, _ = chunkerPolHash.Write(sharingSecret)
	chunkerPolynomial, err := chunker.DerivePolynomial(
		chunkerPolHash.Digest(),
	)
	if err != nil {
		// this should be very very rare
		panic("cannot derive chunker polynomial")
	}
	const MiB = 1024 * 1024
	s := &Store{
		config: config{
			chunkMin:     4 * MiB,
			chunkMax:     16 * MiB,
			chunkAvgBits: 23, // 8 MiB
		},
		nameSecret: blake3DeriveKeySized(
			"bazil.org/plop 2020-04-07 object name boxing",
			sharingSecret,
			32,
		),
		hashSecret: blake3DeriveKeySized(
			"bazil.org/plop 2020-04-07 blob hash for id",
			sharingSecret,
			32,
		),
		nonceSecret: blake3DeriveKeySized(
			"bazil.org/plop 2020-04-07 blob hash for nonce",
			sharingSecret,
			32,
		),
		dataCipher:        newCipher(blobSecret),
		chunkerPolynomial: chunkerPolynomial,

		// Cache objects are typically ~8MB, so keep it small. All we
		// need is to serve smaller-than-extent reads concurrently for
		// a couple of different streams.
		//
		// Ideally, cache size should configured in bytes and we
		// should adjust number of items based on average chunk size
		// (which we technically don't know up front; goal varies by
		// volume and blobs may have been written with different
		// tunables).
		//
		// Also note there are multiple Stores open; this should
		// probably be shared (not sure if intermingling keys is a
		// good idea; maybe switch key from string to struct, with
		// Store pointer).
		//
		// Must be divisible by 4.
		cache: s4lru.New(4 * 10),
	}
	for _, opt := range opts {
		opt(&s.config)
	}
	if len(s.config.buckets) == 0 {
		panic("cas.NewStore must have at least one bucket")
	}
	return s
}

type constantString string

func (s *Store) hashData(prefix constantString, data []byte) []byte {
	h := mustBlake3NewKeyed(s.hashSecret)
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write(data)
	hash := make([]byte, dataHashSize)
	_, _ = h.Digest().Read(hash)
	return hash
}

func (s *Store) nonce(hash []byte) []byte {
	h := mustBlake3NewKeyed(s.nonceSecret)
	_, _ = h.Write(hash)
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	_, _ = h.Digest().Read(nonce)
	return nonce
}

func (s *Store) boxKey(key []byte) []byte {
	h := mustBlake3NewKeyed(s.nameSecret)
	_, _ = h.Write(key)
	boxedKey := h.Sum(nil)
	return boxedKey
}

func (s *Store) uploadToBackend(ctx context.Context, bucket *blob.Bucket, boxedKey string, data []byte) error {
	hasCreateIfNotExist := false
	if gcsClient := (*storage.Client)(nil); bucket.As(&gcsClient) {
		hasCreateIfNotExist = true
	}
	if !hasCreateIfNotExist {
		// object bodies smaller than this aren't worth a separate
		// request to avoid transferring
		const preflightSize = 1 * 1024 * 1024
		if len(data) > preflightSize {
			// do a HEAD to avoid transferring data over and over
			//
			// With multiple alternative buckets, this can still cause
			// some duplication (and is skipped for small objects,
			// anyway).
			exists, err := bucket.Exists(ctx, boxedKey)
			if err != nil {
				return err
			}
			if exists {
				return nil
			}
		}
	}

	opts := &blob.WriterOptions{
		CacheControl:    "public, max-age=2147483648, immutable",
		ContentEncoding: "identity",
		ContentType:     contentTypeV1,
		BeforeWrite: func(as func(interface{}) bool) error {
			// do not add more preconditions without considering the
			// error checking below
			var obj **storage.ObjectHandle
			if as(&obj) {
				*obj = (*obj).If(storage.Conditions{
					// if not exist
					GenerationMatch: 0,
				})
			}
			return nil
		},
	}
	if err := bucket.WriteAll(ctx, boxedKey, data, opts); err != nil {
		switch gcerrors.Code(err) {
		case gcerrors.AlreadyExists:
			return nil
		case gcerrors.FailedPrecondition:
			return nil
		}
		return fmt.Errorf("object write: %w", err)
	}
	return nil
}

func (s *Store) downloadFromBackend(ctx context.Context, bucket *blob.Bucket, boxedKey string, prefix constantString) ([]byte, error) {
	opts := &blob.ReaderOptions{
		// TODO BeforeRead
	}
	br, err := bucket.NewReader(ctx, boxedKey, opts)
	if err != nil {
		return nil, fmt.Errorf("object read open: %w", err)
	}
	defer br.Close()
	switch ct := br.ContentType(); ct {
	default:
		err := &UnexpectedContentTypeError{
			ContentType: ct,
		}
		return nil, err

	case contentTypeV1:
		return s._downloadFromBackendV1(prefix, br)
	}
}

func (s *Store) _downloadFromBackendV1(prefix constantString, br *blob.Reader) ([]byte, error) {
	size := br.Size()
	const maxInt = int(^uint(0) >> 1)
	if size > int64(maxInt) {
		return nil, fmt.Errorf("object is too large: %d", size)
	}
	buf := bytes.NewBuffer(make([]byte, 0, int(size)))
	if _, err := br.WriteTo(buf); err != nil {
		return nil, fmt.Errorf("object read: %w", err)
	}
	return buf.Bytes(), nil
}

// not using Pool.New because zstd.NewWriter can return an error
var zstdEncoders sync.Pool

func (s *Store) saveObject(ctx context.Context, prefix constantString, plaintext []byte) (key []byte, boxedKey string, _ error) {
	hash := s.hashData(prefix, plaintext)
	nonce := s.nonce(hash)
	var zbuf bytes.Buffer
	// put prefix inside crypto but in front of compression
	_, _ = zbuf.WriteString(string(prefix))
	// not using EncodeAll because our data might be big enough to
	// benefit from parallelism
	zw, ok := zstdEncoders.Get().(*zstd.Encoder)
	if ok {
		zw.Reset(&zbuf)
	} else {
		tmp, err := zstd.NewWriter(&zbuf,
			zstd.WithEncoderPadding(32),
		)
		if err != nil {
			return nil, "", fmt.Errorf("zstd error: %w", err)
		}
		zw = tmp
	}
	if _, err := zw.Write(plaintext); err != nil {
		return nil, "", fmt.Errorf("zstd write: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, "", fmt.Errorf("zstd close: %w", err)
	}
	zstdEncoders.Put(zw)
	compressed := zbuf.Bytes()
	ciphertext := s.dataCipher.Seal(compressed[:0], nonce, compressed, hash)

	boxedKeyRaw := s.boxKey(hash)
	boxedKey = zbase32.EncodeToString(boxedKeyRaw)

	m := multiflight.New()
	for _, alt := range s.config.buckets {
		bucket := alt.bucket
		upload := func(ctx context.Context) (interface{}, error) {
			if err := s.uploadToBackend(ctx, bucket, boxedKey, ciphertext); err != nil {
				return nil, err
			}
			return nil, nil
		}
		m.Add(alt.delay, upload)
	}
	if _, err := m.Run(ctx); err != nil {
		return nil, "", err
	}
	return hash, boxedKey, nil
}

func (s *Store) loadObject(ctx context.Context, prefix constantString, hash []byte) ([]byte, error) {
	boxedKeyRaw := s.boxKey(hash)
	boxedKey := zbase32.EncodeToString(boxedKeyRaw)

	m := multiflight.New()
	for _, alt := range s.config.buckets {
		bucket := alt.bucket
		download := func(ctx context.Context) (interface{}, error) {
			ciphertext, err := s.downloadFromBackend(ctx, bucket, boxedKey, prefix)
			if err != nil {
				return nil, err
			}
			return ciphertext, nil
		}
		m.Add(alt.delay, download)
	}
	result, err := m.Run(ctx)
	if err != nil {
		return nil, err
	}
	ciphertext := result.([]byte)
	nonce := s.nonce(hash)
	compressed, err := s.dataCipher.Open(ciphertext[:0], nonce, ciphertext, hash)
	if err != nil {
		return nil, fmt.Errorf("box open: %w", err)
	}

	// check prefix
	if !bytes.HasPrefix(compressed, []byte(prefix)) {
		idx := bytes.IndexByte(compressed, 0)
		if idx < 0 {
			idx = 0
		}
		if idx > len(prefix) {
			idx = len(prefix)
		}
		return nil, fmt.Errorf("wrong prefix: %q", compressed[:idx])
	}
	compressed = compressed[len(prefix):]

	// uncompress
	zr, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("zstd error: %w", err)
	}
	defer zr.Close()
	// not using DecodeAll because our data might be big enough to
	// benefit from parallelism
	var zbuf bytes.Buffer
	if _, err := zr.WriteTo(&zbuf); err != nil {
		return nil, fmt.Errorf("zstd read: %w", err)
	}
	return zbuf.Bytes(), nil
}

func (s *Store) loadObjectCached(ctx context.Context, prefix constantString, hash []byte) ([]byte, error) {
	cacheKey := string(hash)
	s.cacheMu.Lock()
	cache, ok := s.cache.Get(cacheKey)
	s.cacheMu.Unlock()
	if ok {
		// cache hit
		buf := cache.([]byte)
		return buf, nil
	}

	// cache miss
	buf, err := s.loadObject(ctx, prefix, hash)
	if err != nil {
		return nil, err
	}
	s.cacheMu.Lock()
	s.cache.Set(cacheKey, buf)
	s.cacheMu.Unlock()
	return buf, nil
}

func (s *Store) Create(ctx context.Context, r io.Reader) (string, error) {
	var extents bytes.Buffer
	ch := chunker.NewWithBoundaries(r, s.chunkerPolynomial,
		// uint32 to uint is always safe
		uint(s.config.chunkMin), uint(s.config.chunkMax))
	ch.SetAverageBits(s.config.chunkAvgBits)
	buf := make([]byte, 8*1024*1024)
	extent := make([]byte, 8+32)
	var offset uint64
	for {
		chunk, err := ch.Next(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		keyRaw, _, err := s.saveObject(ctx, prefixBlob, chunk.Data)
		if err != nil {
			return "", err
		}
		// First extent always starts at 0, so store *end offset* in
		// extents. This means last extent tells us length of file.
		//
		// A file of size 0 will not have any extents.
		//
		// A file of size 1 will have extent with endOffset=1.
		//
		// TODO also store size in symlink target?
		offset += uint64(len(chunk.Data))
		binary.BigEndian.PutUint64(extent[:8], offset)
		if n := copy(extent[8:], keyRaw); n != len(extent)-8 {
			panic("extent key length error")
		}
		_, _ = extents.Write(extent)
	}

	plaintext := extents.Bytes()
	keyRaw, _, err := s.saveObject(ctx, prefixExtents, plaintext)
	if err != nil {
		return "", err
	}
	key := zbase32.EncodeToString(keyRaw)
	return key, nil
}

func (s *Store) Open(ctx context.Context, key string) (*Handle, error) {
	return newHandle(ctx, s, key)
}

func (s *Store) DebugReadBlob(ctx context.Context, blobKey string) ([]byte, error) {
	hash, err := zbase32.DecodeString(blobKey)
	if err != nil {
		return nil, ErrBadKey
	}
	if len(hash) != dataHashSize {
		return nil, ErrBadKey
	}
	buf, err := s.loadObjectCached(ctx, prefixBlob, hash)
	if err != nil {
		isNotFound := func(err error) bool {
			return gcerrors.Code(err) == gcerrors.NotFound
		}
		if multierr.All(err, isNotFound) {
			return nil, ErrNotExist
		}
		return nil, err
	}
	return buf, nil
}

func (s *Store) DebugBoxKey(key string) (string, error) {
	hash, err := zbase32.DecodeString(key)
	if err != nil {
		return "", ErrBadKey
	}
	if len(hash) != dataHashSize {
		return "", ErrBadKey
	}
	boxed := s.boxKey(hash)
	boxedKey := zbase32.EncodeToString(boxed)
	return boxedKey, nil
}
