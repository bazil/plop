package cas

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/tv42/zbase32"
	"gocloud.dev/gcerrors"
)

type Handle struct {
	store   *Store
	extents []byte
}

func newHandle(ctx context.Context, s *Store, key string) (*Handle, error) {
	hash, err := zbase32.DecodeString(key)
	if err != nil {
		return nil, ErrBadKey
	}
	if len(hash) != dataHashSize {
		return nil, ErrBadKey
	}
	extents, err := s.loadObject(ctx, prefixExtents, hash)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, ErrNotExist
		}
		return nil, err
	}
	if l := len(extents); l%(8+32) != 0 {
		return nil, fmt.Errorf("extents array is corrupted: len=%d", l)
	}
	h := &Handle{
		store:   s,
		extents: extents,
	}
	return h, nil
}

func (h *Handle) Size() int64 {
	if len(h.extents) == 0 {
		return 0
	}
	lastExtent := h.extents[len(h.extents)-extentSize:]
	off := extentOffset(lastExtent)
	return off
}

func (h *Handle) IO(ctx context.Context) *Reader {
	r := &Reader{
		handle: h,
		ctx:    ctx,
	}
	return r
}

type Reader struct {
	handle *Handle
	ctx    context.Context

	// current offset for Read calls, updated in a non-goroutine safe
	// way; Read must not be called concurrently
	readOffset int64
}

// getExtent returns the binary data for extent at idx.
//
// Caller is responsible for ensuring idx is valid.
func (r *Reader) getExtent(idx int) []byte {
	i := idx * extentSize
	return r.handle.extents[i : i+extentSize]
}

func extentOffset(extent []byte) int64 {
	off := binary.BigEndian.Uint64(extent[:8])
	if off > math.MaxInt64 {
		// can't report an error here, but this is a corrupt file
		return math.MaxInt64
	}
	return int64(off)
}

func extentHash(extent []byte) []byte {
	h := extent[8 : 8+dataHashSize]
	return h
}

var _ io.Reader = (*Reader)(nil)

func (r *Reader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := r.ReadAt(p, r.readOffset)
	r.readOffset += int64(n)
	return n, err
}

var _ io.ReaderAt = (*Reader)(nil)

func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	ext, err := r.ExtentAt(offset)
	if err != nil {
		return 0, err
	}
	n := 0
	// offset inside the extent
	off := offset - ext.Start()
	for {
		buf, err := ext.Bytes()
		if err != nil {
			return n, err
		}
		if int64(len(buf)) <= off {
			return n, ErrCorruptBlob
		}
		nn := copy(p, buf[off:])
		p = p[nn:]
		n += nn
		if len(p) == 0 {
			return n, nil
		}
		off = 0
		next, ok := ext.Next()
		if !ok {
			return n, io.EOF
		}
		ext = next
	}
	return n, nil
}

// ExtentAt returns the extent containing offset, or io.EOF if offset
// is outside of stored data.
//
// Note that the extent starting offset can be (and typically is)
// before the requested offset.
func (r *Reader) ExtentAt(offset int64) (*Extent, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	fn := func(i int) bool {
		ext := r.getExtent(i)
		off := extentOffset(ext)
		return off > offset
	}
	numExtents := len(r.handle.extents) / extentSize
	idx := sort.Search(numExtents, fn)
	if idx == numExtents {
		return nil, io.EOF
	}
	ext := &Extent{
		reader: r,
		idx:    idx,
	}
	return ext, nil
}

type Extent struct {
	reader *Reader
	idx    int
}

func (e *Extent) Start() int64 {
	if e.idx == 0 {
		return 0
	}
	// end offset of previous extent is our start
	prev := e.reader.getExtent(e.idx - 1)
	return extentOffset(prev)
}

func (e *Extent) Bytes() ([]byte, error) {
	hash := extentHash(e.reader.getExtent(e.idx))
	cacheKey := string(hash)
	e.reader.handle.store.cacheMu.Lock()
	cache, ok := e.reader.handle.store.cache.Get(cacheKey)
	e.reader.handle.store.cacheMu.Unlock()
	if ok {
		// cache hit
		buf := cache.([]byte)
		return buf, nil
	}

	// cache miss
	buf, err := e.reader.handle.store.loadObject(e.reader.ctx, prefixBlob, hash)
	if err != nil {
		return nil, err
	}
	e.reader.handle.store.cacheMu.Lock()
	e.reader.handle.store.cache.Set(cacheKey, buf)
	e.reader.handle.store.cacheMu.Unlock()
	return buf, nil
}

func (e *Extent) Next() (_ *Extent, ok bool) {
	idx := e.idx + 1
	numExtents := len(e.reader.handle.extents) / extentSize
	if idx == numExtents {
		return nil, false
	}
	ext := &Extent{
		reader: e.reader,
		idx:    idx,
	}
	return ext, true
}
