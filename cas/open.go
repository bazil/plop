package cas

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/tv42/zbase32"
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

	extentAt     int
	extentOffset int
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

func (r *Reader) getExtentHashAt(byteOffset int) (hash []byte, ok bool) {
	if byteOffset > len(r.handle.extents)-extentSize {
		return nil, false
	}
	h := r.handle.extents[byteOffset+8 : byteOffset+8+dataHashSize]
	return h, true
}

var _ io.Reader = (*Reader)(nil)

func (r *Reader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	hash, ok := r.getExtentHashAt(r.extentAt)
	if !ok {
		return 0, io.EOF
	}
	buf, err := r.handle.store.loadObject(r.ctx, prefixBlob, hash)
	if err != nil {
		return 0, err
	}
	buf = buf[r.extentOffset:]
	n := copy(p, buf)
	if n == len(buf) {
		r.extentAt += extentSize
		r.extentOffset = 0
	} else {
		r.extentOffset += n
	}
	return n, nil
}

var _ io.ReaderAt = (*Reader)(nil)

func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	fn := func(i int) bool {
		ext := r.getExtent(i)
		off := extentOffset(ext)
		return off >= offset
	}
	numExtents := len(r.handle.extents) / extentSize
	idx := sort.Search(numExtents, fn)
	if idx == numExtents {
		return 0, io.EOF
	}
	n := 0
	for len(p) > 0 && idx < numExtents {
		extStart := int64(0)
		if idx > 0 {
			extStart = extentOffset(r.getExtent(idx - 1))
		}
		off := offset - extStart
		hash := extentHash(r.getExtent(idx))
		buf, err := r.handle.store.loadObject(r.ctx, prefixBlob, hash)
		if err != nil {
			return n, err
		}
		if int64(len(buf)) <= off {
			return n, ErrCorruptBlob
		}
		nn := copy(p, buf[off:])
		p = p[nn:]
		n += nn
		idx++
	}
	return n, nil
}
