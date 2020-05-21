package cas_test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"testing/quick"

	"context"

	"bazil.org/plop/cas"
	"gocloud.dev/blob/memblob"
)

var seed uint64

func init() {
	// keep this as uint64 just because negative numbers are uglier and can be confused with -opt
	flag.Uint64Var(&seed, "seed", 0, "seed to initialize random number generator")
}

type randReader struct {
	*rand.Rand
}

func (r randReader) Read(p []byte) (n int, err error) {
	for len(p) > 4 {
		binary.BigEndian.PutUint32(p, r.Uint32())
		n += 4
		p = p[4:]
	}
	if len(p) > 0 {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, r.Uint32())
		n += copy(p, buf)
	}
	return n, err
}

func NewRandReader(seed int64) randReader {
	src := rand.NewSource(seed)
	rnd := rand.New(src)
	return randReader{rnd}
}

type quietBytes []byte

func (q quietBytes) String() string {
	const trunc = 8
	if len(q) < trunc {
		return fmt.Sprintf("[%x]", []byte(q))
	}
	return fmt.Sprintf("[%.*x...]", trunc, []byte(q))
}

func (q quietBytes) GoString() string {
	// need both String and GoString; fmt.Sprintf defaults to String,
	// while testing/quick shows GoString form
	return q.String()
}

func TestQuickCompareRead(t *testing.T) {
	randR := NewRandReader(42)
	const size = 1 * 1024 * 1024
	buf := make([]byte, size)
	randR.Read(buf)

	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t",
		// cause extent crossings to happen
		cas.WithChunkLimits(size/100, size/10),
		cas.WithChunkGoal(size/50),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key, err := s.Create(ctx, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	casR := h.IO(ctx)

	readFn := func(r io.ReaderAt) func(length uint, off int64) (quietBytes, int, error) {
		fn := func(length uint, off int64) (quietBytes, int, error) {
			length = length % (size + 1000)
			p := make([]byte, length)
			if off < 0 {
				off = -off
			}
			off = off % (size + 1000)
			n, err := r.ReadAt(p, off)
			q := quietBytes(p[:n])
			t.Logf("ReadAt %d @%d -> %v %v", length, off, q, err)
			return q, n, err
		}
		return fn
	}
	got := readFn(casR)
	rat := bytes.NewReader(buf)
	exp := readFn(rat)

	if err := quick.CheckEqual(got, exp, nil); err != nil {
		t.Error(err)
	}
}
