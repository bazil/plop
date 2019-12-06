// +build gofuzz

package cas

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"gocloud.dev/blob/memblob"
)

func Fuzz(data []byte) int {
	b := memblob.OpenBucket(nil)
	s := NewStore(b, "s3kr1t")

	ctx := context.Background()
	w := s.Create(ctx)
	defer w.Abort()
	n, err := w.Write(data)
	if err != nil {
		panic(fmt.Errorf("write failed: %v", err))
	}
	if g, e := n, len(data); g != e {
		panic(fmt.Errorf("wrong length: %d != %%d", g, e))
	}
	key, err := w.Commit()
	if err != nil {
		panic(fmt.Errorf("commit error: %w", err))
	}

	h, err := s.Open(ctx, key)
	if err != nil {
		panic(fmt.Errorf("Open: %v", err))
	}
	if g, e := h.Size(), int64(len(data)); g != e {
		panic(fmt.Errorf("wrong length: %d != %d", g, e))
	}

	r := h.IO(ctx)
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		panic(fmt.Errorf("Read: %v", err))
	}
	if !bytes.Equal(buf, data) {
		panic("bad content")
	}

	return 0
}
