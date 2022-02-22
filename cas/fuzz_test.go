//go:build go1.18
// +build go1.18

package cas_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"bazil.org/plop/cas"
	"gocloud.dev/blob/memblob"
)

func FuzzRoundtrip(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte(""))
	f.Add([]byte("foo"))
	f.Fuzz(func(t *testing.T, data []byte) {
		b := memblob.OpenBucket(nil)
		s := cas.NewStore("s3kr1t", cas.WithBucket(b))

		ctx := context.Background()
		key, err := s.Create(ctx, bytes.NewReader(data))
		if err != nil {
			panic(fmt.Errorf("create error: %w", err))
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
			t.Fatal(fmt.Errorf("Read: %v", err))
		}
		if !bytes.Equal(buf, data) {
			t.Fatal("bad content")
		}
	})
}
