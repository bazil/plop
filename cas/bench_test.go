package cas_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"bazil.org/plop/cas"
	"gocloud.dev/blob/memblob"
)

func benchmarkCreate(b *testing.B, size int) {
	b.ReportAllocs()
	b.SetBytes(int64(size))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	randR := NewRandReader(1337)
	buf := make([]byte, size)
	randR.Read(buf)

	bucket := memblob.OpenBucket(nil)
	s := cas.NewStore("s3kr1t", cas.WithBucket(bucket))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := bytes.NewReader(nil)
		for pb.Next() {
			r.Reset(buf)
			key, err := s.Create(ctx, r)
			if err != nil {
				b.Fatalf("create failed: %v", err)
			}
			_ = key
		}
	})
}

func BenchmarkCreate(b *testing.B) {
	run := func(name string, size int) {
		b.Run(name, func(b *testing.B) {
			benchmarkCreate(b, size)
		})
	}
	run("1 KiB", 1*1024)
	run("10 KiB", 10*1024)
	run("100 KiB", 100*1024)
	run("1 MiB", 1*1024*1024)
	run("10 MiB", 10*1024*1024)
}

func benchmarkRead(b *testing.B, size int) {
	b.ReportAllocs()
	b.SetBytes(int64(size))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	randR := NewRandReader(1337)
	buf := make([]byte, size)
	randR.Read(buf)

	bucket := memblob.OpenBucket(nil)
	s := cas.NewStore("s3kr1t", cas.WithBucket(bucket))
	key, err := s.Create(ctx, bytes.NewReader(buf))
	if err != nil {
		b.Fatalf("create failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h, err := s.Open(ctx, key)
			if err != nil {
				b.Fatalf("open failed: %v", err)
			}
			r := h.IO(ctx)
			n, err := io.Copy(io.Discard, r)
			if err != nil {
				b.Fatalf("read failed: %v", err)
			}
			if g, e := n, int64(size); g != e {
				b.Errorf("wrong read size: %v != %v", g, e)
			}
		}
	})
}

func BenchmarkRead(b *testing.B) {
	run := func(name string, size int) {
		b.Run(name, func(b *testing.B) {
			benchmarkRead(b, size)
		})
	}
	run("1 KiB", 1*1024)
	run("10 KiB", 10*1024)
	run("100 KiB", 100*1024)
	run("1 MiB", 1*1024*1024)
	run("10 MiB", 10*1024*1024)
}
