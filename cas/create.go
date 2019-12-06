package cas

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/restic/chunker"
	"github.com/tv42/zbase32"
	"golang.org/x/sync/errgroup"
)

type Writer struct {
	ctx   context.Context
	store *Store

	err    error
	chunkW *io.PipeWriter
	chunkG errgroup.Group

	// for use only inside chunker
	chunkR *io.PipeReader

	// written inside chunker, safe to read after chunkG.Wait
	extents bytes.Buffer
}

func newWriter(ctx context.Context, s *Store) *Writer {
	pr, pw := io.Pipe()
	w := &Writer{
		ctx:    ctx,
		store:  s,
		chunkW: pw,
		chunkR: pr,
	}
	w.chunkG.Go(w.chunker)
	return w
}

func (w *Writer) chunker() error {
	ch := chunker.New(w.chunkR, w.store.chunkerPolynomial)
	buf := make([]byte, 8*1024*1024)
	extent := make([]byte, 8+32)
	var offset uint64
	for {
		chunk, err := ch.Next(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		keyRaw, _, err := w.store.saveObject(w.ctx, prefixBlob, chunk.Data)
		if err != nil {
			return err
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
		_, _ = w.extents.Write(extent)
	}
	return nil
}

var _ io.Writer = (*Writer)(nil)

func (w *Writer) Write(p []byte) (int, error) {
	if err := w.err; err != nil {
		return 0, err
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.chunkW.Write(p)
}

// Abort prevents a Commit and releases resources.
// If Commit has already been called, does nothing.
func (w *Writer) Abort() {
	if w.err == ErrAlreadyCommitted || w.err == ErrAborted {
		return
	}
	w.err = ErrAborted
	_ = w.chunkW.CloseWithError(w.err)
}

func (w *Writer) Commit() (string, error) {
	if err := w.err; err != nil {
		return "", err
	}
	if err := w.ctx.Err(); err != nil {
		return "", err
	}

	_ = w.chunkW.Close()
	if err := w.chunkG.Wait(); err != nil {
		w.err = err
		return "", err
	}

	plaintext := w.extents.Bytes()
	keyRaw, _, err := w.store.saveObject(w.ctx, prefixExtents, plaintext)
	if err != nil {
		return "", err
	}

	key := zbase32.EncodeToString(keyRaw)

	w.err = ErrAlreadyCommitted
	return key, nil
}
