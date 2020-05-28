package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadLocalConfigAt(t *testing.T) {
	run := func(p string, want Local) {
		dir, err := os.Open(filepath.Join("testdata", p))
		if err != nil {
			t.Fatalf("cannot open local config dir: %v", err)
		}
		defer dir.Close()
		t.Run(p, func(t *testing.T) {
			got, err := readLocalConfigAt(int(dir.Fd()))
			if err != nil {
				t.Fatalf("readLocalConfigAt: %v", err)
			}
			if got == nil {
				t.Fatalf("unexpected nil local config")
			}
			if g, e := *got, want; g != e {
				t.Errorf("bad config: %+v != %+v", g, e)
			}
		})
	}
	run("one/two/three", Local{DefaultVolume: "three"})
	run("one/two", Local{DefaultVolume: "one"})
	run("one", Local{DefaultVolume: "one"})
}
