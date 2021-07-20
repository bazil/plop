package multierr_test

import (
	"errors"
	"testing"

	"bazil.org/plop/internal/multierr"
)

func TestToSlice(t *testing.T) {
	var (
		errOne = errors.New("one")
		errTwo = errors.New("two")
	)
	var err error = multierr.MultiErr([]error{errOne, errTwo})
	back := err.(multierr.MultiErr)
	t.Logf("back=%#v", back)
	if g, e := len(back), 2; g != e {
		t.Fatalf("bad lenght: %v != %v", g, e)
	}
	if g, e := back[0], errOne; g != e {
		t.Errorf("bad inner error: %v != %v", g, e)
	}
	if g, e := back[1], errTwo; g != e {
		t.Errorf("bad inner error: %v != %v", g, e)
	}
}
