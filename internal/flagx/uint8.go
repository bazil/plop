package flagx

import (
	"flag"
	"strconv"
)

type Uint8 uint8

var _ flag.Value = (*Uint8)(nil)

func (u *Uint8) String() string {
	return strconv.FormatUint(uint64(*u), 10)
}

func (u *Uint8) Set(s string) error {
	tmp, err := strconv.ParseUint(s, 10, 8)
	if err != nil {
		return err
	}
	*u = Uint8(tmp)
	return nil
}
