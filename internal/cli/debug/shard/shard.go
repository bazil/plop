package shard

import (
	"flag"
	"fmt"

	"bazil.org/plop/cas"
	"bazil.org/plop/internal/flagx"
	"github.com/tv42/cliutil/subcommands"
)

type shardCommand struct {
	subcommands.Description
	flag.FlagSet
	Flags struct {
		ShardBits uint8
	}
	Arguments struct {
		BoxedKey []string
	}
}

func (c *shardCommand) Run() error {
	for _, boxedKey := range c.Arguments.BoxedKey {
		prefix, err := cas.DebugShardPrefix(boxedKey, c.Flags.ShardBits)
		if err != nil {
			return err
		}
		fmt.Println(prefix)
	}
	return nil
}

var shard = shardCommand{
	Description: "show shard prefix for a plop object",
}

func init() {
	shard.Var((*flagx.Uint8)(&shard.Flags.ShardBits), "shard-bits", "how many bits to use as shard prefix")

	subcommands.Register(&shard)
}
