package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"

	"bazil.org/plop/cas"
	"bazil.org/plop/internal/config"
	"github.com/tv42/cliutil/subcommands"
	"gocloud.dev/blob"
)

type plop struct {
	flag.FlagSet
	Flags struct {
		Verbose    bool
		Debug      bool
		Config     string
		CPUProfile string
	}

	configOnce sync.Once
	config     *config.Config
	configErr  error
}

var _ Service = (*plop)(nil)

func (p *plop) Setup() (ok bool) {
	if p.Flags.CPUProfile != "" {
		f, err := os.Create(p.Flags.CPUProfile)
		if err != nil {
			log.Printf("cpu profiling: %v", err)
			return false
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Printf("cpu profiling: %v", err)
			return false
		}
	}
	return true
}

func (p *plop) Teardown() (ok bool) {
	if p.Flags.CPUProfile != "" {
		pprof.StopCPUProfile()
	}
	return true
}

func (p *plop) initConfig() {
	p.config, p.configErr = config.ReadConfig(p.Flags.Config)
}

func (p *plop) Config() (*config.Config, error) {
	p.configOnce.Do(p.initConfig)
	return p.config, p.configErr
}

// Volume returns the volume by the given name. If volume name is the
// empty string, default volume will be used.
func (p *plop) Volume(volumeName string) (*config.Volume, error) {
	cfg, err := p.Config()
	if err != nil {
		return nil, err
	}
	if volumeName != "" {
		vol, ok := cfg.GetVolume(volumeName)
		if !ok {
			return nil, fmt.Errorf("no such volume: %q", volumeName)
		}
		return vol, nil
	}
	vol, err := cfg.GetDefaultVolume()
	if err != nil {
		return nil, err
	}
	return vol, nil
}

// Store returns the CAS store for the given volume.
func (p *plop) Store(vol *config.Volume) (*cas.Store, error) {
	ctx := context.TODO()
	bucket, err := blob.OpenBucket(ctx, vol.Bucket.URL)
	if err != nil {
		return nil, err
	}
	var opts []cas.Option
	cfg, err := p.Config()
	if err != nil {
		return nil, err
	}
	opts = append(opts, cfg.Chunker.CASOptions()...)
	opts = append(opts, vol.Chunker.CASOptions()...)
	store := cas.NewStore(bucket, vol.Passphrase, opts...)
	return store, nil
}

// Plop allows command-line callables access to global flags, such as
// verbosity.
var Plop = plop{}

func init() {
	Plop.BoolVar(&Plop.Flags.Verbose, "v", false, "verbose output")
	Plop.BoolVar(&Plop.Flags.Debug, "debug", false, "debug output")

	defaultConfig := ""
	configDir, err := os.UserConfigDir()
	if err != nil {
		log.Printf("no default config: %v", err)
	} else {
		defaultConfig = filepath.Join(configDir, "plop", "config.hcl")
	}
	Plop.StringVar(&Plop.Flags.Config, "config", defaultConfig, "config file to read")
	Plop.StringVar(&Plop.Flags.CPUProfile, "cpuprofile", "", "write cpu profile to file")

	subcommands.Register(&Plop)
}

// Service is an interface that commands can implement to setup and
// teardown services for the subcommands below them.
//
// As Run and potential multiple Teardown failures makes having a
// single error return impossible, Setup and Teardown only get to
// signal a boolean success. Any detail should be exposed via log.
type Service interface {
	Setup() (ok bool)
	Teardown() (ok bool)
}

func run(result subcommands.Result) (ok bool) {
	var cmd interface{}
	for _, cmd = range result.ListCommands() {
		if svc, isService := cmd.(Service); isService {
			ok = svc.Setup()
			if !ok {
				return false
			}
			defer func() {
				// Teardown failures can cause non-successful exit
				if !svc.Teardown() {
					ok = false
				}
			}()
		}
	}
	run := cmd.(subcommands.Runner)
	err := run.Run()
	if err != nil {
		log.Printf("error: %v", err)
		return false
	}
	return true
}

// Main is primary entry point into the plop command line
// application.
func Main() (exitstatus int) {
	progName := filepath.Base(os.Args[0])
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	result, err := subcommands.Parse(&Plop, progName, os.Args[1:])
	if err == flag.ErrHelp {
		result.Usage()
		return 0
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", result.Name(), err)
		result.Usage()
		return 2
	}

	ok := run(result)
	if !ok {
		return 1
	}
	return 0
}
