package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hashicorp/hcl/v2/hclsimple"
	"golang.org/x/sys/unix"
)

type Local struct {
	DefaultVolume string `hcl:"default_volume,optional"`
}

// mergeLocalConfig merges extra into dst, preferring values set in
// dst.
func mergeLocalConfig(dst, extra *Local) {
	if extra == nil {
		return
	}
	if dst.DefaultVolume == "" {
		dst.DefaultVolume = extra.DefaultVolume
	}
}

const localConfigFilename = ".plop.hcl"

func readOneLocalConfigAt(dirfd int) (*Local, error) {
	const filename = localConfigFilename
	fd, err := unix.Openat(dirfd, filename, unix.O_CLOEXEC|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		// ignore the error, keep climbing the tree
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), localConfigFilename)
	defer f.Close()
	src, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var local Local
	if err := hclsimple.Decode(filename, src, evalCtx, &local); err != nil {
		return nil, fmt.Errorf("cannot read local config: %w", err)
	}
	return &local, nil
}

// ReadLocalConfig reads any local configuration files in the current
// directory and above it. Files lower in the tree override files in
// parents.
func ReadLocalConfig() (*Local, error) {
	return readLocalConfigAt(unix.AT_FDCWD)
}

// readLocalConfig reads all local configuration files located at
// dirfd or above. If dirfd is not unix.AT_FDCWD, it will be closed.
func readLocalConfigAt(dirfd int) (*Local, error) {
	defer func() {
		if dirfd != unix.AT_FDCWD {
			unix.Close(dirfd)
		}
	}()
	var config Local
	var dirstat unix.Stat_t
	if err := unix.Fstatat(dirfd, "", &dirstat, unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, fmt.Errorf("cannot stat current directory: %w", err)
	}
	for {
		tmp, err := readOneLocalConfigAt(dirfd)
		if err != nil {
			return nil, err
		}
		mergeLocalConfig(&config, tmp)

		parentfd, err := unix.Openat(dirfd, "..", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOCTTY, 0)
		if err != nil {
			return nil, fmt.Errorf("cannot open parent directory: %w", err)
		}
		if dirfd != unix.AT_FDCWD {
			unix.Close(dirfd)
		}
		dirfd = parentfd

		var parentstat unix.Stat_t
		if err := unix.Fstatat(dirfd, "", &parentstat, unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return nil, fmt.Errorf("cannot stat current directory: %w", err)
		}
		if parentstat.Dev == dirstat.Dev && parentstat.Ino == dirstat.Ino {
			// ".." is the same as "." so we must be at root
			break
		}
		dirstat = parentstat
	}
	return &config, nil
}
