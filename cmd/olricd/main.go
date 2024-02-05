// Copyright 2018-2024 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Server implementation for Olric. Olricd basically manages configuration for you.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olricd/server"
	"github.com/buraksezer/olric/config"
	"github.com/sean-/seed"
)

func usage() {
	var msg = `Usage: olricd [options] ...

Distributed cache and in-memory data structure server.

Options:
  -h, --help    Print this message and exit.
  -v, --version Print the version number and exit.
  -c, --config  Sets configuration file path. Default is olricd-local.yaml in the
                current folder. Set OLRICD_CONFIG to overwrite it.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues
`
	_, err := fmt.Fprintf(os.Stdout, msg, runtime.Version())
	if err != nil {
		panic(err)
	}
}

type arguments struct {
	config  string
	help    bool
	version bool
}

const (
	// DefaultConfigFile is the default configuration file path on a Unix-based operating system.
	DefaultConfigFile = "olricd-local.yaml"

	// EnvConfigFile is the name of environment variable which can be used to override default configuration file path.
	EnvConfigFile = "OLRICD_CONFIG"
)

func main() {
	args := &arguments{}

	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.BoolVar(&args.help, "h", false, "")
	f.BoolVar(&args.help, "help", false, "")

	f.BoolVar(&args.version, "version", false, "")
	f.BoolVar(&args.version, "v", false, "")

	f.StringVar(&args.config, "config", DefaultConfigFile, "")
	f.StringVar(&args.config, "c", DefaultConfigFile, "")

	if err := f.Parse(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, fmt.Sprintf("parsing error: %v\n", err))
		usage()
		os.Exit(1)
	}

	if args.version {
		_, _ = fmt.Fprintf(os.Stderr, "olricd version %s %s %s/%s\n",
			olric.ReleaseVersion,
			runtime.Version(),
			runtime.GOOS,
			runtime.GOARCH,
		)
		return
	} else if args.help {
		usage()
		return
	}

	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()

	envPath := os.Getenv(EnvConfigFile)
	if envPath != "" {
		args.config = envPath
	}

	c, err := config.Load(args.config)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to load the configuration file: %s: %v\n", args.config, err)
		os.Exit(1)
	}

	s, err := server.New(c)
	if err != nil {
		c.Logger.Fatalf("[ERROR] Failed to create a new Olric instance: %v", err)
	}

	if err = s.Start(); err != nil {
		c.Logger.Printf("[ERROR] Failed to start Olric: %v", err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			c.Logger.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
		c.Logger.Fatal("[ERROR] Quit unexpectedly!")
	}

	c.Logger.Print("[INFO] Quit!")
}
