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

/*Package flog is a simple wrapper around Golang's log package which adds verbosity support.*/
package flog // import "github.com/buraksezer/olric/pkg/flog"

import (
	"fmt"
	"log"
	"path"
	"runtime"
	"sync/atomic"
)

/*
Derived from kubernetes/klog:
  * flog.V(1) - Generally useful for this to ALWAYS be visible to an operator
    * Programmer errors:
    * Logging extra info about a panic
    * CLI argument handling
  * flog.V(2) - A reasonable default log level if you don't want verbosity.
    * Information about config (listening on X, watching Y)
    * Errors that repeat frequently that relate to conditions that can be corrected (pod detected as unhealthy)
  * flog.V(3) - Useful steady state information about the service and important log messages that may correlate to
    significant changes in the system.  This is the recommended default log level for most systems.
    * Logging HTTP requests and their exit code
    * System state changing (killing pod)
    * Controller state change events (starting pods)
    * Scheduler log messages
  * flog.V(4) - Extended information about changes
    * More info about system state changes
  * flog.V(5) - Debug level verbosity
    * Logging in particularly thorny parts of code where you may want to come back later and check it
  * flog.V(6) - Trace level verbosity
    * Context to understand the steps leading up to errors and warnings
    * More information for troubleshooting reported issues

	The practical default level is V(2). Developers and QE environments may wish to run at V(3) or V(4).
*/

// A Logger represents an active logging instance that generates lines of
// output to an io.Writer. Each logging operation makes a single call to
// the Writer's Encode method. A Logger can be used simultaneously from
// multiple goroutines; it guarantees to serialize access to the Writer.
type Logger struct {
	logger      *log.Logger
	showLineNum int32
	level       int32
}

// New returns a new Logger
func New(logger *log.Logger) *Logger {
	return &Logger{
		logger: logger,
	}
}

// SetLevel sets verbosity level.
func (f *Logger) SetLevel(level int32) {
	if level < 0 {
		return
	}
	atomic.StoreInt32(&f.level, level)
}

// ShowLineNumber enables line number support if show is bigger than zero.
func (f *Logger) ShowLineNumber(show int32) {
	if show < 0 {
		return
	}
	atomic.StoreInt32(&f.showLineNum, show)
}

// Verbose is a type that implements Printf and Println with verbosity support.
type Verbose struct {
	ok bool
	f  *Logger
}

// V reports whether verbosity at the call site is at least the requested level. The returned value is a struct
// of type Verbose, which implements Printf and Println
func (f *Logger) V(level int32) Verbose {
	return Verbose{
		ok: atomic.LoadInt32(&f.level) >= level,
		f:  f,
	}
}

// Ok will return true if this log level is enabled, guarded by the value of verbosity level.
func (v Verbose) Ok() bool {
	return v.ok
}

// Printf calls v.f.logger.Printf to print to the logger.
// Arguments are handled in the manner of fmt.Printf.
func (v Verbose) Printf(format string, i ...interface{}) {
	if !v.ok {
		return
	}
	if atomic.LoadInt32(&v.f.showLineNum) != 1 {
		v.f.logger.Printf(format, i...)
	} else {
		_, fn, line, _ := runtime.Caller(1)
		v.f.logger.Printf(fmt.Sprintf("%s => %s:%d", format, path.Base(fn), line), i...)
	}
}

// Println calls v.f.logger.Println to print to the logger.
// Arguments are handled in the manner of fmt.Println.
func (v Verbose) Println(i ...interface{}) {
	if !v.ok {
		return
	}
	if atomic.LoadInt32(&v.f.showLineNum) != 1 {
		v.f.logger.Println(i...)
	} else {
		_, fn, line, _ := runtime.Caller(1)
		v.f.logger.Println(fmt.Sprintf("%s => %s:%d", fmt.Sprint(i...), path.Base(fn), line))
	}
}
