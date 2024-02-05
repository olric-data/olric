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

package testutil

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func NewFlogger(c *config.Config) *flog.Logger {
	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	return flogger
}

func NewEngineConfig(t *testing.T) *config.Engine {
	e := config.NewEngine()
	require.NoError(t, e.Sanitize())
	require.NoError(t, e.Validate())
	return e
}

func NewConfig() *config.Config {
	c := config.New("local")
	c.PartitionCount = 7
	mc := memberlist.DefaultLocalConfig()
	mc.BindAddr = "127.0.0.1"
	mc.BindPort = 0
	c.MemberlistConfig = mc

	port, err := GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("GetFreePort returned an error: %v", err))
	}
	c.BindAddr = "127.0.0.1"
	c.BindPort = port
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	c.LeaveTimeout = 500 * time.Millisecond
	if err := c.Sanitize(); err != nil {
		panic(fmt.Sprintf("failed to sanitize default config: %v", err))
	}
	return c
}

func NewServer(c *config.Config) *server.Server {
	sc := &server.Config{
		BindAddr:        c.BindAddr,
		BindPort:        c.BindPort,
		KeepAlivePeriod: time.Second,
	}
	l := NewFlogger(c)
	return server.New(sc, l)
}

func TryWithInterval(max int, interval time.Duration, f func() error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var err error
	err = f()
	if err == nil {
		// Done. No need to try with interval
		return nil
	}

	var count = 1
	for count < max {
		<-ticker.C
		count++
		err = f()
		if err == nil {
			break
		}
	}
	return err
}

func ToKey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func ToVal(i int) []byte {
	return []byte(fmt.Sprintf("%010d", i))
}
