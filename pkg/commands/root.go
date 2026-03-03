// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: teamgramio (teamgram.io@gmail.com)
//

package commands

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/teamgram/marmota/pkg/version"

	"github.com/zeromicro/go-zero/core/logx"
)

var (
	defaultRunner *Runner
	ver           = flag.Bool("version", false, "prints current version")
)

// MainInstance is the interface that application main instances must implement.
type MainInstance interface {
	Initialize() error
	RunLoop()
	Destroy()
}

// Runner holds the main instance and signal channel for one application run.
// Use NewRunner and Run() to avoid package-level globals; Shutdown() triggers graceful exit.
type Runner struct {
	inst  MainInstance
	sigCh chan os.Signal
}

// NewRunner returns a Runner for the given main instance. The caller can
// call r.Run() (blocks until shutdown) and r.Shutdown() from anywhere that
// holds the runner (e.g. inject r.Shutdown into the instance).
func NewRunner(inst MainInstance) *Runner {
	return &Runner{
		inst:  inst,
		sigCh: make(chan os.Signal, 1),
	}
}

// Run runs the main loop: flag parse, version, init, RunLoop in goroutine, then signal loop.
// It blocks until a quit signal is received or Shutdown() is called.
func (r *Runner) Run() {
	flag.Parse()

	if *ver {
		v := version.GetVersion()
		fmt.Printf("Version: %s\nGitBranch: %s\nCommitId: %s\nBuild Date: %s\nGo Version: %s\nOS/Arch: %s\n", v.Version, v.GitBranch, v.GitCommit, v.BuildDate, v.GoVersion, v.Platform)
		os.Exit(0)
	}

	defer logx.Close()

	if r.inst == nil {
		panic(errors.New("inst is nil, exit"))
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	logx.Info("instance initialize...")
	err := r.inst.Initialize()
	logx.Info("inited")
	if err != nil {
		panic(err)
	}

	logx.Info("instance run_loop...")
	go r.inst.RunLoop()

	signal.Notify(r.sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-r.sigCh
		logx.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logx.Infof("instance exit...")
			r.inst.Destroy()
			time.Sleep(time.Second)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}

// Shutdown triggers graceful exit (sends SIGQUIT to the runner's signal loop).
// Safe to call from any goroutine (e.g. from RunLoop or an HTTP handler).
func (r *Runner) Shutdown() {
	select {
	case r.sigCh <- syscall.SIGQUIT:
	default:
		// already shutting down or signal channel full
	}
}

// Instance returns the main instance held by this runner. Prefer keeping a
// reference to your instance in application code instead of using this.
func (r *Runner) Instance() MainInstance {
	return r.inst
}

// Run is a compatibility entry point: it creates a Runner for inst, sets it as
// the default runner (so DoExit() works), then runs until shutdown. Prefer
// using NewRunner(inst) and r.Run() in new code so no package-level state is used.
func Run(inst MainInstance) {
	r := NewRunner(inst)
	defaultRunner = r
	defer func() { defaultRunner = nil }()
	r.Run()
}

// DoExit triggers graceful exit when the process was started with Run(inst).
// It has no effect if Run() was not used or has already returned. In new code,
// prefer holding the *Runner and calling r.Shutdown() so no global state is used.
func DoExit() {
	if defaultRunner != nil {
		defaultRunner.Shutdown()
	}
}
