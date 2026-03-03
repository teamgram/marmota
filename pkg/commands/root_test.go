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
	"sync"
	"testing"

	"github.com/zeromicro/go-zero/core/logx"
)

type NullInstance struct {
	state int
	m     func()
}

func (e *NullInstance) Initialize() error {
	logx.Info("null instance initialize...")
	e.state = 1
	return nil
}

func (e *NullInstance) RunLoop() {
	logx.Info("null run_loop...")
	e.state = 2
	e.m()
}

func (e *NullInstance) Destroy() {
	logx.Info("null destroy...")
	e.state = 3
}

func TestRun1(t *testing.T) {
	instance := &NullInstance{}

	result := instance.state
	expect := 0
	if result != expect {
		t.Error(`expect:`, expect, `result:`, result)
	}
}

func TestRun2(t *testing.T) {
	instance := &NullInstance{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	instance.m = func() {
		logx.Info("done...")
		wg.Done()
	}
	// Simulate RunLoop having run (state becomes 2 and m() is invoked)
	instance.RunLoop()
	wg.Wait()
	result := instance.state
	expect := 2
	if result != expect {
		t.Error(`expect:`, expect, `result:`, result)
	}
}

// testRunnerInstance is used by TestRunner_Shutdown to verify Runner.Run + Shutdown flow.
type testRunnerInstance struct {
	runLoopStarted chan struct{}
	destroyCh      chan struct{}
}

func (t *testRunnerInstance) Initialize() error { return nil }

func (t *testRunnerInstance) RunLoop() {
	close(t.runLoopStarted)
	<-make(chan struct{}) // block until process exits
}

func (t *testRunnerInstance) Destroy() {
	close(t.destroyCh)
}

func TestRunner_Shutdown(t *testing.T) {
	inst := &testRunnerInstance{
		runLoopStarted: make(chan struct{}),
		destroyCh:      make(chan struct{}),
	}
	r := NewRunner(inst)
	go r.Run()
	<-inst.runLoopStarted
	r.Shutdown()
	<-inst.destroyCh
}

func TestRunner_Instance(t *testing.T) {
	inst := &NullInstance{}
	r := NewRunner(inst)
	if r.Instance() != inst {
		t.Error("Instance() should return the same instance")
	}
}
