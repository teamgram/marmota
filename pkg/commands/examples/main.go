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

package main

import (
	"github.com/teamgram/marmota/pkg/commands"
	"github.com/zeromicro/go-zero/core/logx"
)

type exapmlesInstance struct {
	state int
	m     func()
}

func (e *exapmlesInstance) Initialize() error {
	logx.SetUp(logx.LogConf{
		ServiceName:         "examples",
		Mode:                "file",
		Encoding:            "json",
		TimeFormat:          "",
		Path:                "logs",
		Level:               "info",
		Compress:            false,
		KeepDays:            0,
		StackCooldownMillis: 100,
		MaxBackups:          0,
		MaxSize:             0,
		Rotation:            "daily",
	})
	logx.Info("null instance initialize")
	return nil
}

func (e *exapmlesInstance) RunLoop() {
	logx.Info("null run_loop...")
	// commands.GSignal <- syscall.SIGQUIT
	commands.DoExit()
}

func (e *exapmlesInstance) Destroy() {
	logx.Info("null destroy...")
}

func main() {
	commands.Run(&exapmlesInstance{})
}
