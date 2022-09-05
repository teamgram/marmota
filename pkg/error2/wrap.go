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

package error2

import (
	"fmt"
	"runtime"
	"strings"
)

func getCaller(callDepth int) string {
	_, file, line, ok := runtime.Caller(callDepth)
	if !ok {
		return ""
	}

	return prettyCaller(file, line)
}

func prettyCaller(file string, line int) string {
	idx := strings.LastIndexByte(file, '/')
	if idx < 0 {
		return fmt.Sprintf("%s:%d", file, line)
	}

	idx = strings.LastIndexByte(file[:idx], '/')
	if idx < 0 {
		return fmt.Sprintf("%s:%d", file, line)
	}

	return fmt.Sprintf("%s:%d", file[idx+1:], line)
}

// Wrap returns an error that wraps err with given message.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("[%s] %s: %w", getCaller(2), message, err)
}

// Wrapf returns an error that wraps err with given format and args.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("[%s] %s: %w", getCaller(2), fmt.Sprintf(format, args...), err)
}
