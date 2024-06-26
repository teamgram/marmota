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

package strings2

import (
	"strconv"
	"strings"

	"github.com/teamgram/marmota/pkg/hack"
)

const (
	maxInt32Len  = 10
	maxInt64Len  = 19
	maxUint32Len = 10
	maxUint64Len = 20
)

// int64Len returns the number of characters needed to represent an int64 in decimal.
func int64Len(num int64) int {
	if num == 0 {
		return 1
	}
	length := 0
	if num < 0 {
		length++ // for the '-' sign
		num = -num
	}
	for num > 0 {
		length++
		num /= 10
	}
	return length
}

// int32Len returns the number of characters needed to represent an int32 in decimal.
func int32Len(num int32) int {
	if num == 0 {
		return 1
	}
	length := 0
	if num < 0 {
		length++ // for the '-' sign
		num = -num
	}
	for num > 0 {
		length++
		num /= 10
	}
	return length
}

// uint32Len returns the number of characters needed to represent a uint32 in decimal.
func uint32Len(num uint32) int {
	if num == 0 {
		return 1
	}
	length := 0
	for num > 0 {
		length++
		num /= 10
	}
	return length
}

// uint64Len returns the number of characters needed to represent a uint64 in decimal.
func uint64Len(num uint64) int {
	if num == 0 {
		return 1
	}
	length := 0
	for num > 0 {
		length++
		num /= 10
	}
	return length
}

func JoinInt32List(s []int32, sep string) string {
	l := len(s)
	if l == 0 {
		return ""
	}

	if l == 1 {
		return strconv.FormatInt(int64(s[0]), 10)
	}

	totalLen := (l - 1) * len(sep)
	// Calculate total length needed for the final string
	if l < 200000 {
		totalLen += l * maxInt32Len
	} else {
		for i := range s {
			totalLen += int32Len(s[i])
		}
	}

	buf := make([]byte, 0, totalLen)

	for i := 0; i < l-1; i++ {
		buf = strconv.AppendInt(buf, int64(s[i]), 10)
		buf = append(buf, sep...)
	}
	buf = strconv.AppendInt(buf, int64(s[l-1]), 10)

	return hack.String(buf)
}

func JoinUint32List(s []uint32, sep string) string {
	l := len(s)
	if l == 0 {
		return ""
	}

	if l == 1 {
		return strconv.FormatUint(uint64(s[0]), 10)
	}

	totalLen := (l - 1) * len(sep)
	// Calculate total length needed for the final string
	if l < 200000 {
		totalLen += l * maxUint32Len
	} else {
		for i := range s {
			totalLen += uint32Len(s[i])
		}
	}

	buf := make([]byte, 0, totalLen)

	for i := 0; i < l-1; i++ {
		buf = strconv.AppendUint(buf, uint64(s[i]), 10)
		buf = append(buf, sep...)
	}
	buf = strconv.AppendUint(buf, uint64(s[l-1]), 10)

	return hack.String(buf)
}

func JoinInt64List(s []int64, sep string) string {
	l := len(s)
	if l == 0 {
		return ""
	}

	if l == 1 {
		return strconv.FormatInt(s[0], 10)
	}

	totalLen := (l - 1) * len(sep)
	// Calculate total length needed for the final string
	if l < 200000 {
		totalLen += l * 6
	} else {
		for i := range s {
			totalLen += int64Len(s[i])
		}
	}

	buf := make([]byte, 0, totalLen)

	for i := 0; i < l-1; i++ {
		buf = strconv.AppendInt(buf, s[i], 10)
		buf = append(buf, sep...)
	}
	buf = strconv.AppendInt(buf, s[l-1], 10)

	return hack.String(buf)
}

func JoinUint64List(s []uint64, sep string) string {
	l := len(s)
	if l == 0 {
		return ""
	}

	if l == 1 {
		return strconv.FormatUint(s[0], 10)
	}

	totalLen := (l - 1) * len(sep)
	// Calculate total length needed for the final string
	if l < 200000 {
		totalLen += l * maxUint64Len
	} else {
		for i := range s {
			totalLen += uint64Len(s[i])
		}
	}

	buf := make([]byte, 0, totalLen)

	for i := 0; i < l-1; i++ {
		buf = strconv.AppendUint(buf, s[i], 10)
		buf = append(buf, sep...)
	}
	buf = strconv.AppendUint(buf, s[l-1], 10)

	return hack.String(buf)
}

// SplitInt32List split string into int32 slice.
func SplitInt32List(s, p string) ([]int32, error) {
	if s == "" {
		return nil, nil
	}
	sArr := strings.Split(s, p)
	res := make([]int32, 0, len(sArr))
	for _, sc := range sArr {
		i, err := strconv.ParseInt(sc, 10, 32)
		if err != nil {
			return nil, err
		}
		res = append(res, int32(i))
	}
	return res, nil
}

// SplitInt64List split string into int64 slice.
func SplitInt64List(s, p string) ([]int64, error) {
	if s == "" {
		return nil, nil
	}
	sArr := strings.Split(s, p)
	res := make([]int64, 0, len(sArr))
	for _, sc := range sArr {
		i, err := strconv.ParseInt(sc, 10, 64)
		if err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

// IsAlNumString returns true if an alpha numeric string consists of characters a-zA-Z0-9
//
// https://github.com/nebulaim/telegramd/issues/99
//
// there are some issue in username validation
// 1 - issue : username can be only numeric [must check first character for alphabet]
// 2 - issue :underscore not allowed [must edit IsAlNumString and add underscore support to it.
//
//	by the way underscore can't repeat without any character between them. for example s_a_b_a. ]
func IsAlNumString(s string) bool {
	c := 0
	prevtmp := ' '
	for _, r := range s {
		switch {
		case '0' <= r && r <= '9':
			c++
			break
		case 'a' <= r && r <= 'z':
			c++
			break
		case 'A' <= r && r <= 'Z':
			c++
			break
		case prevtmp != '_' && '_' == r:
			c++
			break
		}
		prevtmp = r
	}
	return len(s) == c
}
