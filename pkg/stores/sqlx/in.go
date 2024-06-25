// Copyright © 2024 Teamgram Authors. All Rights Reserved.
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

package sqlx

import (
	"strings"

	"github.com/teamgram/marmota/pkg/strings2"
)

func InInt32List(elems []int32) string {
	return strings2.JoinInt32List(elems, ",")
}

func InUint32List(elems []uint32) string {
	return strings2.JoinUint32List(elems, ",")
}

func InInt64List(elems []int64) string {
	return strings2.JoinInt64List(elems, ",")
}

func InUint64List(elems []uint64) string {
	return strings2.JoinUint64List(elems, ",")
}

func InStringList(elems []string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return "'" + elems[0] + "'"
	}
	n := 2 + 3*(len(elems)-1) // "','" between elements
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString("'")
	b.WriteString(elems[0])
	b.WriteString("'")
	for _, s := range elems[1:] {
		b.WriteString(",'")
		b.WriteString(s)
		b.WriteString("'")
	}
	return b.String()
}
