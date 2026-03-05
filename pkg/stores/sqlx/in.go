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

// escapeSingleQuote escapes single quotes for safe SQL string literal embedding.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func InStringList(elems []string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		escaped := escapeSingleQuote(elems[0])
		return "'" + escaped + "'"
	}

	var b strings.Builder
	b.WriteString("'")
	b.WriteString(escapeSingleQuote(elems[0]))
	b.WriteString("'")
	for _, s := range elems[1:] {
		b.WriteString(",'")
		b.WriteString(escapeSingleQuote(s))
		b.WriteString("'")
	}
	return b.String()
}
