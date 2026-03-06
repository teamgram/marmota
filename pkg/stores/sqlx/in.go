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

// inBuckets defines fixed placeholder count tiers for IN clauses.
// By rounding up to these sizes, the SQL text stays stable across different
// list lengths, allowing MySQL to reuse prepared statement cache.
var inBuckets = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}

// nextBucket returns the smallest bucket size >= n.
// Falls back to n itself for very large lists.
func nextBucket(n int) int {
	for _, b := range inBuckets {
		if b >= n {
			return b
		}
	}
	return n
}

// placeholders returns a string of `n` comma-separated "?" markers, e.g. "?,?,?".
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n*2 - 1)
	b.WriteByte('?')
	for i := 1; i < n; i++ {
		b.WriteString(",?")
	}
	return b.String()
}

// BucketInt64List returns a bucketed placeholder string and padded args for use in
// parameterized IN clauses. The placeholder count is rounded up to a fixed bucket
// size so that MySQL can cache the prepared statement.
//
// Example:
//
//	ph, args := BucketInt64List([]int64{10, 20, 30})
//	// ph   = "?,?,?,?"   (bucket=4)
//	// args = [10, 20, 30, 30]  (padded with last element)
//	query := fmt.Sprintf("SELECT * FROM t WHERE id IN (%s)", ph)
//	db.Query(query, args...)
func BucketInt64List(elems []int64) (string, []interface{}) {
	if len(elems) == 0 {
		return "", nil
	}

	bucket := nextBucket(len(elems))
	args := make([]interface{}, bucket)
	for i := 0; i < len(elems); i++ {
		args[i] = elems[i]
	}
	// pad remaining slots with the last element (semantically neutral for IN)
	last := elems[len(elems)-1]
	for i := len(elems); i < bucket; i++ {
		args[i] = last
	}

	return placeholders(bucket), args
}

// BucketInt32List is the int32 variant of BucketInt64List.
func BucketInt32List(elems []int32) (string, []interface{}) {
	if len(elems) == 0 {
		return "", nil
	}

	bucket := nextBucket(len(elems))
	args := make([]interface{}, bucket)
	for i := 0; i < len(elems); i++ {
		args[i] = elems[i]
	}
	last := elems[len(elems)-1]
	for i := len(elems); i < bucket; i++ {
		args[i] = last
	}

	return placeholders(bucket), args
}

// BucketStringList is the string variant of BucketInt64List.
func BucketStringList(elems []string) (string, []interface{}) {
	if len(elems) == 0 {
		return "", nil
	}

	bucket := nextBucket(len(elems))
	args := make([]interface{}, bucket)
	for i := 0; i < len(elems); i++ {
		args[i] = elems[i]
	}
	last := elems[len(elems)-1]
	for i := len(elems); i < bucket; i++ {
		args[i] = last
	}

	return placeholders(bucket), args
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
