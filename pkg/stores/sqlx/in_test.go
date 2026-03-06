// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package sqlx

// 单元测试函数
// go test -v
// go test -v -run TestContainsInt
// go test -v -run TestContainsInt -bench=.
// go test -v -run TestContainsInt -bench=. -benchmem
// go test -v -run TestContainsInt -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof -blockprofile=block.prof

import (
	"testing"
)

// InInt32List()函数的测试函数
func TestInInt32List(t *testing.T) {
	// 准备测试数据
	elems := []int32{1, 2, 3, 4, 5}
	// 调用测试函数
	result := InInt32List(elems)
	// 判断测试结果
	if result != "1,2,3,4,5" {
		t.Errorf("InInt32List() = %s; expected 1,2,3,4,5", result)
	}
}

// InUint32List()函数的测试函数
func TestInUint32List(t *testing.T) {
	// 准备测试数据
	elems := []uint32{1, 2, 3, 4, 5}
	// 调用测试函数
	result := InUint32List(elems)
	// 判断测试结果
	if result != "1,2,3,4,5" {
		t.Errorf("InUint32List() = %s; expected 1,2,3,4,5", result)
	}
}

// InInt64List()函数的测试函数
func TestInInt64List(t *testing.T) {
	// 准备测试数据
	elems := []int64{1, 2, 3, 4, 5}
	// 调用测试函数
	result := InInt64List(elems)
	// 判断测试结果
	if result != "1,2,3,4,5" {
		t.Errorf("InInt64List() = %s; expected 1,2,3,4,5", result)
	}
}

// InUint64List()函数的测试函数
func TestInUint64List(t *testing.T) {
	// 准备测试数据
	elems := []uint64{1, 2, 3, 4, 5}
	// 调用测试函数
	result := InUint64List(elems)
	// 判断测试结果
	if result != "1,2,3,4,5" {
		t.Errorf("InUint64List() = %s; expected 1,2,3,4,5", result)
	}
}

func TestNextBucket(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{8, 8},
		{9, 16},
		{17, 32},
		{33, 64},
		{65, 128},
		{129, 256},
		{257, 512},
		{513, 513}, // beyond max bucket, returns n itself
	}
	for _, tt := range tests {
		got := nextBucket(tt.n)
		if got != tt.want {
			t.Errorf("nextBucket(%d) = %d; want %d", tt.n, got, tt.want)
		}
	}
}

func TestPlaceholders(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, ""},
		{1, "?"},
		{2, "?,?"},
		{4, "?,?,?,?"},
	}
	for _, tt := range tests {
		got := placeholders(tt.n)
		if got != tt.want {
			t.Errorf("placeholders(%d) = %q; want %q", tt.n, got, tt.want)
		}
	}
}

func TestBucketInt64List(t *testing.T) {
	tests := []struct {
		name     string
		elems    []int64
		wantPH   string
		wantArgs []interface{}
	}{
		{
			name:     "empty",
			elems:    []int64{},
			wantPH:   "",
			wantArgs: nil,
		},
		{
			name:     "single element, bucket=1",
			elems:    []int64{42},
			wantPH:   "?",
			wantArgs: []interface{}{int64(42)},
		},
		{
			name:     "3 elements, bucket=4, padded",
			elems:    []int64{10, 20, 30},
			wantPH:   "?,?,?,?",
			wantArgs: []interface{}{int64(10), int64(20), int64(30), int64(30)},
		},
		{
			name:     "4 elements, bucket=4, exact fit",
			elems:    []int64{1, 2, 3, 4},
			wantPH:   "?,?,?,?",
			wantArgs: []interface{}{int64(1), int64(2), int64(3), int64(4)},
		},
		{
			name:     "5 elements, bucket=8",
			elems:    []int64{1, 2, 3, 4, 5},
			wantPH:   "?,?,?,?,?,?,?,?",
			wantArgs: []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(5), int64(5), int64(5)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ph, args := BucketInt64List(tt.elems)
			if ph != tt.wantPH {
				t.Errorf("placeholders = %q; want %q", ph, tt.wantPH)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args len = %d; want %d", len(args), len(tt.wantArgs))
			}
			for i, a := range args {
				if a != tt.wantArgs[i] {
					t.Errorf("args[%d] = %v; want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

// InStringList()函数的测试函数
func TestInStringList(t *testing.T) {
	tests := []struct {
		name     string
		elems    []string
		expected string
	}{
		{
			name:     "normal",
			elems:    []string{"1", "2", "3", "4", "5"},
			expected: "'1','2','3','4','5'",
		},
		{
			name:     "empty",
			elems:    []string{},
			expected: "",
		},
		{
			name:     "single",
			elems:    []string{"hello"},
			expected: "'hello'",
		},
		{
			name:     "single quote escape",
			elems:    []string{"it's", "test"},
			expected: "'it''s','test'",
		},
		{
			name:     "sql injection attempt",
			elems:    []string{"'; DROP TABLE users; --"},
			expected: "'''; DROP TABLE users; --'",
		},
		{
			name:     "multiple single quotes",
			elems:    []string{"a''b", "c'd"},
			expected: "'a''''b','c''d'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InStringList(tt.elems)
			if result != tt.expected {
				t.Errorf("InStringList(%v) = %s; expected %s", tt.elems, result, tt.expected)
			}
		})
	}
}
