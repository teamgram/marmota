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
