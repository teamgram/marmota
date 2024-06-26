// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package strings2

// 基准测试
// go test -v -run=none -bench=.
// go test -v -run=none -bench=. -benchmem
// go test -v -run=none -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof -blockprofile=block.prof

import (
	"testing"
)

// JoinInt32List()函数的基准测试
func BenchmarkJoinInt32List(b *testing.B) {
	// 准备基准测试数据
	elems := []int32{1, 2, 3, 4, 5}
	// 执行基准测试
	for i := 0; i < b.N; i++ {
		JoinInt32List(elems, ",")
	}
}

// JoinUint32List()函数的基准测试
func BenchmarkJoinUint32List(b *testing.B) {
	// 准备基准测试数据
	elems := []uint32{1, 2, 3, 4, 5}
	// 执行基准测试
	for i := 0; i < b.N; i++ {
		JoinUint32List(elems, ",")
	}
}

// JoinInt64List()函数的基准测试
func BenchmarkJoinInt64List(b *testing.B) {
	// 准备基准测试数据
	elems := []int64{1, 2, 3, 4, 5}
	// 执行基准测试
	for i := 0; i < b.N; i++ {
		JoinInt64List(elems, ",")
	}
}

// JoinUint64List()函数的基准测试
func BenchmarkJoinUint64List(b *testing.B) {
	// 准备基准测试数据
	elems := []uint64{1, 2, 3, 4, 5}
	// 执行基准测试
	for i := 0; i < b.N; i++ {
		JoinUint64List(elems, ",")
	}
}
