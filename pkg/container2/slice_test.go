// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package container2

// 实现性能测试函数
// go test -bench=.
// go test -bench=ContainsInt -benchmem
// go test -bench=ContainsInt -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof -blockprofile=block.prof

import (
	"testing"
)

func BenchmarkContainsInt(b *testing.B) {
	list := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for i := 0; i < b.N; i++ {
		ContainsInt(list, 5)
	}
}
