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

package utils

import (
	"sort"
)

// Int32Slice IntSlice attaches the methods of Interface to []int, sorting in increasing order.
type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Int32Intersect(nums1 Int32Slice, nums2 Int32Slice) []int32 {
	if nums1 == nil || nums2 == nil {
		return []int32{}
	}
	sort.Sort(nums1)
	sort.Sort(nums2)
	var x = 0
	var y = 0
	var result Int32Slice
	for {
		if x < len(nums1) && y < len(nums2) {
			if nums1[x] == nums2[y] {
				result = append(result, nums1[x])
				x++
				y++
			} else if nums1[x] > nums2[y] {
				y++
			} else {
				x++
			}
		} else {
			break
		}

	}
	return result
}

// Int64Slice IntSlice attaches the methods of Interface to []int, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Int64Intersect(nums1 Int64Slice, nums2 Int64Slice) []int64 {
	if nums1 == nil || nums2 == nil {
		return []int64{}
	}
	sort.Sort(nums1)
	sort.Sort(nums2)
	var x = 0
	var y = 0
	var result Int64Slice
	for {
		if x < len(nums1) && y < len(nums2) {
			if nums1[x] == nums2[y] {
				result = append(result, nums1[x])
				x++
				y++
			} else if nums1[x] > nums2[y] {
				y++
			} else {
				x++
			}
		} else {
			break
		}

	}
	return result
}
