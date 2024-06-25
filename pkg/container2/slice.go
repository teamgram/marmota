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

package container2

func ContainsInt(slice []int, target int) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func ContainsInt32(slice []int32, target int32) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func ContainsInt64(slice []int64, target int64) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func ContainsUint32(slice []uint32, target uint32) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func ContainsUint64(slice []uint64, target uint64) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func ContainsString(slice []string, target string) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

func AppendIgnoreNil(slice []interface{}, elems ...interface{}) []interface{} {
	for _, e := range elems {
		if e != nil {
			slice = append(slice, e)
		}
	}
	return slice
}
