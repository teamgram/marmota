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

func ContainsInt(list []int, i int) bool {
	for idx := range list {
		if list[idx] == i {
			return true
		}
	}
	return false
}

func ContainsInt32(list []int32, i int32) bool {
	for idx := range list {
		if list[idx] == i {
			return true
		}
	}
	return false
}

func ContainsInt64(list []int64, i int64) bool {
	for idx := range list {
		if list[idx] == i {
			return true
		}
	}
	return false
}

func ContainsUint32(list []uint32, i uint32) bool {
	for idx := range list {
		if list[idx] == i {
			return true
		}
	}
	return false
}

func ContainsUint64(list []uint64, i uint64) bool {
	for idx := range list {
		if list[idx] == i {
			return true
		}
	}
	return false
}

func ContainsString(list []string, str string) bool {
	for idx := range list {
		if list[idx] == str {
			return true
		}
	}
	return false
}

func AppendIgnoreNil(list []interface{}, elems ...interface{}) []interface{} {
	for idx := range elems {
		if elems[idx] != nil {
			list = append(list, elems[idx])
		}
	}
	return list
}
