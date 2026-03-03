/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package hack gives you some efficient functionality at the cost of
// breaking some Go rules.
package hack

import "unsafe"

// StringArena lets you consolidate allocations for a group of strings
// that have similar life length
type StringArena struct {
	buf []byte
	str string
}

// NewStringArena creates an arena of the specified size.
func NewStringArena(size int) *StringArena {
	sa := &StringArena{buf: make([]byte, 0, size)}
	sa.str = unsafe.String(unsafe.SliceData(sa.buf[:cap(sa.buf)]), cap(sa.buf))
	return sa
}

// NewString copies a byte slice into the arena and returns it as a string.
// If the arena is full, it returns a traditional go string.
func (sa *StringArena) NewString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(sa.buf)+len(b) > cap(sa.buf) {
		return string(b)
	}
	start := len(sa.buf)
	sa.buf = append(sa.buf, b...)
	return sa.str[start : start+len(b)]
}

// SpaceLeft returns the amount of space left in the arena.
func (sa *StringArena) SpaceLeft() int {
	return cap(sa.buf) - len(sa.buf)
}

// String force casts a []byte to a string.
// USE AT YOUR OWN RISK
func String(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// StringPointer returns &s[0], which is not allowed in go
func StringPointer(s string) unsafe.Pointer {
	return unsafe.Pointer(unsafe.StringData(s))
}

// Bytes bring a no copy convert from string to byte slice
// consider the risk
func Bytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func BytesFromPtr(ptr uintptr, len int) []byte {
	if len <= 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(ptr)), len)
}
