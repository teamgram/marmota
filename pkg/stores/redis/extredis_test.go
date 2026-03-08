// Copyright (c) 2026 The Teamgram Authors (https://teamgram.net).
//  All rights reserved.
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

package redis

import (
	"context"
	"testing"

	"github.com/zeromicro/go-zero/core/stores/redis/redistest"
)

func TestExpireWithResult(t *testing.T) {
	rds := redistest.CreateRedis(t)
	ctx := context.Background()

	// Set a key so it exists.
	if err := rds.SetCtx(ctx, "expire-key", "value"); err != nil {
		t.Fatalf("SetCtx: %v", err)
	}

	// Expire an existing key should return true.
	got, err := ExpireWithResultCtx(ctx, rds, "expire-key", 10)
	if err != nil {
		t.Fatalf("ExpireWithResultCtx: %v", err)
	}
	if !got {
		t.Error("expected true for existing key, got false")
	}

	// Expire a non-existent key should return false.
	got, err = ExpireWithResultCtx(ctx, rds, "no-such-key", 10)
	if err != nil {
		t.Fatalf("ExpireWithResultCtx: %v", err)
	}
	if got {
		t.Error("expected false for non-existent key, got true")
	}

	// Test the non-Ctx variant as well.
	got, err = ExpireWithResult(rds, "expire-key", 5)
	if err != nil {
		t.Fatalf("ExpireWithResult: %v", err)
	}
	if !got {
		t.Error("expected true for existing key via ExpireWithResult, got false")
	}
}

func TestMsetStrings(t *testing.T) {
	rds := redistest.CreateRedis(t)
	ctx := context.Background()

	kv := map[string]string{
		"mset-a": "1",
		"mset-b": "2",
		"mset-c": "3",
	}

	if err := MsetStringsCtx(ctx, rds, kv); err != nil {
		t.Fatalf("MsetStringsCtx: %v", err)
	}

	for k, want := range kv {
		got, err := rds.GetCtx(ctx, k)
		if err != nil {
			t.Fatalf("GetCtx(%s): %v", k, err)
		}
		if got != want {
			t.Errorf("GetCtx(%s) = %q, want %q", k, got, want)
		}
	}

	// Test the non-Ctx variant.
	kv2 := map[string]string{"mset-d": "4"}
	if err := MsetStrings(rds, kv2); err != nil {
		t.Fatalf("MsetStrings: %v", err)
	}
	got, err := rds.GetCtx(ctx, "mset-d")
	if err != nil {
		t.Fatalf("GetCtx(mset-d): %v", err)
	}
	if got != "4" {
		t.Errorf("GetCtx(mset-d) = %q, want %q", got, "4")
	}
}

func TestMsetnx(t *testing.T) {
	rds := redistest.CreateRedis(t)
	ctx := context.Background()

	kv := map[string]string{
		"nx-a": "1",
		"nx-b": "2",
	}

	// First call: none of the keys exist, should return true.
	ok, err := MsetnxCtx(ctx, rds, kv)
	if err != nil {
		t.Fatalf("MsetnxCtx (first): %v", err)
	}
	if !ok {
		t.Error("expected true on first MsetnxCtx, got false")
	}

	// Verify the keys were actually set.
	for k, want := range kv {
		got, err := rds.GetCtx(ctx, k)
		if err != nil {
			t.Fatalf("GetCtx(%s): %v", k, err)
		}
		if got != want {
			t.Errorf("GetCtx(%s) = %q, want %q", k, got, want)
		}
	}

	// Second call: keys already exist, should return false.
	ok, err = MsetnxCtx(ctx, rds, kv)
	if err != nil {
		t.Fatalf("MsetnxCtx (second): %v", err)
	}
	if ok {
		t.Error("expected false on second MsetnxCtx, got true")
	}

	// Test the non-Ctx variant with fresh keys.
	ok, err = Msetnx(rds, map[string]string{"nx-c": "3"})
	if err != nil {
		t.Fatalf("Msetnx: %v", err)
	}
	if !ok {
		t.Error("expected true for Msetnx with new key, got false")
	}
}
