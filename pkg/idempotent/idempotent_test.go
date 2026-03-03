// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package idempotent

import (
	"context"
	"errors"
	"testing"
)

func TestWaitCacheImmediateHit(t *testing.T) {
	ctx := context.Background()
	var v int

	calls := 0
	getter := func(ctx context.Context, vv any) (bool, error) {
		calls++
		p := vv.(*int)
		*p = 42
		return true, nil
	}

	if err := waitCache(ctx, 0, getter, &v); err != nil {
		t.Fatalf("waitCache unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("getter called %d times, want 1", calls)
	}
	if v != 42 {
		t.Fatalf("v = %d, want 42", v)
	}
}

func TestWaitCacheTimeoutWithEmptyCache(t *testing.T) {
	ctx := context.Background()

	getter := func(ctx context.Context, vv any) (bool, error) {
		return false, nil
	}

	err := waitCache(ctx, 0, getter, nil)
	if !errors.Is(err, ErrorCacheEmpty) {
		t.Fatalf("waitCache error = %v, want ErrorCacheEmpty", err)
	}

}
