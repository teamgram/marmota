// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package random2

import (
	"math/rand"
)

type Range struct {
	Begin int
	End   int
}

func RandIntFromRange(r Range) int {
	if r.End-r.Begin <= 0 {
		return r.Begin
	}
	return rand.Intn((r.End-r.Begin)+1) + r.Begin
}
