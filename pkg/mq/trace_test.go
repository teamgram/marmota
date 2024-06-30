// Copyright © 2024 Teamgram open source community. All rights reserved.
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

package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/zeromicro/go-zero/core/logx"
	ztrace "github.com/zeromicro/go-zero/core/trace"
)

func TestMain(m *testing.M) {
	fmt.Println("begin")
	ztrace.StartAgent(ztrace.Config{})
	m.Run()
	fmt.Println("end")
}

func TestStartProducerSpan(t *testing.T) {
	ctx, span := startProducerSpan(context.Background(), "test")
	span.End()
	logx.WithContext(ctx).Infof("1")
	tr, _ := span.SpanContext().MarshalJSON()
	t.Log(string(tr))

	ctx2, span2 := startConsumerSpan(ctx, "abc")
	defer span2.End()
	logx.WithContext(ctx2).Infof("2")
	tr2, _ := span2.SpanContext().MarshalJSON()
	t.Log(string(tr2))
}

func TestStartConsumerSpan(t *testing.T) {
	ctx, span := startConsumerSpan(context.Background(), "test")
	span.End()
	logx.WithContext(ctx).Infof("1")
	tr, _ := span.SpanContext().MarshalJSON()
	t.Log(string(tr))
}
