// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

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
