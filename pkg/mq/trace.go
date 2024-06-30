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

	"github.com/IBM/sarama"
	ztrace "github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

// spanName is used to identify the span name for the SQL execution.
const spanName = "kafka.producer"

var kafkaAttributeKey = attribute.Key("kafka.method")

func startProducerSpan(ctx context.Context, method string) (context.Context, trace.Span) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	tracer := otel.GetTracerProvider().Tracer(ztrace.TraceName)
	ctx, span := tracer.Start(ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindClient),
	)
	span.SetAttributes(kafkaAttributeKey.String(method))
	ztrace.Inject(ctx, otel.GetTextMapPropagator(), &md)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx, span
}

func endProducerSpan(span trace.Span, err error) {
	defer span.End()

	if err == nil {
		span.SetStatus(codes.Ok, "")
		return
	}

	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
}

func startConsumerSpan(ctx context.Context, method string) (context.Context, trace.Span) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	bags, spanCtx := ztrace.Extract(ctx, otel.GetTextMapPropagator(), &md)
	ctx = baggage.ContextWithBaggage(ctx, bags)
	tracer := otel.GetTracerProvider().Tracer(ztrace.TraceName)

	ctx, span := tracer.Start(trace.ContextWithRemoteSpanContext(ctx, spanCtx),
		spanName,
		trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(kafkaAttributeKey.String(method))

	return ctx, span
}

func extractTraceHeaders(ctx context.Context) (headers []sarama.RecordHeader) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return
	}

	headers = make([]sarama.RecordHeader, 0, len(md))

	for k, v := range md {
		if len(v) == 0 {
			continue
		}

		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v[0]),
		})
	}

	return
}

func injectTraceHeaders(headers []*sarama.RecordHeader) (ctx context.Context) {
	ctx = context.Background()
	if len(headers) == 0 {
		return
	}

	md := make(metadata.MD, len(headers))

	for _, h := range headers {
		md[string(h.Key)] = []string{string(h.Value)}
	}

	return metadata.NewIncomingContext(ctx, md)
}

func tryGetMethodByHeaders(headers []*sarama.RecordHeader) (method string) {
	for _, h := range headers {
		if string(h.Key) == "method" {
			method = string(h.Value)
			break
		}
	}

	return
}
