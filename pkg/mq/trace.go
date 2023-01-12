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

package kafka

import (
	"context"
	"database/sql"

	"github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var sqlAttributeKey = attribute.Key("kafka.method")

func startSpan(ctx context.Context, method string) (context.Context, oteltrace.Span) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	start, span := tracer.Start(ctx,
		spanName,
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	span.SetAttributes(sqlAttributeKey.String(method))

	return start, span
}

func endSpan(span oteltrace.Span, err error) {
	defer span.End()

	if err == nil || err == sql.ErrNoRows {
		span.SetStatus(codes.Ok, "")
		return
	}

	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
}
