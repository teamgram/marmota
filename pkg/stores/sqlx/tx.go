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

package sqlx

import (
	"context"
	"database/sql"

	"github.com/teamgram/marmota/pkg/stores/sqlx/reflectx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Tx struct {
	conn sqlx.Session
	ctx  context.Context
	*reflectx.Mapper
}

func newTx(c context.Context, sess sqlx.Session) *Tx {
	return &Tx{
		conn:   sess,
		ctx:    c,
		Mapper: mapper(),
	}
}

func (tx *Tx) Context() context.Context {
	return tx.ctx
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.conn.ExecCtx(tx.ctx, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return nil, err
	}

	return tx.conn.ExecCtx(tx.ctx, q, args...)
}

func (tx *Tx) Prepare(query string) (sqlx.StmtSession, error) {
	return tx.conn.PrepareCtx(tx.ctx, query)
}

func (tx *Tx) QueryRow(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowCtx(tx.ctx, v, query, args...)
}

// NamedQueryRow using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedQueryRow(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRow(v, q, args...)
}

func (tx *Tx) QueryRowPartial(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowPartialCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRowPartial(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRowPartial(v, q, args...)
}

func (tx *Tx) QueryRows(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowsCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRows(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRows(v, q, args...)
}

func (tx *Tx) QueryRowsPartial(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowsPartialCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRowsPartial(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRowsPartial(v, q, args...)
}
