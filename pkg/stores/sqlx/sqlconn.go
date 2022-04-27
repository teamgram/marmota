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

type conn struct {
	conn sqlx.SqlConn
	*reflectx.Mapper
}

func newConn(c sqlx.SqlConn) *conn {
	return &conn{
		conn:   c,
		Mapper: mapper(),
	}
}

func (db *conn) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.conn.ExecCtx(ctx, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *conn) NamedExec(c context.Context, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return nil, err
	}

	return db.conn.ExecCtx(c, q, args...)
}

func (db *conn) Prepare(ctx context.Context, query string) (sqlx.StmtSession, error) {
	return db.conn.PrepareCtx(ctx, query)
}

// TODO:
//func (db *conn) NamedPrepare(ctx context.Context, query string) (sqlx.StmtSession, error) {
//	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
//	if err != nil {
//		return nil, err
//	}
//
//	return db.conn.PrepareCtx(ctx, query)
//}

func (db *conn) QueryRow(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowCtx(ctx, v, query, args...)
}

// NamedQueryRow using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *conn) NamedQueryRow(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRow(c, v, q, args...)
}

func (db *conn) QueryRowPartial(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowPartialCtx(ctx, v, query, args...)
}

func (db *conn) NamedQueryRowPartial(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRowPartial(c, v, q, args...)
}

func (db *conn) QueryRows(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowsCtx(ctx, v, query, args...)
}

func (db *conn) NamedQueryRows(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRows(c, v, q, args...)
}

func (db *conn) QueryRowsPartial(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowsPartialCtx(ctx, v, query, args...)
}

func (db *conn) NamedQueryRowsPartial(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRowsPartial(c, v, q, args...)
}

func (db *conn) RawDB() (*sql.DB, error) {
	return db.conn.RawDB()
}

func (db *conn) Transact(ctx context.Context, fn func(*Tx) error) error {
	return db.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		tx := newTx(ctx, session)
		return fn(tx)
	})
}
