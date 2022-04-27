// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
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
	*reflectx.Mapper
}

func newTx(sess sqlx.Session) *Tx {
	return &Tx{
		conn:   sess,
		Mapper: mapper(),
	}
}

func (db *Tx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.conn.ExecCtx(ctx, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *Tx) NamedExec(c context.Context, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return nil, err
	}

	return db.conn.ExecCtx(c, q, args...)
}

func (db *Tx) Prepare(ctx context.Context, query string) (sqlx.StmtSession, error) {
	return db.conn.PrepareCtx(ctx, query)
}

func (db *Tx) QueryRow(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowCtx(ctx, v, query, args...)
}

// NamedQueryRow using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *Tx) NamedQueryRow(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRow(c, v, q, args...)
}

func (db *Tx) QueryRowPartial(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowPartialCtx(ctx, v, query, args...)
}

func (db *Tx) NamedQueryRowPartial(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRowPartial(c, v, q, args...)
}

func (db *Tx) QueryRows(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowsCtx(ctx, v, query, args...)
}

func (db *Tx) NamedQueryRows(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRows(c, v, q, args...)
}

func (db *Tx) QueryRowsPartial(ctx context.Context, v interface{}, query string, args ...interface{}) error {
	return db.conn.QueryRowsPartialCtx(ctx, v, query, args...)
}

func (db *Tx) NamedQueryRowsPartial(c context.Context, v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return err
	}
	return db.QueryRowsPartial(c, v, q, args...)
}
