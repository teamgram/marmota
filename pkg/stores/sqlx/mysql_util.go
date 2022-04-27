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
	"errors"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"strings"
)

type CommonDAO struct {
	db *DB
}

func NewCommonDAO(db *DB) *CommonDAO {
	return &CommonDAO{db}
}

func (dao *CommonDAO) DB() *DB {
	return dao.db
}

// CheckExists
// 检查是否存在
func (dao *CommonDAO) CheckExists(ctx context.Context, table string, params map[string]interface{}) bool {
	if len(params) == 0 {
		logx.WithContext(ctx).Errorf("CheckExists - [%s] error: params empty!", table)
		return false
	}

	names := make([]string, 0, len(params))
	for k, _ := range params {
		names = append(names, k+" = :"+k)
	}
	sql := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1", table, strings.Join(names, " AND "))

	var (
		existed int
	)

	err := dao.db.NamedQueryRowPartial(ctx, &existed, sql, params)
	if err != nil && err != ErrNotFound {
		logx.WithContext(ctx).Errorf("CheckExists - [%s] error: %s", table, err)
		return false
	}

	return existed == 1
}

func (dao *CommonDAO) CalcSize(ctx context.Context, table string, params map[string]interface{}) int {
	if len(params) == 0 {
		logx.WithContext(ctx).Errorf("calcSize - [%s] error: params empty!", table)
		return 0
	}

	aValues := make([]interface{}, 0, len(params))
	names := make([]string, 0, len(params))
	for k, v := range params {
		names = append(names, k+" = ?")
		aValues = append(aValues, v)
	}
	sql := fmt.Sprintf("SELECT count(id) FROM %s WHERE %s", table, strings.Join(names, " AND "))

	var count int
	err := dao.db.QueryRow(ctx, &count, sql, aValues...)
	if err != nil {
		logx.Errorf("calcSize - [%s] error: %s", sql, err)
		return 0
	}
	return count
}

func (dao *CommonDAO) CalcSizeByWhere(ctx context.Context, table, where string) int {
	sql := fmt.Sprintf("SELECT count(id) FROM %s WHERE %s", table, where)

	var count int
	err := dao.db.QueryRow(ctx, &count, sql)
	if err != nil {
		logx.Errorf("calcSize - [%s] error: %s", sql, err)
		return 0
	}
	return count
}

// CheckExists
// 检查是否存在
func CheckExists(ctx context.Context, db *DB, table string, params map[string]interface{}) (bool, error) {
	if len(params) == 0 {
		logx.WithContext(ctx).Errorf("checkExists - [%s] error: params empty!", table)
		return false, errors.New("params empty")
	}

	names := make([]string, 0, len(params))
	for k, _ := range params {
		names = append(names, k+" = :"+k)
	}
	sql := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1", table, strings.Join(names, " AND "))

	var (
		existed int
	)

	err := db.NamedQueryRowPartial(ctx, &existed, sql, params)
	if err != nil && err != ErrNotFound {
		logx.WithContext(ctx).Errorf("CheckExists - [%s] error: %s", table, err)
		return false, err
	}

	return existed == 1, nil
}
