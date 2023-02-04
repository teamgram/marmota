// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package main

import (
	"context"
	"fmt"

	"github.com/teamgram/marmota/pkg/stores/sqlc"
	"github.com/teamgram/marmota/pkg/stores/sqlx"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/jsonx"
	"github.com/zeromicro/go-zero/core/stores/cache"
)

type Config struct {
	Mysql sqlx.Config
	Cache cache.CacheConf
}

type CacheData struct {
	K string   `json:"k"`
	A string   `json:"a"`
	B int      `json:"b"`
	C int32    `json:"c"`
	D []string `json:"d"`
}

var (
	v1 = &CacheData{
		K: "tm_1572598612301451270",
		A: "1",
		B: 1,
		C: 1,
		D: []string{"1", "1"},
	}

	v2 = &CacheData{
		K: "tm_1572599736324591618",
		A: "2",
		B: 2,
		C: 2,
		D: []string{"2", "2"},
	}

	j1 = `{"k":"tm_1572598612301451270","a":"1","b":1,"c":1,"d":["1","1"]}`
	j2 = `{"k":"tm_1572599736324591618","a":"2","b":2,"c":2,"d":["2","2"]}`
)

func main() {
	var c Config
	conf.MustLoad("./cache.yaml", &c)

	conn := sqlc.NewConn(sqlx.NewMySQL(&c.Mysql), c.Cache)
	_ = conn

	keyList := []string{
		"tm_1572598612301451270",
		"tm_1572599736324591618",
		"tm_1572599736324591619",
	}

	values := map[string]*CacheData{}
	conn.QueryRows(
		context.Background(),
		func(ctx context.Context, conn *sqlx.DB, keys ...string) (map[string]interface{}, error) {
			v := make(map[string]interface{}, 0)
			for _, k := range keys {
				switch k {
				case keyList[0]:
					values[k] = v1
					v[k] = v1
				case keyList[1]:
					values[k] = v2
					v[k] = v2
				default:
					v[k] = nil
				}
			}
			return v, nil
		},
		func(k, data string) (interface{}, error) {
			fmt.Println("cached - k: ", k, ", v: ", data)
			var v *CacheData
			err := jsonx.UnmarshalFromString(data, &v)
			if err != nil {
				return nil, err
			}
			values[k] = v
			return v, nil
		},
		keyList...)

	for k, v := range values {
		fmt.Println("k: ", k, ", v: ", v)
	}
}
