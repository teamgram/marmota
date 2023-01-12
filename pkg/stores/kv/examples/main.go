// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package main

import (
	"context"
	"fmt"

	"github.com/teamgram/marmota/pkg/stores/kv"
	"github.com/zeromicro/go-zero/core/conf"
)

type Config struct {
	KV kv.KvConf
}

func main() {
	var c Config
	conf.MustLoad("./kv.yaml", &c)

	kv := kv.NewStore(c.KV)
	vals, _ := kv.MgetCtx(context.Background(), "document_1572598612301451270", "document_1572599736324591618")
	for _, v := range vals {
		fmt.Println(v)
	}
}
