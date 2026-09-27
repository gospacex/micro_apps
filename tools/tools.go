// Package tools 锁定本模块的外部与本地 SDK 依赖（防止 go mod tidy 因暂时无代码引用而裁剪 require）。
//
// 说明：
//   - 8 个 hubx 本地 SDK 以 replace 指向 ../{rpcx,configx,logx,dbx,cachex,mqx,otelx,webx}；
//     rpcx 无根包，故此处引其子包 rpcx/tracing。
//   - cobra（job CLI）与 hashicorp/consul/api（服务发现）为外部依赖。
//   - 本文件仅 import 用于锁定依赖图，不参与任何业务装配。
package tools

import (
	_ "github.com/gospacex/cachex"
	_ "github.com/gospacex/configx"
	_ "github.com/gospacex/dbx"
	_ "github.com/gospacex/logx"
	_ "github.com/gospacex/mqx"
	_ "github.com/gospacex/otelx"
	_ "github.com/gospacex/webx"
	_ "github.com/hashicorp/consul/api"
	_ "github.com/kitexx/kitex-framework/rpcx"
	_ "github.com/kitexx/kitex-framework/tracing"
	_ "github.com/spf13/cobra"
)
