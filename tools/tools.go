// Package tools 锁定本模块的外部与本地 SDK 依赖（防止 go mod tidy 因暂时无代码引用而裁剪 require）。
//
// 说明：
//   - hubx 本地 SDK 以 replace 指向 ../{configx,logx,dbx,cachex,mqx,otelx,webx,traceflag}。
//     原 ../rpcx（外部模块 kitex-framework）已从本地工作树丢失且全仓无副本，
//     其 tracing / logx / rpcx 三个子包的能力已分别由 otelx、traceflag 与
//     仓内 pkg/rpcx 承接，依赖链上不再有不可控的本地目录。
//   - dbx 本地工作树已无根包（仅 orm/config/dbsql 子包），改以 gorm.io/gorm
//     锁定 ORM 依赖版本（与 ../dbx/go.mod 的 v1.25.12 一致，勿被 MVS 拉高）。
//   - cobra（job CLI）与 hashicorp/consul/api（服务发现）为外部依赖。
//   - 本文件仅 import 用于锁定依赖图，不参与任何业务装配。
package tools

import (
	_ "github.com/gospacex/cachex"
	_ "github.com/gospacex/configx"
	_ "github.com/gospacex/logx"
	_ "github.com/gospacex/mqx"
	_ "github.com/gospacex/otelx"
	_ "github.com/gospacex/traceflag"
	_ "github.com/gospacex/webx"
	_ "github.com/hashicorp/consul/api"
	_ "github.com/kitex-contrib/obs-opentelemetry/tracing"
	_ "github.com/spf13/cobra"
	_ "gorm.io/gorm"

	_ "hubx/micro_apps/pkg/rpcx"
)
