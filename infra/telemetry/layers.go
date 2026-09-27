// 分层埋点助手（与 monolith internal/telemetry 逐字对齐的命名规则）。
package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// Start 分层埋点：span 名统一为 <layer>.<entity>.<op>，
// 例如 handler.user.create / service.order.create / repository.order.create_with_items。
//
// 各服务 layer 取值（分层约定）：
//   - handler      webx/gateway HTTP 入站
//   - service      业务编排层
//   - repository   数据访问层（GORM Find/JOIN）
//   - middleware   限流/身份校验等中间件
//   - messaging    MQ 生产/消费（job 回调）
//   - task         定时任务（job schedule 对账）
//
// 返回的 ctx 必须继续向下传递——子 span 与日志入链都依赖 ctx 中的活动 span。
// 总闸关闭时 current 为 noop tracer，Start 零开销，业务代码照常书写。
func Start(ctx context.Context, layer, entity, op string) (context.Context, trace.Span) {
	return current.Start(ctx, layer+"."+entity+"."+op)
}

// Tracer 返回当前 tracer（给需要手动创建 span 的场景）
func Tracer() trace.Tracer { return current }
