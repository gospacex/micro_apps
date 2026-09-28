// Package service srv-pay 业务编排层。
//
// 语义（brief §srv-pay）：
//   - Pay：开支付单（pay_orders insert 事务，同 user+order 已存在未终态单
//     → 5002 幂等重放直接返回既有单；channel 白名单 alipay/wechat/union，
//     非法 → 1001）
//   - Callback：回调流水落 pay_callbacks（追加写）；已存在 success 回调
//     → 幂等 Accepted；校验失败（支付单不存在/渠道不匹配）→ 5003 拒收；
//     success=true → 支付单 pending/processing → success + paid_at
//   - ListPayFlows：只读流水（对账数据源，user_id 隔离 + 日期闭区间）
//
// 业务错误一律经 Response.Code/Message 返回（pkg/codes），网络/内部错误
// 走 Go error（rpcx 错误体），两套不混用。
package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/gospacex/logx"

	paypb "hubx/micro_apps/pkg/kitex_gen/pay"

	"hubx/micro_apps/apps/srv-pay/internal/repository"
	"hubx/micro_apps/pkg/codes"
)

// 支付渠道白名单（pay_orders.channel 枚举）
var payChannels = map[string]bool{"alipay": true, "wechat": true, "union": true}

// PayService 支付业务编排
type PayService struct {
	repo   *repository.PayRepository
	logger logx.Logger
}

// NewPayService 构造
func NewPayService(repo *repository.PayRepository, logger logx.Logger) *PayService {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &PayService{repo: repo, logger: logger}
}

// Pay 开支付单
func (s *PayService) Pay(ctx context.Context, req *paypb.PayReq) (*paypb.PayResp, error) {
	resp := &paypb.PayResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if req.GetOrderId() <= 0 {
		c := codes.Fail(codes.CodeInvalidParam, "order id required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	channel := normalizeChannel(req.GetChannel())
	if channel == "" {
		c := codes.Fail(codes.CodeInvalidParam, "channel must be alipay/wechat/union")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	payOrderNo := newPayOrderNo()
	// Amount：pb PayReq 无金额字段（对账金额以渠道回调 raw_body 为准），
	// 开单落 0，回调迁移 success 时不改写金额（deviation 记录）
	po, err := s.repo.CreatePayOrderTx(ctx, repository.PayOrder{
		UserID:  uint64(req.GetUserId()),
		OrderID: uint64(req.GetOrderId()),
		Channel: channel,
	}, payOrderNo)
	if err != nil {
		if errors.Is(err, repository.ErrPayDuplicate) {
			// 重复支付（幂等重放）：查既有单返回
			existing, ferr := s.lookupPending(ctx, uint64(req.GetUserId()), uint64(req.GetOrderId()))
			if ferr == nil && existing != nil {
				resp.PayOrderNo = existing.PayOrderNo
				resp.Status = existing.Status
				resp.Amount = existing.Amount
				c := codes.Fail(codes.CodePayDuplicate, "pay order already exists")
				resp.Code, resp.Message = c.Code, c.Message
				return resp, nil
			}
			c := codes.Fail(codes.CodePayDuplicate, "pay order already exists")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		s.logger.WarnContext(ctx, "create pay order failed", logx.Err(err))
		c := codes.Fail(codes.CodePayFailed, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.PayOrderNo = po.PayOrderNo
	resp.Status = po.Status
	resp.Amount = po.Amount
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// lookupPending 查同 user+order 既有支付单（幂等重放返回既有单号）
func (s *PayService) lookupPending(ctx context.Context, userID, orderID uint64) (*repository.PayOrder, error) {
	return s.repo.FindByUserOrder(ctx, userID, orderID)
}

// Callback 渠道异步回调
func (s *PayService) Callback(ctx context.Context, req *paypb.PayCallbackReq) (*paypb.PayCallbackResp, error) {
	resp := &paypb.PayCallbackResp{UserId: req.GetUserId()}
	if req.GetPayOrderNo() == "" {
		c := codes.Fail(codes.CodeInvalidParam, "pay_order_no required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	po, err := s.repo.FindByPayOrderNo(ctx, req.GetPayOrderNo())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			// 支付单不存在 → 拒收（5003），回调流水仍记录（success=0 便于审计）
			s.recordCallback(ctx, req, false)
			c := codes.Fail(codes.CodeCallbackReject, "pay order not found")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	// 渠道一致性校验（渠道串改 → 拒收）
	if req.GetChannel() != "" && req.GetChannel() != po.Channel {
		s.recordCallback(ctx, req, false)
		c := codes.Fail(codes.CodeCallbackReject, "channel mismatch")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	// 幂等：已 success 的支付单重复回调 → Accepted（不重复迁移）
	if po.Status == repository.PayStatusSuccess {
		s.recordCallback(ctx, req, true)
		resp.Accepted = true
		resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
		return resp, nil
	}
	if req.GetSuccess() {
		if _, merr := s.repo.MarkSuccess(ctx, po.PayOrderNo); merr != nil {
			s.logger.WarnContext(ctx, "mark pay success failed", logx.Err(merr))
			c := codes.Fail(codes.CodeInternal, merr.Error())
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		s.recordCallback(ctx, req, true)
	} else {
		if _, merr := s.repo.MarkFailed(ctx, po.PayOrderNo); merr != nil {
			s.logger.WarnContext(ctx, "mark pay failed-state update failed", logx.Err(merr))
			// 迁移失败不拒收：流水已记录，状态由对账兜底
		}
		s.recordCallback(ctx, req, false)
	}
	resp.Accepted = true
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// recordCallback 回调流水追加写（审计留痕；失败仅告警不阻断应答）
func (s *PayService) recordCallback(ctx context.Context, req *paypb.PayCallbackReq, success bool) {
	cb := &repository.PayCallback{
		PayOrderNo: req.GetPayOrderNo(),
		Channel:    req.GetChannel(),
		RawBody:    req.GetRawBody(),
		Success:    boolToInt(success),
	}
	if err := s.repo.RecordCallback(ctx, cb); err != nil {
		s.logger.WarnContext(ctx, "record pay callback failed",
			logx.String("pay_order_no", cb.PayOrderNo), logx.Err(err))
	}
}

// ListPayFlows 支付流水
func (s *PayService) ListPayFlows(ctx context.Context, req *paypb.ListPayFlowsReq) (*paypb.ListPayFlowsResp, error) {
	resp := &paypb.ListPayFlowsResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	flows, err := s.repo.ListFlows(ctx, uint64(req.GetUserId()), req.GetFromDate(), req.GetToDate(), int(req.GetLimit()))
	if err != nil {
		c := codes.Fail(codes.CodeInvalidParam, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	pbFlows := make([]*paypb.PayFlow, 0, len(flows))
	for i := range flows {
		pbFlows = append(pbFlows, &paypb.PayFlow{
			PayOrderId: int64(flows[i].ID),
			PayOrderNo: flows[i].PayOrderNo,
			OrderId:    int64(flows[i].OrderID),
			Amount:     flows[i].Amount,
			Status:     flows[i].Status,
			CreatedAt:  flows[i].CreatedAt.Unix(),
		})
	}
	resp.Flows = pbFlows
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// normalizeChannel 渠道归一（小写匹配白名单；空 → ""）
func normalizeChannel(ch string) string {
	ch = strings.ToLower(strings.TrimSpace(ch))
	if payChannels[ch] {
		return ch
	}
	return ""
}

// newPayOrderNo 支付单号（uk_pay_no，≤64 字符）：P + yyyyMMddHHmmss + 12 位 hex 随机
func newPayOrderNo() string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	return "P" + time.Now().Format("20060102150405") + strconv.FormatUint(uint64(binary.BigEndian.Uint64(buf[0:8])), 16)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
