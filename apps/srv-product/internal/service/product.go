// Package service srv-product 业务编排层。
//
// 语义（brief §srv-product）：
//   - GetProduct：products ⋈ categories ⋈ inventories 3 表 JOIN Find；
//     on_shelf=0 → 3001；命中缓存 product:hot:{id} TTL 30s（On("cache")
//     门控，缓存读失败 fail-open 直查 DB）
//   - ReserveStock：本地事务行锁扣 inventories（repository.ReserveStockTx）；
//     库存不足 → 3002；行锁/更新失败 → 3003；下架商品 → 3001
//   - ListProducts：分页列表 + 库存列投影（inventories 批量查）
//
// 业务错误一律经 Response.Code/Message 返回（pkg/codes），网络/内部错误
// 走 Go error（rpcx 错误体），两套不混用。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/logx"

	productpb "hubx/micro_apps/pkg/kitex_gen/product"

	"hubx/micro_apps/apps/srv-product/internal/repository"
	"hubx/micro_apps/pkg/codes"
)

// 热点缓存参数（brief：product:hot:{id} TTL 30s）
const (
	cacheKeyPrefix = "product:hot:"
	cacheTTL       = 30 * time.Second
)

// ProductService 商品业务编排
type ProductService struct {
	repo    *repository.ProductRepository
	cache   cachex.Cache // nil → 缓存门控关闭（fail-open 直查 DB）
	traceOn bool         // 调用链开时写 traceparent（缓存值不携带链路，仅作数据缓存）
	logger  logx.Logger
}

// NewProductService 构造（cache 允许 nil：cachex 门控关闭形态）
func NewProductService(
	db *repository.ProductRepository,
	cache cachex.Cache,
	traceOn bool,
	logger logx.Logger,
) *ProductService {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &ProductService{repo: db, cache: cache, traceOn: traceOn, logger: logger}
}

// GetProduct 查商品（缓存优先：product:hot:{id} TTL 30s）
func (s *ProductService) GetProduct(ctx context.Context, req *productpb.GetProductReq) (*productpb.GetProductResp, error) {
	resp := &productpb.GetProductResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if req.GetProductId() <= 0 {
		c := codes.Fail(codes.CodeInvalidParam, "product id required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}

	// 1) 缓存命中短路（只缓存已上架商品快照；库存列不走缓存——
	// 预占后库存即时变化，stale 会导致超卖误判）
	if s.cache != nil {
		if raw, cerr := s.cache.Get(ctx, cacheKeyPrefix+itoa(req.GetProductId())); cerr == nil && raw != "" {
			var cached struct {
				Product *productpb.ProductInfo
			}
			if jerr := json.Unmarshal([]byte(raw), &cached); jerr == nil && cached.Product != nil {
				resp.Product = cached.Product
				resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
				return resp, nil
			}
		}
	}

	// 2) 3 表 JOIN（categories/inventories LEFT JOIN 未命中 → 零值）
	pr, err := s.repo.GetProduct(ctx, uint64(req.GetProductId()))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c := codes.Fail(codes.CodeProductOffShelf, "product not found")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if pr.Product.OnShelf != 1 {
		c := codes.Fail(codes.CodeProductOffShelf, "product off shelf")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	info := &productpb.ProductInfo{
		ProductId:    int64(pr.Product.ID),
		Title:        pr.Product.Title,
		Price:        pr.Product.Price,
		Stock:        int32(pr.Inventory.Available),
		CategoryName: pr.Category.Name,
	}
	// 3) 回填缓存（缓存写失败忽略：数据已就绪）
	if s.cache != nil {
		if raw, jerr := json.Marshal(map[string]*productpb.ProductInfo{"p": info}); jerr == nil {
			_ = s.cache.Set(ctx, cacheKeyPrefix+itoa(req.GetProductId()), string(raw), cacheTTL)
		}
	}
	resp.Product = info
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// ReserveStock 库存预占（srv-order.CreateOrder 上游调用）
func (s *ProductService) ReserveStock(ctx context.Context, req *productpb.ReserveStockReq) (*productpb.ReserveStockResp, error) {
	resp := &productpb.ReserveStockResp{UserId: req.GetUserId(), ProductId: req.GetProductId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if req.GetProductId() <= 0 || req.GetQuantity() <= 0 {
		c := codes.Fail(codes.CodeInvalidParam, "product id and quantity required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	// 下架商品不可预占（先查主表 status 再进事务，避免无谓行锁）
	pr, err := s.repo.GetProduct(ctx, uint64(req.GetProductId()))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c := codes.Fail(codes.CodeProductOffShelf, "product not found")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if pr.Product.OnShelf != 1 {
		c := codes.Fail(codes.CodeProductOffShelf, "product off shelf")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}

	remaining, err := s.repo.ReserveStockTx(ctx, uint64(req.GetProductId()), int(req.GetQuantity()))
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrStockNotEnough):
			c := codes.Fail(codes.CodeStockNotEnough, "stock not enough")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		case errors.Is(err, repository.ErrInventoryMissing), errors.Is(err, repository.ErrReserveFailed):
			c := codes.Fail(codes.CodeReserveStockFail, "reserve stock failed")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		default:
			s.logger.WarnContext(ctx, "reserve stock tx failed",
				logx.Int("product", int(req.GetProductId())), logx.Err(err))
			c := codes.Fail(codes.CodeReserveStockFail, err.Error())
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
	}
	// 预占成功后失效热点缓存（库存列快照过期）
	if s.cache != nil {
		_ = s.cache.Delete(ctx, cacheKeyPrefix+itoa(req.GetProductId()))
	}
	resp.Ok = true
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	_ = remaining
	return resp, nil
}

// ListProducts 商品列表（库存列批量投影）
func (s *ProductService) ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	resp := &productpb.ListProductsResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	products, err := s.repo.ListProducts(ctx, int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	ids := make([]uint64, 0, len(products))
	for i := range products {
		ids = append(ids, products[i].ID)
	}
	stocks, err := s.repo.GetStock(ctx, ids)
	if err != nil {
		// 库存投影失败降级：列表仍返回（Stock=0），记 warn（非主链路）
		s.logger.WarnContext(ctx, "stock projection failed (list returns zero stock)", logx.Err(err))
		stocks = nil
	}
	out := make([]*productpb.ProductInfo, 0, len(products))
	for i := range products {
		stock, ok := stocks[products[i].ID]
		if !ok {
			stock = 0
		}
		out = append(out, &productpb.ProductInfo{
			ProductId: int64(products[i].ID),
			Title:     products[i].Title,
			Price:     products[i].Price,
			Stock:     int32(stock),
		})
	}
	resp.Products = out
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// itoa int64 → string（缓存 key 拼接）
func itoa(v int64) string {
	return fmt.Sprint(v)
}
