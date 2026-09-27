// Package consul 测试：httptest 模拟 Consul KV HTTP API（/v1/kv/<key>），
// 验证 Register/Resolve/Deregister/Target 的读写与幂等语义。
package consul

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeConsul 内存版 Consul KV（仅实现本包用到的 GET/PUT /v1/kv/<key>）。
//
// 注意：Consul Go client 的 KV 端点路径为 /v1/kv/<key>，且会对 key 中的
// "/" 做百分号编码（hubx/svc/srv-order → hubx%2Fsvc%2Fsrv-order），
// http server 的 r.URL.Path 是解码后路径，按解码结果落库即可
// （同一 key 编码形态固定，解码后唯一）。
type fakeConsul struct {
	mu    sync.Mutex
	store map[string]string
}

type kvPair struct {
	Key   string
	Value string // base64
}

func newFakeConsul() *fakeConsul {
	return &fakeConsul{store: map[string]string{}}
}

// server 返回 httptest 服务器；客户端地址形如 "127.0.0.1:<port>"
func (f *fakeConsul) server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := kvKeyFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			if val, found := f.store[key]; found {
				pair := kvPair{Key: key, Value: base64.StdEncoding.EncodeToString([]byte(val))}
				if err := json.NewEncoder(w).Encode([]kvPair{pair}); err != nil {
					t.Errorf("encode: %v", err)
				}
				return
			}
			// Consul 语义：KV 不存在 → 200 + 空数组
			if err := json.NewEncoder(w).Encode([]kvPair{}); err != nil {
				t.Errorf("encode empty: %v", err)
			}
		case http.MethodPut:
			buf := make([]byte, r.ContentLength)
			if r.Body != nil {
				n, _ := r.Body.Read(buf)
				buf = buf[:n]
			}
			f.store[key] = string(buf)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
}

// kvKeyFromPath 从请求路径提取 KV key（前缀 "/v1/kv/"）
func kvKeyFromPath(path string) (string, bool) {
	key, found := strings.CutPrefix(path, "/v1/kv/")
	if !found || key == "" {
		return "", false
	}
	return key, true
}

func TestConsulRegisterResolveRoundtrip(t *testing.T) {
	fc := newFakeConsul()
	ts := fc.server(t)
	defer ts.Close()
	addr := ts.Listener.Addr().String()

	// 无此服务 → 空
	addrs, err := Resolve(addr, "srv-order")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("空表应返回空 slice, got %v", addrs)
	}

	// 注册两个实例
	if err := Register(addr, "srv-order", "10.0.0.1:9003"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := Register(addr, "srv-order", "10.0.0.2:9003"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 幂等：重复注册不产生重复项
	if err := Register(addr, "srv-order", "10.0.0.1:9003"); err != nil {
		t.Fatalf("Register idempotent: %v", err)
	}

	addrs, err = Resolve(addr, "srv-order")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("期望 2 个实例, got %v", addrs)
	}

	// Target 逗号拼接（kitex 多实例负载入口）
	target, err := Target(addr, "srv-order")
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	if target != "10.0.0.1:9003,10.0.0.2:9003" && target != "10.0.0.2:9003,10.0.0.1:9003" {
		t.Fatalf("Target 拼接异常: %s", target)
	}

	// 服务间互不串号
	if err := Register(addr, "gateway", "10.0.0.1:8000"); err != nil {
		t.Fatalf("Register gateway: %v", err)
	}
	addrs, _ = Resolve(addr, "gateway")
	if len(addrs) != 1 || addrs[0] != "10.0.0.1:8000" {
		t.Fatalf("gateway 注册表异常: %v", addrs)
	}

	// Deregister 移除一个实例
	if err := Deregister(addr, "srv-order", "10.0.0.1:9003"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	addrs, _ = Resolve(addr, "srv-order")
	if len(addrs) != 1 || addrs[0] != "10.0.0.2:9003" {
		t.Fatalf("Deregister 后应剩 1 个实例: %v", addrs)
	}
	// Deregister 幂等（不存在的实例）
	if err := Deregister(addr, "srv-order", "10.9.9.9:1"); err != nil {
		t.Fatalf("Deregister idempotent: %v", err)
	}
}

func TestConsulValidateParams(t *testing.T) {
	if err := Register("127.0.0.1:8500", "", "a:1"); err == nil {
		t.Fatal("空 serviceID 应报错")
	}
	if err := Register("127.0.0.1:8500", "svc", ""); err == nil {
		t.Fatal("空 hostPort 应报错")
	}
	if _, err := Target("127.0.0.1:8500", ""); err == nil {
		t.Fatal("空 serviceID 应报错")
	}
	if _, err := Resolve("invalid:port", "svc"); err == nil {
		t.Fatalf("非法地址应报错, got %v", err)
	}
}
