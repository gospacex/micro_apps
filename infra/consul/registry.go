// Package consul 基于 hashicorp/consul/api 的 KV 服务注册表（装配总公式第 4 步）。
//
// 背景（§0.1 已核实）：rpcx 无 Consul resolver / 注册中心集成，寻址靠
// ClientOptions.Target（逗号分隔多实例）。本包因此做"服务发现降级"：
//
//	服务侧   Register(agent, svcID, hostPort)   → KV  hubx/svc/<svcID> 追加 host:port
//	客户端侧 Resolve(agent, svcID)               → 读 KV → []host:port → strings.Join(",", )
//
// KV 值形态：JSON 字符串数组 ["host:port", ...]（按 key 原子写整个数组，
// 实例增删都重新序列化全量，避免 KV 读改写竞态下的部分写入）。
//
// Consul 原生 service catalog（PutService/Deregister）带 health check，但要求
// 每个服务实例常驻 agent 心跳；本案例按计划的 KV 注册表实现（§0.1 定死），
// 好处：实例掉线由调用方周期刷新 + 超时剔除，无需 agent 健康探测。
package consul

import (
	"encoding/json"
	"fmt"

	consulapi "github.com/hashicorp/consul/api"
)

// KVPrefix 服务注册表键前缀：hubx/svc/<serviceID>
const KVPrefix = "hubx/svc"

// KVKey 返回服务注册表键
func KVKey(serviceID string) string { return KVPrefix + "/" + serviceID }

// Register 把本服务实例写进注册表（幂等：同 host:port 已存在则直接保留）。
// agentAddr 仅用于创建客户端（如 "127.0.0.1:8500"）。
// 写入语义：读全量 → 去重追加 → 原子 Set 整个数组（CheckAndSet 非必需，
// 本案例单写者=实例启动/心跳周期，竞态窗口影响有限，注释记录）。
func Register(agentAddr, serviceID, hostPort string) error {
	if serviceID == "" {
		return fmt.Errorf("consul: empty serviceID")
	}
	if hostPort == "" {
		return fmt.Errorf("consul: empty hostPort")
	}
	c, err := newClient(agentAddr)
	if err != nil {
		return err
	}
	key := KVKey(serviceID)
	// 读当前值（KV 可能不存在 → 空数组）
	pairs, _, err := c.KV().Get(key, nil)
	if err != nil {
		return fmt.Errorf("consul: read %s: %w", key, err)
	}
	addrs := []string{}
	if pairs != nil && string(pairs.Value) != "" {
		if err := json.Unmarshal(pairs.Value, &addrs); err != nil {
			return fmt.Errorf("consul: parse %s: %w", key, err)
		}
	}
	exists := false
	for _, a := range addrs {
		if a == hostPort {
			exists = true
			break
		}
	}
	if exists {
		return nil // 幂等：不重写
	}
	addrs = append(addrs, hostPort)
	val, err := json.Marshal(addrs)
	if err != nil {
		return err
	}
	_, err = c.KV().Put(&consulapi.KVPair{Key: key, Value: val}, &consulapi.WriteOptions{})
	if err != nil {
		return fmt.Errorf("consul: write %s: %w", key, err)
	}
	return nil
}

// Deregister 从注册表移除本服务实例（优雅退出反序调用）。
func Deregister(agentAddr, serviceID, hostPort string) error {
	if serviceID == "" {
		return fmt.Errorf("consul: empty serviceID")
	}
	c, err := newClient(agentAddr)
	if err != nil {
		return err
	}
	key := KVKey(serviceID)
	pairs, _, err := c.KV().Get(key, nil)
	if err != nil {
		return fmt.Errorf("consul: read %s: %w", key, err)
	}
	if pairs == nil {
		return nil // 无此服务 → 幂等成功
	}
	addrs := []string{}
	if string(pairs.Value) != "" {
		if err := json.Unmarshal(pairs.Value, &addrs); err != nil {
			return fmt.Errorf("consul: parse %s: %w", key, err)
		}
	}
	kept := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a != hostPort {
			kept = append(kept, a)
		}
	}
	if len(kept) == len(addrs) {
		return nil // 本实例不在表里
	}
	val, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	_, err = c.KV().Put(&consulapi.KVPair{Key: key, Value: val}, &consulapi.WriteOptions{})
	if err != nil {
		return fmt.Errorf("consul: write %s: %w", key, err)
	}
	return nil
}

// Resolve 读服务注册表，返回全部 host:port（无此服务返回空 slice 而非 error）。
// 调用方用 strings.Join(addrs, ",") 得到 kitex ClientOptions.Target。
func Resolve(agentAddr, serviceID string) ([]string, error) {
	if serviceID == "" {
		return nil, fmt.Errorf("consul: empty serviceID")
	}
	c, err := newClient(agentAddr)
	if err != nil {
		return nil, err
	}
	pairs, _, err := c.KV().Get(KVKey(serviceID), nil)
	if err != nil {
		return nil, fmt.Errorf("consul: read %s: %w", KVKey(serviceID), err)
	}
	if pairs == nil || string(pairs.Value) == "" {
		return []string{}, nil
	}
	var addrs []string
	if err := json.Unmarshal(pairs.Value, &addrs); err != nil {
		return nil, fmt.Errorf("consul: parse %s: %w", KVKey(serviceID), err)
	}
	return addrs, nil
}

// Target 读注册表并拼 kitex ClientOptions.Target（逗号分隔；空表返回空串，
// 调用方应 fail fast）。
func Target(agentAddr, serviceID string) (string, error) {
	addrs, err := Resolve(agentAddr, serviceID)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("consul: no instance for %s", serviceID)
	}
	out := make([]string, 0, len(addrs))
	out = append(out, addrs...)
	return joinComma(out), nil
}

// newClient 创建 Consul 客户端（agentAddr 为空用官方默认 127.0.0.1:8500）
func newClient(agentAddr string) (*consulapi.Client, error) {
	conf := consulapi.DefaultConfig()
	if agentAddr != "" {
		conf.Address = agentAddr
	}
	return consulapi.NewClient(conf)
}

// joinComma 逗号拼接（与 strings.Join 等价，显式写出便于单测注入）
func joinComma(addrs []string) string {
	if len(addrs) == 0 {
		return ""
	}
	out := addrs[0]
	for _, a := range addrs[1:] {
		out += "," + a
	}
	return out
}
