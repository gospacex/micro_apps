// Package nacos 封装 hubx/configx 的远程 Nacos 装配（装配总公式第 1 步）。
//
// 语义（已对 configx 源码核实，注释与原计划文字冲突时以本文件为准）：
//   - **key 级裁决**：Provider.Get 远程有的 key 用远程值，没有则逐 key 回本地
//     viper 兜底（"本地 config.yaml 兜底"不是整文件级，是 key 级）
//   - **单实例单 dataId**：共享配置 = 11 个服务 data_id 都指向 hubx.trace，
//     服务私有段放本地 config.yaml
//   - **远程开启但 Nacos 不可用 → 直接报错**（configx.New 不降级）
//   - **无自动热更新落地**：SDK 有 Watch（Nacos 长轮询，回调 key 级），
//     但各 SDK 不自动重建 DB/MQ/缓存连接池——本案例按"改配置须重启"设计，
//     重启脚本重新走 Load
package nacos

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gospacex/configx"
)

// 默认 Nacos 地址（docker compose 的 nacos 服务）；可用环境变量覆盖
const (
	EnvNacosServer   = "HUBX_NACOS_SERVER" // "127.0.0.1:8848" 形式
	DefaultNacosIP   = "127.0.0.1"
	DefaultNacosPort = 8848
)

// Load 装配配置句柄：本地 localPath（yaml）兜底 + 远程 Nacos（dataID/group）
// 按 key 级裁决覆盖。
//
// 参数：
//   - ctx：用于检查调用方取消（Nacos 拉取阻塞在 configx.New 内部，
//     ctx 仅做前置守卫；句柄的 Close 由调用方负责）
//   - localPath：本地 yaml 路径（必填，服务私有段 + 官方 xmiddleware 段都在这里）
//   - dataID/group：远程共享配置；dataID 为空时**纯本地**（远程不启用），
//     本地必须完整（含 trace 段）
//
// 远程启用且 Nacos 不可达时返回 error（不降级——避免"以为用了远程
// 实际全本地"的静默漂移）。
func Load(ctx context.Context, localPath, dataID, group string) (configx.Config, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if strings.TrimSpace(localPath) == "" {
		return nil, fmt.Errorf("nacos loader: localPath required")
	}

	opts := &configx.ConfigOptions{
		ConfigFile: localPath,
		ConfigType: "yaml",
	}

	dataID = strings.TrimSpace(dataID)
	if dataID != "" {
		if strings.TrimSpace(group) == "" {
			group = "HUBX"
		}
		opts.Remote = configx.RemoteConfig{
			Enabled:  true,
			Provider: "nacos",
			Nacos: configx.NacosConfig{
				ServerConfigs: nacosServers(),
				Group:         group,
				DataID:        dataID,
			},
		}
	}

	cfg, err := configx.New(opts)
	if err != nil {
		if dataID != "" {
			return nil, fmt.Errorf("nacos loader: remote config (dataId=%s group=%s): %w", dataID, group, err)
		}
		return nil, fmt.Errorf("nacos loader: local config %s: %w", localPath, err)
	}
	return cfg, nil
}

// nacosServers 解析 Nacos 地址：env HUBX_NACOS_SERVER（"ip:port"）优先，
// 缺省 127.0.0.1:8848
func nacosServers() []configx.NacosServerConfig {
	addr := os.Getenv(EnvNacosServer)
	if addr == "" {
		addr = DefaultNacosIP + ":" + strconv.Itoa(DefaultNacosPort)
	}
	host, portStr, _ := strings.Cut(addr, ":")
	if host == "" {
		host = DefaultNacosIP
	}
	port := uint64(DefaultNacosPort)
	if p, err := strconv.ParseUint(portStr, 10, 64); err == nil && p > 0 {
		port = p
	}
	return []configx.NacosServerConfig{{IP: host, Port: port}}
}
