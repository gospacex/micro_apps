// Package config 测试：Trace.On(kind) 的 7 种组合（矩阵 7 组合的单元测试版）。
//
// 组合来源：总闸（enabled）× 细分开关（db/cache/mq）× 官方段覆盖（resolveTraceSwitches）。
// 取值链路：Default() 打底 → trace 段显式键覆盖 → 官方段键补位（仅当 trace 键未显式写出）。
package config

import "testing"

// memSwitches 内存 Switches 实现（模拟 configx.Config 的显式键视图）
type memSwitches struct {
	set map[string]bool // key → 显式写出的值
}

func (m *memSwitches) IsSet(key string) bool {
	_, ok := m.set[key]
	return ok
}

func (m *memSwitches) GetBool(key string) bool {
	return m.set[key]
}

// explicitTraceKeys 构造 trace 段"显式写出"的键值（模拟 yaml 中出现的键）
func explicitTraceKeys(v map[string]bool, on bool) *memSwitches {
	m := &memSwitches{set: make(map[string]bool)}
	if !on {
		return m // 总闸关时细分/官方段的显式性不影响结果
	}
	for k, v := range v {
		m.set["trace."+k] = v
	}
	return m
}

// build 模拟 config.Load 的取值链路：Default → trace 显式键 → Resolve（官方段补位）
func build(explicit map[string]bool, masterOn bool, official map[string]bool) Trace {
	t := Default().Trace
	if !masterOn {
		t.Enabled = false
	}
	// trace 段显式键覆盖
	for k, v := range explicit {
		switch k {
		case "db":
			t.DB = v
		case "cache":
			t.Cache = v
		case "mq":
			t.MQ = v
		}
	}
	// 官方段补位
	ms := explicitTraceKeys(explicit, masterOn)
	for k, v := range official {
		ms.set[k] = v
	}
	t.Resolve(ms, DefaultInstance)
	return t
}

// TestTraceOnCombinations 7 组合表驱动（与 trace 矩阵逐行对应）
func TestTraceOnCombinations(t *testing.T) {
	cases := []struct {
		name     string
		explicit map[string]bool // trace 段显式写出的细分键
		masterOn bool
		official map[string]bool // 官方段显式键（未显式 trace 键时补位）
		want     map[string]bool // kind → On(kind)
	}{
		{
			name:     "全开：总闸开 + 细分全 true + 官方段全 true",
			explicit: map[string]bool{"db": true, "cache": true, "mq": true},
			masterOn: true,
			official: map[string]bool{
				SQLXTraceKey(DefaultInstance):    true,
				CacheXTraceKey(DefaultInstance):  true,
				MQXCallChainKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": true, "cache": true, "mq": true},
		},
		{
			name:     "总闸关：无论细分/官方段，一律 off",
			explicit: map[string]bool{"db": true, "cache": true, "mq": true},
			masterOn: false,
			official: map[string]bool{
				SQLXTraceKey(DefaultInstance):   true,
				CacheXTraceKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": false, "cache": false, "mq": false},
		},
		{
			name:     "db=off：trace.db 显式 false 覆盖官方段 true",
			explicit: map[string]bool{"db": false, "cache": true, "mq": true},
			masterOn: true,
			official: map[string]bool{
				SQLXTraceKey(DefaultInstance):    true, // 显式 trace.db 优先，官方段被忽略
				CacheXTraceKey(DefaultInstance):  true,
				MQXCallChainKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": false, "cache": true, "mq": true},
		},
		{
			name:     "cache=off：trace.cache 显式 false",
			explicit: map[string]bool{"db": true, "cache": false, "mq": true},
			masterOn: true,
			official: map[string]bool{
				CacheXTraceKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": true, "cache": false, "mq": true},
		},
		{
			name:     "mq=off：trace.mq 显式 false",
			explicit: map[string]bool{"db": true, "cache": true, "mq": false},
			masterOn: true,
			official: map[string]bool{
				MQXCallChainKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": true, "cache": true, "mq": false},
		},
		{
			name:     "省略细分：trace 段未写细分键 → 默认 true，官方段未写 → 保持默认",
			explicit: map[string]bool{},
			masterOn: true,
			official: map[string]bool{},
			want:     map[string]bool{"db": true, "cache": true, "mq": true},
		},
		{
			name:     "官方段关：trace 段省略 db，官方段 enable_trace=false 补位生效",
			explicit: map[string]bool{},
			masterOn: true,
			official: map[string]bool{
				SQLXTraceKey(DefaultInstance):    false,
				CacheXTraceKey(DefaultInstance):  true,
				MQXCallChainKey(DefaultInstance): true,
			},
			want: map[string]bool{"db": false, "cache": true, "mq": true},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := build(c.explicit, c.masterOn, c.official)
			for kind, want := range c.want {
				if actual := got.On(kind); actual != want {
					t.Fatalf("On(%q) = %v, want %v (enabled=%v db=%v cache=%v mq=%v)",
						kind, actual, want, got.Enabled, got.DB, got.Cache, got.MQ)
				}
			}
		})
	}
}

// TestTraceOnUnknownKind 未知 kind 跟随总开关
func TestTraceOnUnknownKind(t *testing.T) {
	off := Trace{Enabled: false}
	if off.On("custom") {
		t.Fatal("总闸关时未知 kind 应 false")
	}
	on := Trace{Enabled: true}
	if !on.On("custom") {
		t.Fatal("总闸开时未知 kind 应 true")
	}
}

// TestResolveNilSwitches nil 句柄时不做覆盖（纯 trace 段语义）
func TestResolveNilSwitches(t *testing.T) {
	tc := Trace{Enabled: true, DB: false}
	tc.Resolve(nil, "")
	if tc.DB {
		t.Fatal("nil 句柄应保持原值")
	}
}
