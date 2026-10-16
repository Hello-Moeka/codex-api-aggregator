# AI Agent 部署手册

本文面向接手本仓库的 AI Agent。目标是完成一个独立、可验证、可回滚的部署。所有命令都从本仓库根目录执行。不要把命令执行成功等同于模型服务验收成功。

## 1. 部署边界与输入

- 不修改用户的 `.codex`、CC Switch 或其他客户端配置；只交付 API 地址和读取 Key 的本地方式。
- 不导入相邻生产目录中的账号、Key、配置或日志。用户指定的配置来源仅在其授权范围内读取。
- Key、OAuth token、Cookie、设备码、账号标识均不写入报告、聊天、截图或 Git。不要直接执行 `cat private/secrets.json` 或打印完整状态响应。
- 默认仅开放本机 API 18317 和控制面板 18318。局域网/API 公网开放必须有用户明确授权；控制面板没有远程登录保护，不能直接暴露到局域网或公网。
- 账号设备授权由用户在官方页面完成。Agent 不索要账号密码。
- 测试、恢复探测会消耗上游额度；先执行模拟测试，真实测试使用用户已授权的来源。
- 不执行会删除数据卷的命令。升级前备份 private/，备份不进入发行目录或仓库。

部署前确认：目标目录、操作系统与容器架构、Docker/Compose 可用性、端口是否被占用、已有同名 Compose 项目、上游地址/Key/协议/模型、是否使用官方槽位、是否授权真实测试。缺少上游配置时仍可完成空实例结构验收，但必须说明推理尚未验收。

## 2. 目录与组件

| 路径或组件 | 用途 |
| --- | --- |
| scripts/controller.py | 本地控制面板、供应商余额、模型同步、OAuth 设备登录 |
| scripts/router.py | 请求选路、来源/模型熔断、恢复探测、额度保护、元数据日志 |
| scripts/bootstrap_open_source.py | 首次生成随机客户端/管理 Key，以及缺失的配置文件；不覆盖已有文件 |
| web/ | 控制面板静态资源 |
| private/*.example.json | 可发布模板，不含真实账号 |
| private/ | 运行时凭据及数据库，不得发布 |
| docker/cli-proxy-api | 外部 CLIProxyAPI Linux 二进制，发行目录不附带 |
| init | 一次性初始化服务：生成私有配置，再生成网关配置 |
| gateway | CLIProxyAPI 协议适配与 OAuth，容器端口 18317，仅内部访问 |
| router | API 入口，容器 18319 映射宿主 18317 |
| panel | 容器 18318 映射本机 18318 |

## 3. 准备外部网关与依赖

当前适配基线为 CLIProxyAPI v8.0.31，不要未经验证升级为其他版本。官方仓库为 `https://github.com/router-for-me/CLIProxyAPI`，使用其 Releases 页面获取与 Docker Linux 架构一致的发行资产；Windows 宿主也必须使用 Linux 二进制。

下载后对照该版本 checksums.txt 验证 SHA256，再解压并将可执行文件放到 `docker/cli-proxy-api`。同时保留上游 LICENSE、版权声明与版本信息；不要把本项目的 AGPL 覆盖到第三方组件。官方网关仅作为独立外部依赖，当前源码包没有携带其二进制。

Dockerfile 已固定 Python 3.13、aiohttp 3.13.3、PyYAML 6.0.3。先执行：

```powershell
docker version
docker compose version
```

若本机已有本项目或测试版本，修改新 compose.yaml 的顶层 name 及镜像标签，选择空闲宿主端口。只修改端口映射无法修改控制面板 Host/Origin 校验；面板默认应保持 18318。并行部署面板需同步适配校验，不要临时关闭 CSRF。

## 4. 创建配置

复制 compose.example.yaml 为 compose.yaml。为了不启用示例供应商，默认 upstreams.example.json 是空数组。可以直接首次启动空实例；若已有授权的供应商参数，先复制 upstreams.example.json 为 private/upstreams.json 并填入真实值。

一个供应商示例（仅示例，不要原样启用）：

```json
[
  {
    "id": "provider-a",
    "name": "Provider A",
    "base_url": "https://provider.example/v1",
    "api_key": "REPLACE_LOCALLY",
    "api_format": "openai_responses",
    "models": [{"model": "MODEL_FROM_PROVIDER"}],
    "usage_kind": "none"
  }
]
```

字段规则：

- id 必须唯一且稳定，建议小写字母、数字和连字符，不含斜杠；name 同样保持唯一，余额记录按 name 关联。
- base_url 必须为 API 根路径，例如末尾 /v1；模型同步会追加 /models。
- api_format 为 openai_chat 时使用 Chat 兼容适配；其他值当前进入 Codex/Responses 适配。目录存在不代表完整 Responses/工具兼容，必须实测。
- models 中 model 是上游原名。目录只在手动同步时更新，不在后台自动同步。
- usage_kind 为 none 或不提供 usage_url 时，余额显示未知，不因未知直接排除来源；已有额度等待不会被未知余额解除。
- TopAPI 类余额结构使用 usage_kind: topapi，并填 usage_url 和 usage_headers；返回需为 success:true、data.balance/data.used/data.currency。
- New API 类余额使用 usage_kind: newapi，返回需为 success:true、data.quota/data.used_quota，当前按 500000 额度单位折算 1 CNY。不同站点换算不同时应先适配代码，不盲目套用。
- usage_headers 为完整的余额查询请求头对象，可能与推理 Key 不同，不假设接口或鉴权方式通用。
- disable_codex_cloaking 为 true 时可配置 headers 适配上游。默认不要额外伪造客户端头。
- 当前展示固定按 1 USD = 7 CNY，原余额判断及扣费不变。

首次运行没有 control-settings.json 时，控制器按 upstreams.json 顺序创建来源，前面默认带 Plus A/B 两个槽位。未登录槽位会被跳过，不妨碍中转站。

已有 control-settings.json 不会自动导入新增供应商。新增时同时在其 sources 数组添加：

```json
{"id":"provider-a","name":"Provider A","kind":"api","enabled":true,"models":[{"upstream":"MODEL_FROM_PROVIDER","alias":"MODEL_FROM_PROVIDER"}]}
```

两个文件中的 id/name 必须对应。变更前备份两个文件，停止当前实例的 panel/router 后修改，重新生成配置并启动。不要删除整个 control-settings.json 来导入，避免丢失启停、排序和模型配置。

## 5. 首次构建与启动

PowerShell：

```powershell
Copy-Item compose.example.yaml compose.yaml
docker compose build gateway
docker compose up -d
docker compose ps -a
```

Linux shell 将复制命令换为 `cp compose.example.yaml compose.yaml`。若 compose.yaml 已存在，先检查内容，禁止覆盖。

init 会自动运行 bootstrap，然后以 PREPARE_ONLY=1 运行控制器，生成 private/docker-config.yaml。gateway 必须等 init 成功退出后启动，router 等 gateway 健康后启动，panel 最后启动。不要跳过 init，也不要手工把示例 Key 当运行 Key。

正常现象：init 为 Exited (0)，gateway/router 为 healthy，panel 为 running。启动失败时按 init → gateway → router → panel 顺序排查；只查看必要日志，设备授权日志可能含临时设备码，不上传原始日志。

访问控制面板 `http://127.0.0.1:18318/`，在对应 Plus 槽位启动设备码登录，由用户完成授权。第三方来源在“上游模型”中手动同步目录。来源增删目前需要配置文件操作，面板支持已有来源的启停、排序、同步、测试和恢复检查。

本版本一键全部测试固定使用 gpt-6.1-sol；单来源测试可以选择目录中的其他模型。不要把不存在该模型时的一键跳过当成供应商故障。

## 6. 验收顺序

### 6.1 结构验收，不消耗上游额度

```powershell
docker compose config --quiet
docker compose ps -a
Invoke-RestMethod http://127.0.0.1:18317/health
Invoke-RestMethod http://127.0.0.1:18318/health
```

在内存读取 private/secrets.json 中 client，使用 Bearer 头访问 /v1/models，只输出状态码和模型数量，不输出 Key。无 Key 访问 /v1/models 必须返回 401。检查 18318 仅绑定回环，gateway 没有宿主发布端口。不要把 /health 可用当作推理完成。

### 6.2 模拟故障测试，不消耗上游额度

使用项目依赖的 Python 环境运行：

```powershell
New-Item -ItemType Directory reports -Force | Out-Null
$env:ROUTING_REPORT='reports/routing-verification.json'
$env:RESERVE_REPORT='reports/reserve-verification.json'
python scripts/verify_routing.py
python scripts/verify_reserve.py
python scripts/verify_recovery_timer.py
python scripts/verify_reclaim.py
python scripts/verify_audit.py
node scripts/verify_presentation.cjs
```

测试使用临时目录和模拟凭据，不连接真实供应商。报告变量是报告输出位置；部分脚本默认 /tmp，Windows 应按上述设置并确保 reports 存在。

最后一项需要 Node.js。六套检查合计 104 项，覆盖路由、额度预留、恢复计时、30 分钟保持与回迁、审计回归及状态展示。

### 6.3 真实推理验收，需要用户授权的来源

先测试一个来源、一个实际支持的模型：普通文本、流式完成、工具调用（客户端需要时）、错误和取消处理。一次简单文本成功不能宣称完整 Codex 兼容。无需遍历所有模型消耗额度。

分别确认无前缀调用的顺序回退、带来源前缀调用不跨来源。模型级无渠道只隔离该模型；超时连续 3 次或最近 10 次有 6 次失败触发来源熔断。恢复探测失败继续退避，连续两次成功恢复。额度预留在任一官方窗口剩余不超过 10% 时跳过账号。

官方恢复接管只在下一请求边界发生，不打断输出。previous_response_id 仍绑定原来源，无法安全跨来源续接时应提交完整历史。进程重启不保留会话/response_id 映射。不要用真实账号故意耗尽额度来做验收。

中转站会话保持至少 30 分钟；靠前来源恢复需要两次成功、默认 5 分钟观察和额外验证。当前来源故障立即回退；官方恢复接管不受 30 分钟限制。健康 Plus B 不会被 Plus A 主动抢占。完整规则及按钮区别见 [切换逻辑说明](docs/切换逻辑说明.md)。

报告必须区分：结构验证、模拟验证、真实上游验证、客户端验收；未测项明确标记。不要声称用户设备端可用，除非实际从该设备验证。

## 7. 升级、回滚和自动启动

升级前保存 private/ 的受限备份及旧镜像标识，确认没有正在输出的关键请求。重新构建镜像，执行 `docker compose up -d` 更新服务并复查健康和鉴权。web 绑定挂载立即生效，Python 代码必须重建镜像并重建容器。

回滚时恢复旧源码/镜像；必要时恢复相同备份的配置。不要随意回滚日志数据库。备份含凭据，禁止公开。

容器 restart: unless-stopped 表示 Docker 引擎启动后自动恢复；不等于操作系统启动时引擎必定运行。Windows Docker Desktop 通常是用户登录后启动，Linux 需检查 Docker 服务启动设置。只有实际重启验收后才能声称已验证开机自启。

## 8. 开源发布与 AGPL

仓库 LICENSE 包含完整 AGPL v3 文本；本项目采用 AGPL-3.0-or-later。保留 NOTICE 和第三方许可。发布前检查忽略规则，排除真实 private/、reports/、运行二进制、缓存及个人地址。网页图标为内联 SVG，勿额外复制未获许可的素材。

若将修改后的服务提供给他人通过网络使用，应按 AGPL 要求向这些用户提供对应版本的源代码获取方式。部署者应提供实际仓库或源码下载入口，不能填虚构仓库地址；源码应覆盖运行版本及构建安装所需脚本，不能附带私密运行配置。

## 9. 完成后的交付格式

仅交付：部署目录、实例名、API Base URL、面板本机地址、Key 的本地获取方式、官方授权完成情况、模拟/真实测试结果、启动方式、备份和回滚位置、未验收项。不要把 Key 或设备码贴到交付消息。
