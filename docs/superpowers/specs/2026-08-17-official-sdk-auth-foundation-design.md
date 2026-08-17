# Official SDK Authentication Foundation Design

## Goal

完整迁移到 Polymarket 官方 Rust V2 SDK 分为三个可独立验收的阶段。本设计只覆盖第 1 阶段：引入官方 SDK 的 L1 API 凭据创建/派生能力，提供明确的 CLI，安全更新现有 `config.yaml`，并将公共 HTTP 配置切换到官方 V2 主机。本阶段不替换现有订单执行路径，不运行真实认证命令，不发送订单。

后续阶段分别为：

1. 用官方 SDK 替换自定义订单构造、签名、HMAC 和 POST 路径，同时保留 dry-run 与风险安全门。
2. 增加 pUSD 余额/授权只读检查、订单查询、撤单和异常恢复，再进行无资金真实鉴权验收。

## Confirmed Decisions

- 使用 `polymarket_client_sdk_v2` 的官方 L1 认证实现，不自行重复实现 `ClobAuth` EIP-712。
- 采用独立认证适配层，不增加自定义/SDK 双后端开关，也不提前抽象完整交易 trait。
- 新增可直接运行的创建和派生 CLI，但本阶段只用离线测试验证，不对真实 CLOB 执行。
- 成功取得凭据后原子更新现有 `config.yaml`；不在 stdout、日志或错误上下文中输出完整 API Key、Secret、Passphrase 或私钥。
- `create-api-key` 与 `derive-api-key` 保持显式、独立，不使用静默 create-or-derive fallback。
- 公共 HTTP 主机更新为官方 V2 `https://clob-v2.polymarket.com`；WebSocket 主机本阶段不改。
- 现有自定义 `clob.rs` 订单路径、EOA-only 规则、dry-run、风控和下单安全门本阶段不改。

## Scope

### In Scope

- 增加官方 Rust V2 SDK CLOB 依赖。
- 新建 SDK L1 认证适配层。
- 新增 `auth create-api-key` 与 `auth derive-api-key` CLI，支持可选 `--nonce <u32>`，默认 nonce 为 `0`。
- 认证成功后校验响应字段并安全更新 `--credentials` 指定的既有 YAML 文件。
- 修正 `config.json` 与 `config.dryrun-public.json` 的 CLOB HTTP V2 主机。
- 使用公开测试私钥、固定输入和本地回环服务器完成离线测试。
- 更新 README、凭据示例和 Obsidian 项目记录。

### Out of Scope

- 本阶段不调用真实 CLOB API，不创建或派生真实凭据。
- 不替换现有 V2 订单 EIP-712、L2 HMAC、POST、持仓或 TP/SL 逻辑。
- 不实现 Proxy、Safe 或 POLY_1271 认证；仍仅允许 EOA `signature_type=0`。
- 不查询余额或授权，不批准 token，不 wrap pUSD，不下单、不撤单。
- 不自动启用 `enable_trading`，不关闭 `mock_trading`。
- 不推送到远端，不执行全仓格式化。

## Architecture

### Dependencies

`Cargo.toml` 增加官方 `polymarket_client_sdk_v2` 0.6 系列并启用 CLOB 功能。该 SDK 的最低 Rust 版本是 1.88；本地 Rust 1.97.1 满足要求。现有 Alloy 0.8/Signer 0.5 继续服务自定义订单路径；官方 SDK 的 Alloy 类型被限制在新认证模块内部，避免跨版本类型扩散。

增加 `tempfile` 用于在凭据文件同目录创建临时文件并原子持久化。临时文件和目标文件位于同一文件系统，避免跨文件系统 rename 失去原子性。

### Authentication Adapter

新建 `src/service/clob_auth.rs`，公开一个窄接口：

```rust
pub enum ApiKeyAction {
    Create,
    Derive,
}

pub struct AuthRequest {
    pub action: ApiKeyAction,
    pub nonce: Option<u32>,
}

pub async fn obtain_api_credentials(
    cfg: &AppConfig,
    request: AuthRequest,
) -> anyhow::Result<polymarket_client_sdk_v2::auth::Credentials>;
```

适配层从 `AppConfig.credentials.private_key` 构造官方 SDK 的 `LocalSigner`，显式设置 Polygon chain ID `137`，并验证 signer 地址与 EOA funder 相同。它使用 `cfg.site.clob_api_base` 创建 SDK client，根据 `ApiKeyAction` 只调用一个明确端点：

- `Create` → `POST /auth/api-key`
- `Derive` → `GET /auth/derive-api-key`

生产 CLI 在调用适配层前必须验证主机精确等于 `https://clob-v2.polymarket.com`。测试通过模块内部的可注入 host/signer helper 指向 `127.0.0.1` 回环服务器；该 helper 不暴露给普通 CLI。

官方 SDK 的 `Credentials` 使用 `SecretString` 保存 Secret 和 Passphrase，默认 Debug 输出保持脱敏。适配层不把这些字段复制到可 Debug 的普通结构。

### CLI

`src/main.rs` 增加嵌套子命令：

```text
polymarket-toolkits auth create-api-key [--nonce N]
polymarket-toolkits auth derive-api-key [--nonce N]
```

全局 `--config` 和 `--credentials` 参数继续生效。运行认证命令本身视为用户对该次 L1 外部调用的显式授权；它不代表启用交易。

CLI 流程：

1. 加载公共配置和现有凭据文件。
2. 校验 EOA 私钥、funder、signature type 和官方 V2 host。
3. 调用认证适配层。
4. 校验 API Key 可序列化，Secret 和 Passphrase 非空。
5. 原子更新凭据文件。
6. 输出保存路径、signer 地址和脱敏 API Key；不输出完整凭据。

CLI 不使用 create-or-derive fallback。服务返回“已存在”等状态错误时，`create-api-key` 直接失败并建议用户显式运行 `derive-api-key`，但不会自动发起第二次请求。

### Credential Persistence

`src/config.rs` 增加一个专用于更新既有凭据文件的函数。函数要求目标文件已经存在且可解析为当前 `config.yaml` schema；不存在时返回错误并提示先复制 `config.yaml.example`。

持久化流程：

1. 读取原文件完整字节，用于失败不变性测试。
2. 解析现有 bot 凭据结构。
3. 只替换 `api_key`、`api_secret`、`api_passphrase`。
4. 验证 private key、funder 和 signature type 仍存在且未改变。
5. 在目标文件同目录创建命名临时文件。
6. 写入完整 YAML，flush，并执行 `sync_all`。
7. 继承目标文件的标准权限属性。
8. 使用 `NamedTempFile::persist` 原子替换目标。

任何验证、序列化、写入、flush、sync 或 persist 错误都在替换前返回；原文件不删除、不截断。流程不创建包含凭据的备份文件，成功后不得遗留临时文件。

YAML 重新序列化不承诺保留注释，但必须保留 schema 中全部字段和值。未知顶层或 bot 字段不得静默丢失：若当前 typed schema 无法无损承载文件内容，则本阶段应使用 `serde_yaml::Value` 定位并更新 `bot` 下三个 API 字段，同时保留其他 mapping。

## Data Flow

```text
CLI auth command
  → AppConfig::load(existing public config + existing credentials YAML)
  → exact V2 host / EOA validation
  → clob_auth adapter
  → official SDK L1 EIP-712 headers
  → one create OR derive request
  → SDK Credentials (SecretString)
  → validate non-empty fields
  → same-directory temporary YAML
  → flush + sync + atomic persist
  → redacted success message
```

没有任何步骤会修改交易开关、启动 bot 或调用订单端点。

## Error Handling and Security

- 凭据文件不存在、不是文件、YAML 无效或缺少既有账户字段：调用前失败，不发请求。
- 主机不是精确的官方 HTTPS V2 host：生产 CLI 失败，不产生 L1 签名或请求。
- 私钥无效、funder 不匹配、signature type 不是 EOA：调用前失败。
- HTTP 或 SDK 错误：不写凭据文件。错误文案只包含状态、方法和端点，不拼接成功响应或凭据 Debug。
- API Key 无效、Secret/Passphrase 为空：不写凭据文件。
- 文件更新失败：原文件字节保持不变；临时文件对象负责清理未持久化文件。
- 成功日志不输出完整 API Key；只显示固定长度前后缀。Secret、Passphrase 和私钥从不进入 tracing fields。
- `config.yaml` 已在 `.gitignore` 中；测试和安全扫描继续确认没有凭据被加入 Git。
- 认证成功不会修改 `enable_trading=false` 或 `mock_trading=true`。

## Testing

所有新增行为遵循 RED → GREEN TDD，测试不访问互联网。

### Adapter Tests

- 使用官方公开测试私钥、固定 timestamp/nonce 和官方已发布签名向量验证 L1 header wiring。
- 本地回环服务器确认 Create 使用 `POST /auth/api-key`，Derive 使用 `GET /auth/derive-api-key`。
- nonce 传递到 `POLY_NONCE`；地址与 signer 一致。
- Create 的 HTTP 状态失败不会调用 Derive；Derive 失败不会调用 Create。
- 成功 fixture 的 `apiKey`、`secret`、`passphrase` 能被 SDK 解析。
- 空字段或错误 JSON 被拒绝，不进入持久化。

### Persistence Tests

- 更新后只有三个 API 字段改变，private key、funder、signature type 和未知 YAML 字段保持。
- 不存在的目标文件被拒绝且不创建新文件。
- 空 API Key、Secret 或 Passphrase 被拒绝，原字节不变。
- 模拟写盘或 persist 失败时原字节不变。
- 成功后目标同目录没有本次操作遗留的临时文件。
- 凭据相关结构的 Debug/Display 和 CLI summary 不含 fixture Secret/Passphrase/完整 API Key。

### CLI and Configuration Tests

- Clap 能解析两个认证子命令及 `--nonce`。
- 认证 CLI 拒绝非官方 host；模块内部测试 helper 仍可使用回环 host。
- `config.json` 和 `config.dryrun-public.json` 的 `clob_api_base` 是 `https://clob-v2.polymarket.com`。
- 两份公共配置仍为 `enable_trading=false`、`mock_trading=true`，且没有 API 凭据。
- CLI help 明确认证命令会联网但不会下单。

### Regression Gates

- `cargo test --offline`
- `cargo build --release --offline`
- `cargo clippy --all-targets -- -D warnings`
- `git diff --check`
- 安全扫描确认没有真实私钥/API 凭据、没有开启实盘。
- 现有 47 项订单、HMAC、dry-run 和回放测试继续通过。
- `cargo fmt --check` 继续执行并记录；仓库既有格式差异不通过全仓格式化修复，本阶段只保证新改动不扩大格式差异。

## Documentation

- README 中英文版增加认证 CLI、显式 create/derive、原子写盘和脱敏行为说明。
- `config.yaml.example` 说明必须先复制为既有 `config.yaml`，认证命令不会自动创建私钥文件。
- 文档明确本阶段没有真实运行认证命令，没有产生真实 API 凭据，也不代表可以实盘。
- 完成后更新 Obsidian 项目记录，只保存稳定状态、测试结果和后续门禁，不保存任何凭据或原始认证输出。

## Acceptance Criteria

- 官方 SDK L1 认证适配层、两个 CLI 和原子凭据更新均有离线测试。
- 生产 CLI 只能向官方 V2 host 发出 L1 请求。
- 完整凭据不会出现在 stdout、日志、Debug、Git 或 Obsidian。
- V2 HTTP host 已修正，默认交易安全开关保持锁定。
- 所有离线测试、Release build、严格 Clippy 和 diff 检查通过。
- 本阶段未访问真实 CLOB，未创建/派生真实凭据，未发送订单。

## References

- Official Rust V2 SDK: https://github.com/Polymarket/rs-clob-client-v2
- Official Rust SDK authentication implementation: https://github.com/Polymarket/rs-clob-client-v2/blob/main/src/auth.rs
- Official Rust SDK API-key tests: https://github.com/Polymarket/rs-clob-client-v2/blob/main/tests/auth.rs
- Official Rust SDK create-or-derive example: https://github.com/Polymarket/rs-clob-client-v2/blob/main/examples/clob/keys/create_or_derive_api_key.rs
- Official Python V2 L1 headers: https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/headers/headers.py
- Official Python V2 L1 signing: https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/signing/eip712.py
