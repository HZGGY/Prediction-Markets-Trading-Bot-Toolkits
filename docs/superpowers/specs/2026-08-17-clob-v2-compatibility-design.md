# CLOB V2 Compatibility and EOA Signing Design

## Goal

将本地 Rust CLOB 客户端从旧版 V1 订单签名迁移到 Polymarket CLOB V2 的 EIP-712 订单格式，完成可重复的离线签名和请求体测试；本阶段不连接真实 CLOB、不读取真实私钥、不广播订单。

## Scope

- 支持 CLOB V2 的 EOA 签名类型 `0`。
- 将 `signature_type` 与 `funder_address` 作为显式账户配置，不再通过“funder 是否等于 signer”猜测账户类型。
- `SignatureType` 解析支持官方编号 `0/1/2/3`；第一阶段只有 `0` 允许进入可签名 EOA 路径，`1/2/3` 返回明确的未支持错误，不产生订单。
- 采用 V2 exchange domain：chain 137、官方 V2 exchange/neg-risk exchange 地址、domain version `2`。
- 从 Gamma `negRisk` 元数据选择 standard 或 neg-risk V2 verifying contract，并将该属性保存到纸面持仓供 TP/SL 退出订单复用。
- V2 签名结构包含 `salt`、`maker`、`signer`、`tokenId`、`makerAmount`、`takerAmount`、`side(uint8)`、`signatureType(uint8)`、`timestamp(ms)`、`metadata(bytes32)`、`builder(bytes32)`；不再将 V1 的 `taker`、`expiration`、`nonce`、`feeRateBps` 纳入签名。
- POST `/order` 仍保留用于 GTD 订单生命周期的外层 `expiration`，但不将它放入 EIP-712 digest。
- POST 请求的 `owner` 使用 L2 API Key；L2 HMAC 先将 API Secret 按 URL-safe Base64 解码，再对精确请求字符串执行 HMAC-SHA256，并输出带 padding 的 URL-safe Base64。builder HMAC 头不在本阶段实现，`builder` 使用零值 `bytes32`。
- 保留现有 dry-run、无凭证降级和订单执行安全门；默认配置继续 `enable_trading=false`、`mock_trading=true`。

## Out of Scope

- 代理钱包、Gnosis Safe、POLY_1271 的链上合约签名验证。
- 创建/派生 API 凭证的网络流程。
- pUSD 余额查询、授权、USDC.e→pUSD wrap 和真实资金检查。
- 真实 CLOB HTTP/WebSocket 验收、真实订单广播、撤单和异常恢复。
- 任何实盘配置或私钥录入。

## Architecture

### Configuration

`Credentials` 增加可选 `signature_type`，凭证文件示例明确要求 EOA 使用 `0`。加载配置时将编号解析为 `SignatureType`，未知编号立即报错；已知但第一阶段不支持的 `1/2/3` 在构造签名客户端时返回可读错误。`funder_address` 始终作为订单 `maker`，私钥派生地址作为订单 `signer`。

`ExchangeConfig` 更新到 V2 合约地址和 domain version `2`。默认的 `metadata`、`builder` 使用零值，避免引入 builder 注册或额外认证。

### Signing

将 `sol!` 定义替换为 V2 `Order`，并用固定测试时间戳、固定 salt 和固定 EOA 私钥构造确定性 digest。生产路径使用 Unix epoch milliseconds 生成 `timestamp`；`Gtc` 的外层 expiration 为 `0`，短时订单仍按现有配置产生秒级 expiration。

`SignedOrder` 只序列化 V2 POST 所需字段：`salt`、`maker`、`signer`、`tokenId`、`makerAmount`、`takerAmount`、`side`、`expiration`、`timestamp`、`metadata`、`builder`、`signatureType`、`signature`。V1 字段不得出现在 JSON 或 EIP-712 type string 中。

### Safety

`ClobClient::new` 在任何签名操作前校验：V2 exchange 配置、签名类型、signer/funder 地址格式和私钥长度。对 `1/2/3` 的错误必须是拒绝状态，而不是自动降级为 `PolyProxy`。现有 `live_trading_allowed()` 继续是唯一发送订单的安全门。

## Test Contract

测试必须先写并观察 RED，再实现 GREEN：

1. `SignatureType` 能将 `0/1/2/3` 解析为对应枚举，未知值拒绝。
2. EOA V2 构造出的 EIP-712 type string 只包含 V2 字段，且不包含 `taker`、`expiration`、`nonce`、`feeRateBps`。
3. 固定输入产生固定 V2 digest 和固定签名；改变 domain version、V2 exchange 地址、timestamp 或 metadata 时 digest 必须改变。
4. V2 JSON 请求体包含外层 `expiration` 和 V2 字段，不包含 V1 字段；`side` 为 wire 字符串 `BUY/SELL`，签名字段使用十六进制。
5. EOA `signature_type=0` 允许构造订单；`1/2/3` 在第一阶段明确失败，且不会进入 HTTP POST。
6. 现有 HMAC RFC 向量、金额单位、BUY/SELL maker/taker amount 逻辑和全量项目测试继续通过。
7. 配置解析在没有凭证的 dry-run 场景保持兼容；真实交易配置缺少显式 `signature_type` 时拒绝初始化而不是猜测。
8. Gamma `negRisk=true` 使用 neg-risk V2 exchange 签名，普通市场使用 standard V2 exchange；相同订单输入的两个签名必须不同。
9. POST `owner` 等于 API Key；固定的 Base64URL API Secret 与 prehash 产生固定的、带 padding 的 L2 HMAC 签名，无效 secret 编码必须拒绝。

## Verification Gate

完成标准：

- CLOB V2 单元测试全部通过，包含至少一个固定 digest/signature fixture。
- `cargo test --offline`、`cargo build --release --offline` 和 `cargo clippy --all-targets -- -D warnings` 通过。
- 不执行真实私钥签名，不向 `clob.polymarket.com` 发送订单请求。
- 文档和配置示例明确说明本阶段仍不是实盘授权；完成本阶段不等于资金安全验收或可直接实盘。

## References

- Polymarket V2 migration: https://docs.polymarket.com/v2-migration
- Polymarket authentication: https://docs.polymarket.com/getting-started/api#authentication
- Polymarket POST order API: https://docs.polymarket.com/api-reference/trade/post-a-new-order
- Official Rust V2 order types: https://github.com/Polymarket/rs-clob-client-v2/blob/main/src/clob/types/mod.rs
- Official Python V2 HMAC implementation: https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/signing/hmac.py
- Official Python V2 order posting: https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/client.py
