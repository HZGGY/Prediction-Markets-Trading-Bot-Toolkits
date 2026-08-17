# 预测市场工具包

<div align="center">

<img width="820" alt="Polymarket 工具包 TUI" src="https://github.com/user-attachments/assets/b6c51ba1-14c6-4582-858c-e9441516dd1d" />
<img width="820" alt="预测市场工具包 仪表盘" src="https://github.com/user-attachments/assets/2ae5783d-be8e-458d-8da4-1ff82aada3db" />

### 平台无关的预测市场交易基础设施 — 任何带订单簿的市场

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![Rust CI](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/actions/workflows/rust.yml/badge.svg)](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/actions/workflows/rust.yml)
[![Stars](https://img.shields.io/github/stars/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits?style=flat-square&color=6e40c9)](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/stargazers)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Tokio](https://img.shields.io/badge/async-tokio-blue.svg?style=flat-square)](https://tokio.rs/)
[![Live venues](https://img.shields.io/badge/已上线-7_平台-6e40c9.svg?style=flat-square)](#平台覆盖)
[![Beta venues](https://img.shields.io/badge/测试中-2_平台-f5a623.svg?style=flat-square)](#平台覆盖)
[![Roadmap](https://img.shields.io/badge/路线图-25+_平台-555.svg?style=flat-square)](#平台覆盖)

> **一套执行核心。一套风控层。覆盖所有平台。**
> 十款策略机器人运行在同一套久经实战的引擎与平台无关的适配层之上。接入一个新市场只需写**一个适配器**——而不是重建一个机器人。今天有七个平台已在生产环境上线，另有两个平台处于测试阶段并已接入实时市场数据；预测市场宇宙的其余部分都是适配器驱动的路线图。

<br/>

[![在 Telegram 联系](https://img.shields.io/badge/💬_在_Telegram_联系-@HarrierOnChain-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)
&nbsp;
[![PnL Profit 已上线](https://img.shields.io/badge/🚀_PnL_Profit-访问_pnlpro.fit-16a34a?style=for-the-badge)](https://pnlpro.fit)

**[快速开始](#-快速开始) • [策略](#策略) • [托管服务](#-托管与跟单交易抢先体验) • [平台覆盖](#平台覆盖) • [引擎](#引擎) • [安全](#安全) • [联系方式](#联系方式)**

**🌐 Language / 语言 / Язык:** [English](README.md) • [简体中文](#预测市场工具包) • [Русский](README.ru.md)

</div>

---

## 🚀 快速开始

用本工具包交易有两种方式——**自己运行**，或**让我们替你运行**。

<table>
<tr>
<td width="50%" valign="top">

### 🛠️ 自己运行机器人

开源引擎，你的密钥，你的钱包。

```bash
# 1. 克隆一个平台仓库（以 Polymarket 为例）
git clone https://github.com/HarrierOnChain/Polymarket
cd Polymarket

# 2. 配置——复制示例
cp config.example.yaml config.yaml

# 3. 先空跑（不真正下单）
cargo run --release -- run copy-trading
```

每款机器人默认 `enable_trading: false`——完整执行链路会一直空跑，直到**你**亲手打开实盘。各平台配置与图文讲解见对应的[平台仓库](#平台覆盖)。

> **本地 Polymarket CLOB V2 状态（2026-08-17）：** 已提供官方 Rust SDK L1 API 凭据创建/派生能力，并保留现有 V2 原始订单路径；当前仅支持 EOA 账户（`signature_type: 0`）。代理钱包、Safe 和 POLY_1271 会被拒绝。pUSD 资金/授权和真实订单往返测试仍是独立前置条件；默认配置继续保持 dry-run，不代表已经获准开启实盘。

#### 显式 CLOB API 凭据命令

先将 `config.yaml.example` 复制为已存在的 `config.yaml`，填入 EOA 私钥、与私钥匹配的 funder 地址和 `signature_type: 0`，然后明确选择其中一个操作：

```powershell
.\target\release\polymarket-toolkits.exe `
  --config .\config.json `
  --credentials .\config.yaml `
  auth create-api-key

.\target\release\polymarket-toolkits.exe `
  --config .\config.json `
  --credentials .\config.yaml `
  auth derive-api-key
```

这两个命令只允许连接 `https://clob-v2.polymarket.com`。Create 和 Derive 严格分离，不会静默 fallback。成功后只会原子更新既有 YAML 中的三个 API 凭据字段；终端和日志只显示脱敏后的 API Key 摘要。获取凭据不会自动开启交易，也不会关闭 mock 模式。本开发阶段只通过本地回环测试验证，未对真实 CLOB 执行任何一个命令。

</td>
<td width="50%" valign="top">

### 💼 让我们替你运行

托管账户 + 跟单交易，全程托管。无需搭建，无需管理密钥。

- 从链上排行榜挑一位**已被验证的领投者**，或挑一个策略
- 我们运行机器人；你只需盯着仪表盘
- 分级订阅 + 绩效费——[查看方案](#-托管与跟单交易抢先体验)

> 🧪 **处于抢先体验测试阶段（纸面交易）。** 目前为模拟资金；托管实盘交易正在向候补名单逐步开放。

**[→ 在 Telegram 加入抢先体验候补名单](https://t.me/HarrierOnChain)**

</td>
</tr>
</table>

---

## 数据一览

<div align="center">

| ⭐ Star | 🍴 Fork | 🟢 已上线平台 | 🎯 策略 | ⚙️ 引擎 | 🧪 空跑 |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **359+** | **239+** | **7**（+2 测试中） | **10** | **Rust · <1ms/事件** | **全链路** |

*只用真实、诚实的信号——上方的 [GitHub Star](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/stargazers)、CI 状态与平台数量都可自行核验。没有虚假好评，没有挑拣过的 P&L。*

</div>

---

## 策略

完整的十款生产级交易机器人组合，每一款都围绕一个清晰、独立的市场优势精心打造。所有策略共享同一套久经实战的执行核心、风控层与平台无关的适配层——你获得的是一致的性能表现、统一的风险控制、以及覆盖全部玩法的统一运维界面。挑一个匹配你判断的优势上场；底层基础设施已经为你搭好了。

> 📦 **完整的图文讲解、截图与各平台配置都放在每个市场各自的专属仓库里** —— 目录见 [平台覆盖](#平台覆盖)。下表是策略索引；每款机器人都运行在共享引擎与[安全层](#安全)之上，并完整支持空跑模式。

| # | 策略 | 一句话优势 | 关键规格 |
|---|------|-----------|----------|
| 1 | 🎯 **跟单交易** | 镜像已被证明拥有 alpha 的钱包 | 多钱包 · FAK/GTD · 熔断器 |
| 2 | ⚡ **BTC 5m / 15m / 1h 套利** | 短窗口 BTC 涨跌上的速度优势 | ~42ms 端到端 · FAK |
| 3 | 💰 **跨平台套利** | 锁价差，不锁方向 | Polymarket ↔ Kalshi ↔ PredictIt · 对冲双腿 |
| 4 | 🎯 **方向性套利** | 套利底仓（Up + Down < $1），再向更有优势的一侧倾斜 | 对冲底仓 · 仅限价单 |
| 5 | 📈 **价差耕作** | 一千次 0.5¢ 小胜复利成大数字 | 买卖价差捕获 · 单笔 P&L |
| 6 | 🏆 **体育执行** | 点击。成交。完成——不到 50ms | NBA / NFL / 足球 · &lt;50ms FAK |
| 7 | 🎯 **结算狙击** | 95¢ 近确定性 → 确定的 $1.00 派息 | 确定性扫描 · 持有至结算 |
| 8 | 📊 **订单簿失衡** | 信号本身就是订单簿——无需外部数据源 | 实时 OBI · 500ms 刷新 |
| 9 | 💰 **做市商** | 当庄家，不当赌客 | 双边 GTD · 库存倾斜 |
| 10 | ⚡ **链上鲸鱼信号** | 比公开仓位 API 早 3–30 秒 | Polygon 区块订阅 · ABI calldata 解码 |

<details>
<summary><b>几款旗舰优势的实际原理</b>（点击展开）</summary>

<br/>

**🎯 跟单交易 ——** 把机器人指向一个或多个链上战绩过硬的钱包，它会按你设定的规模镜像其成交，配有每钱包上限、FAK/GTD 订单类型，以及在异常爆发时暂停的熔断器。搭配[链上排行榜](#-托管与跟单交易抢先体验)来挑选跟谁。

**💰 跨平台套利 ——** 同一个现实问题常常同时挂在 Polymarket、Kalshi *和* PredictIt 上，价格略有差异。引擎会在各平台间**严格匹配同一份合约**（严格匹配——不制造虚假配对），并**仅在价差覆盖来回手续费时**才捕获它。跨平台市场大多是有效的，所以这是耐心游戏：它等待真正的错位，而不是硬凑交易。

**🎯 方向性套利 ——** 当 Yes + No 组合价低于 \$1 时买入（结构性套利底仓），再把额外仓位向更有上行空间的一侧倾斜。仅限价单、对冲底仓——用结构而非直觉来提升期望值。

**🎯 结算狙击 ——** 扫描近乎确定（如 95¢+）、市场实质已定但尚未派息的合约，持有到 \$1.00。高胜率、单笔低收益——靠成交量复利，而不是靠大幅波动。

**📊 订单簿失衡 ——** 无外部数据源、无预言机：信号本身就是订单簿。近盘口买卖深度的倾斜成为短线方向判断，每 500ms 刷新一次。

</details>

<div align="center">

💬 **想针对你的平台或资金规模详解某个策略？** → **[t.me/HarrierOnChain](https://t.me/HarrierOnChain)**

</div>

---

## 💼 托管与跟单交易（抢先体验）

**不想自己运维基础设施？** 把同一套引擎当作服务来用。开一个托管账户，挑一位已被验证的领投者或一个策略，让托管机器人替你运行——你只需在实时仪表盘上看余额、P&L 和费用的变化。

> 🧪 **状态：抢先体验测试阶段——纸面交易（模拟资金）。** 你今天就能零风险地体验完整产品、排行榜与费用经济模型。**使用真实资金的托管*实盘*交易由候补名单管控，尚未开放**——托管、安全审计与合规牌照优先。在这些完成之前，我们绝不碰真钱。

### 你能获得什么

| | |
|---|---|
| 📈 **链上排行榜** | 真实的 Polymarket 钱包按可核验的**链上 P&L**排名（利润或成交量，1 天 / 7 天 / 30 天 / 全期）。一键跟单已被验证的交易者。 |
| 🤖 **托管策略机器人** | 同一套十策略引擎，替你运行。无密钥、无服务器、无运维。 |
| 💰 **跨平台套利** | **Polymarket ↔ Kalshi ↔ PredictIt** 的实时价格，并以 Manifold 作为虚拟币共识信号。 |
| 🛡️ **同一套安全层** | 熔断器、深度护卫、下单底线——来自开源引擎的护栏，同样应用于每个托管账户。 |

### 抢先体验方案

| 方案 | 价格 | 绩效费 | 适合谁 |
|---|---|---|---|
| 🆓 **Starter** | 免费 | — | 在**纸面模式**下零风险学习机器人 |
| 🔥 **Pro** | \$49 / 月 | 10%（高水位线） | 想要托管机器人 + 更多策略的自主交易者 |
| 💎 **Managed** | \$199 / 月 | 20%（高水位线） | 全策略跟单、彻底放手 |

*绩效费采用**高水位线**——只对超过历史峰值的新利润收费，绝不对你自己的入金或回撤修复收费。所示价格为抢先体验与纸面测试期定价。*

<div align="center">

[![加入候补名单](https://img.shields.io/badge/🚀_加入抢先体验候补名单-Telegram-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

</div>

---

## 平台覆盖

引擎与平台无关：任何对外提供订单簿或仓位数据的平台，都能通过单个适配器接入。
当前有七个平台**已在生产环境上线**；预测市场的其余版图都在适配器驱动的路线图上。

**图例：** 🟢 已上线 · 🟡 测试中（适配器调试中） · ⚪ 路线图（适配器驱动）

> 🟡 **测试中 = 已接入并验证的实时价格数据，已连入跨平台套利引擎；完整策略执行仍在测试。**
> PredictIt（真钱）与 Manifold（虚拟币共识信号）现已与 Polymarket、Kalshi 一并提供实时价格。

### 🟢 已上线

| 平台 | 类型 | 运行中的策略 |
|---|---|---|
| **Polymarket** | 去中心化（Polygon / pUSD） | 全部 10 款 — 完整覆盖 |
| **Kalshi** | CFTC 监管（美国） | 跨平台套利 · 结算狙击 · OBI · 做市 · 方向性套利 · 价差耕作 · 体育 |
| **Limitless** | 链上订单簿 | 结算狙击 · OBI · 价差耕作 |
| **Drift BET** | Solana | BTC 套利 · OBI · 做市 · 鲸鱼信号 |
| **Augur** | 以太坊 | 结算狙击 · OBI |
| **Azuro** | 去中心化协议 | 体育 · OBI |
| **Myriad Markets** | 加密 | OBI · 方向性套利 |

### 传统 / 合规平台

| 平台 | 类型 | 状态 | 最适配的策略 |
|---|---|---|---|
| **Robinhood Predictions** | 券商集成 | ⚪ 路线图 | 方向性套利 · 体育 |
| **Crypto.com Predictions** | 加密集成 | ⚪ 路线图 | BTC 套利 · 方向性套利 |
| **OG.com** | 社交 / 多结果 | ⚪ 路线图 | 体育 · OBI · 做市 |
| **DraftKings Predictions** | 体育 | ⚪ 路线图 | 体育执行 |
| **FanDuel Predicts** | 体育 | ⚪ 路线图 | 体育执行 |
| **Fanatics Markets** | 体育 / 娱乐 | ⚪ 路线图 | 体育执行 |
| **Interactive Brokers ForecastTrader** | 金融事件 | ⚪ 路线图 | 结算狙击 · 价差耕作 · 做市 |
| **PredictIt** | 学术 / 美国政治 | 🟡 测试中 | **跨平台套利——实时价格数据** · 结算狙击（仅研究，有下注上限） |

### 加密 / 去中心化平台

| 平台 | 链 / 类型 | 状态 | 最适配的策略 |
|---|---|---|---|
| **Hedgehog Markets** | Solana / 社交 | ⚪ 路线图 | 跟单交易 · 方向性套利 |
| **Zeitgeist** | Polkadot | ⚪ 路线图 | OBI · 做市 |
| **Projection Finance** | 波动率 / 模拟 | ⚪ 路线图 | 方向性套利 · 价差耕作 |
| **Better Fan** | 体育 / 电竞 | ⚪ 路线图 | 体育执行 |
| **Manifold Markets** | 虚拟币（玩乐性质） | 🟡 测试中 | **共识信号——实时概率数据** · 方向性套利回测 |

> **想优先接入某个平台？** 适配器开发是需求驱动的——如果你交易的平台尚未上线，
> [在 Telegram 联系我](https://t.me/HarrierOnChain)，它就能往队列前面挪。

---

## 引擎

Rust 编写，基于 Tokio 异步，一套执行核心支撑所有策略与所有平台。适配层意味着接入新市场只需一个适配器——而不是一个新机器人。

### 性能

| | |
|---|---|
| **事件处理** | 每个事件 < 1ms |
| **下单执行** | 端到端 < 100ms |
| **仓位轮询** | 每个钱包约 200ms |
| **内存占用** | 基线约 50MB |
| **CPU** | 现代硬件下 < 5% |
| **并发** | 信号量限速（默认：25 请求 / 10 秒） |

---

## 安全

| | |
|---|---|
| **熔断器** | 在配置窗口内出现 N 笔连续大额成交后自动暂停 |
| **深度护卫** | 每笔下单前校验订单簿流动性 |
| **空跑模式** | 完整执行链路运行但不真正下单 |
| **下单底线** | 强制最小交易额，避免负 EV 微交易 |

熔断器在连续大额交易超过阈值，或订单簿深度低于下限时触发。一旦触发，执行将被屏蔽至冷却期结束。触发状态与冷却时间会被记录并显示在 TUI 中。

**建议：**

| 阶段 | 操作 |
|------|------|
| 初始部署 | 用 `enable_trading: false` 至少跑完一整轮观察 |
| 首次实盘 | 在信任信号前，将 `copy_percentage` 保持在 5–10% |
| 长期运行 | 关注熔断器触发事件——它们会暴露执行异常 |
| 生产环境 | 使用专用钱包，仅放入你计划部署的资金 |

---

## 联系方式

项目正在持续维护与开发中。无论你想**运行机器人**、**加入托管抢先体验候补名单**、请求**新的平台适配器**，还是想聊聊 Polymarket 工具与算法策略——都欢迎联系。

<div align="center">

[![在 Telegram 联系](https://img.shields.io/badge/💬_Telegram-@HarrierOnChain-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

| 平台 | 链接 |
|------|------|
| **Telegram** | [t.me/HarrierOnChain](https://t.me/HarrierOnChain) |
| **讨论区** | [GitHub Discussions](../../discussions) |

*响应时间通常在数小时内。欢迎提问、反馈、平台请求与正经合作。*

</div>

---

## 免责声明

> 在预测市场交易涉及真实的财务风险。本软件按"原样"提供，不附带任何形式的担保或对结果的保证，且不构成投资建议。投入真实资金前，请务必先以 `enable_trading: false` 进行充分测试。**托管 / 跟单交易服务处于抢先体验测试阶段，运行于纸面模式（模拟资金）**——它不托管真实资金，任何实盘上线都将先行完成托管、审计与合规牌照。请确保遵守各平台的服务条款以及你所在司法管辖区的相关法规。

---

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)
[![Telegram](https://img.shields.io/badge/💬_Telegram-@HarrierOnChain-229ED9?style=flat-square&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

**为 Polymarket、Kalshi、Limitless 等预测市场社区而构建**

[返回顶部](#预测市场工具包)

</div>

[机器人的力量](http://x.com/theparuchh/status/2053766299281416621)
