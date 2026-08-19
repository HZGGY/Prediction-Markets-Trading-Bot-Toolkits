# Prediction Market Toolkits

<div align="center">

<img width="820" alt="Polymarket Toolkits TUI" src="https://github.com/user-attachments/assets/b6c51ba1-14c6-4582-858c-e9441516dd1d" />
<img width="820" alt="Prediction Market Toolkits dashboard" src="https://github.com/user-attachments/assets/2ae5783d-be8e-458d-8da4-1ff82aada3db" />

### Venue-agnostic prediction-market trading infrastructure — any market with an order book

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![Rust CI](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/actions/workflows/rust.yml/badge.svg)](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/actions/workflows/rust.yml)
[![Stars](https://img.shields.io/github/stars/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits?style=flat-square&color=6e40c9)](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/stargazers)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Tokio](https://img.shields.io/badge/async-tokio-blue.svg?style=flat-square)](https://tokio.rs/)
[![Live venues](https://img.shields.io/badge/live-7_venues-6e40c9.svg?style=flat-square)](#venue-coverage)
[![Beta venues](https://img.shields.io/badge/beta-2_venues-f5a623.svg?style=flat-square)](#venue-coverage)
[![Roadmap](https://img.shields.io/badge/roadmap-25+_venues-555.svg?style=flat-square)](#venue-coverage)

> **One execution core. One risk layer. Every venue.**
> Ten strategy bots run on a single battle-tested engine and a venue-agnostic adapter stack. Adding a market means writing **one adapter** — not rebuilding a bot. Seven venues are live in production today, two more are in beta with live market data, and the rest of the prediction-market universe is adapter-driven roadmap.

<br/>

[![Chat on Telegram](https://img.shields.io/badge/💬_Chat_on_Telegram-@HarrierOnChain-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)
&nbsp;
[![PnL Profit — live](https://img.shields.io/badge/🚀_PnL_Profit-Live_at_pnlpro.fit-16a34a?style=for-the-badge)](https://pnlpro.fit)

**[Quick Start](#-quick-start) • [Strategies](#strategies) • [Managed Service](#-managed--copy-trading--early-access) • [Venue Coverage](#venue-coverage) • [Engine](#engine) • [Safety](#safety) • [Contact](#contact)**

**🌐 Language / 语言 / Язык:** [English](#prediction-market-toolkits) • [简体中文](README.zh-CN.md) • [Русский](README.ru.md)

</div>

---

## 🚀 Quick Start

Two ways to trade with the toolkit — **run it yourself**, or **let us run it for you**.

<table>
<tr>
<td width="50%" valign="top">

### 🛠️ Run the bots yourself

Open-source engine, your keys, your wallet.

```bash
# 1. Grab a venue repo (Polymarket shown)
git clone https://github.com/HarrierOnChain/Polymarket
cd Polymarket

# 2. Configure — copy the example
cp config.example.yaml config.yaml

# 3. Dry-run first (no real orders)
cargo run --release -- run copy-trading
```

Every bot ships with `enable_trading: false` by default — the full execution path runs in dry-run until *you* flip it. Per-venue configs and walkthroughs live in each [venue repo](#venue-coverage).

> **Local Polymarket CLOB V2 status (2026-08-18):** the official Rust V2 SDK 0.6 is the only local production path for order build, signing, L2 authentication, and POST. It is available for EOA accounts only (`signature_type: 0`); proxy, Safe, and POLY_1271 accounts are rejected. Copy entries and TP/SL exits are true FOK orders. The committed defaults remain strict paper mode and are not live-trading authorization.

#### Explicit CLOB API credential commands

After copying `config.yaml.example` to an existing `config.yaml` and filling the EOA private key, matching funder address, and `signature_type: 0`, choose exactly one operation:

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

These commands contact only `https://clob-v2.polymarket.com`. Create and derive are explicit and never silently fall back to each other. If `PM_PRIVATE_KEY` or `PM_FUNDER_ADDRESS` is set, its effective account must match the account already stored in the target YAML; otherwise the command stops before signing or networking. On success, only the three API credential fields in the existing YAML are atomically updated; terminal/log output shows only a redacted API-key summary. HTTP failures expose only safe status/method/path details, and a create conflict tells you to run `derive-api-key` explicitly. Obtaining credentials does not enable trading or disable mock mode. The tested SDK dependency graph is fixed by the tracked `Cargo.lock`. Phase 2 validated the flow with local loopback tests only and did not execute either command against the real CLOB.

**Phase-2 execution boundary:** strict paper mode neither signs nor calls any CLOB endpoint, including midpoint. Only an exact fully matched SDK response updates local positions. Any uncertain result writes the persistent `execution-halt.json` marker, blocks every later entry and exit, and is never retried. Phase 2 did not authorize live trading.

#### Phase 3A durable recovery operator boundary — offline/loopback acceptance only

Phase 3A adds a fail-closed local recovery ledger; it **does not authorize live trading, real-funds use, real credentials, or public-endpoint recovery**. Its implementation and acceptance use offline tests and loopback fixtures only. Do not run the recovery commands below against a production host or with real credentials as a Phase 3A acceptance step.

The authoritative ledger path is `trading.execution_ledger_path` (default: `execution-ledger.jsonl`). Its sibling active snapshot and lock are derived as `<ledger>.active.json` and `<ledger>.lock`; they are not independently configurable. Never delete, truncate, edit, replace, or “repair” the JSONL ledger or active snapshot. Never delete or edit `execution-halt.json` to resume: marker removal cannot resolve an active ledger intent, and manual changes can leave the process safely halted.

Global options are parsed before `recovery`. Use placeholders only; `--credentials`, when present, is also global and must precede `recovery`.

```powershell
# Local only: public configuration and the lock-bearing ledger; credentials are ignored and not loaded.
.\target\release\polymarket-toolkits.exe --config <public-config.json> recovery inspect [--intent <intent-id>] [--show-order-id]
.\target\release\polymarket-toolkits.exe --config <public-config.json> recovery apply --intent <intent-id> --confirm <challenge>
.\target\release\polymarket-toolkits.exe --config <public-config.json> recovery acknowledge --intent <intent-id> --confirm <challenge>

# Explicit network authority for one named exact operation only: credentials are required.
.\target\release\polymarket-toolkits.exe --config <public-config.json> --credentials <credentials.yaml> recovery reconcile --intent <intent-id>
.\target\release\polymarket-toolkits.exe --config <public-config.json> --credentials <credentials.yaml> recovery prepare-cancel --intent <intent-id>
.\target\release\polymarket-toolkits.exe --config <public-config.json> --credentials <credentials.yaml> recovery cancel --intent <intent-id> --confirm <challenge>
```

`inspect`, `apply`, and `acknowledge` load only public configuration and local durable state; they ignore credential sources and cannot construct the recovery SDK gateway. `reconcile`, `prepare-cancel`, and `cancel` require credentials and authorize only the named exact operation for that invocation. Default inspection prints an order-ID hint; a complete ID is shown only after explicit local `inspect --show-order-id`. There is no `--force`, `--yes`, retry, automatic reconcile/apply/acknowledge, restart repost, or automatic marker cleanup.

Use the state-dependent flow, never a universal happy path:

```text
inspect -> reconcile -> [prepare-cancel -> cancel -> reconcile]
                    \-> apply -> acknowledge
```

- Start with `inspect`. A locally proven `NotSent` intent may receive a fresh acknowledgement challenge. A proven exact zero-fill terminal result (`ReconciledNoFill`) may also be acknowledged with its fresh challenge.
- An exact positive, full FOK match (`ReconciledMatched`) does not change positions by itself. Run fresh `apply`, then use the new fresh challenge for `acknowledge`.
- Only a fresh exact **Live** result may enable `prepare-cancel`. One cancellation means one ledger-owned exact order, one DELETE, then mandatory exact re-query; the DELETE response is never sufficient evidence. A `Pending` result receives no cancellation challenge and remains halted.
- A 404/not-found ambiguity, partial fill, missing or mismatched fields/trades, malformed or unavailable evidence, unknown status, uncertain cancellation, or a post-cancel mismatch remains halted and cannot be acknowledged.

Startup is zero-network and non-healing. `active_unresolved` requires manual recovery/reconciliation before restart. `cleanup_pending` means resume the bounded acknowledgement cleanup only—do not re-reconcile or delete the marker. `orphan_marker` means preserve and inspect the marker; do not delete it. For a locked ledger, integrity conflict, corrupt/truncated ledger, or inconsistent snapshot, stop and follow the static diagnostic: preserve the files, identify the lock owner where applicable, and do not edit, overwrite, heal, or retry startup.

Phase 3B remains the account-capability gate (pUSD/buying power, standard/neg-risk and conditional-token allowances, account/funder/signature-type consistency, and open-order reservations). Phase 3C requires separate explicit authorization for controlled real-endpoint acceptance, including no-funds authentication and read-only checks. Phase 3D requires a separate design and explicit authorization for an isolated EOA micro-value evaluation with hard limits, per-order human confirmation, monitoring, and rollback. Until all applicable later gates are independently complete, this repository is **not live-ready**.

</td>
<td width="50%" valign="top">

### 💼 Let us run them for you

Managed accounts + copy-trading, hosted. No setup, no keys to manage.

- Pick a **proven leader** from the on-chain leaderboard, or a strategy
- We run the bots; you keep an eye on the dashboard
- Tiered subscription + performance fee — [see plans](#-managed--copy-trading--early-access)

> 🧪 **In early-access beta (paper trading).** Simulated funds today; managed live trading is rolling out to the waitlist.

**[→ Join the early-access waitlist on Telegram](https://t.me/HarrierOnChain)**

</td>
</tr>
</table>

---

## By the numbers

<div align="center">

| ⭐ Stars | 🍴 Forks | 🟢 Live venues | 🎯 Strategies | ⚙️ Engine | 🧪 Dry-run |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **359+** | **239+** | **7** (+2 beta) | **10** | **Rust · <1ms/event** | **Every path** |

*Real, honest signals only — [GitHub stars](https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits/stargazers), CI status, and venue counts you can verify above. No fake testimonials, no cherry-picked P&L.*

</div>

---

## Strategies

A complete suite of ten production-grade trading bots, each engineered around a distinct, well-defined market edge. Every strategy runs on the same battle-tested execution core, risk layer, and venue-agnostic adapter stack — so you get consistent performance, unified risk controls, and a single operational surface across every play in the book. Pick the edge that fits your thesis; the infrastructure is already built.

> 📦 **Full walkthroughs, screenshots, and per-venue configs live in each market's dedicated repo** — see [Venue Coverage](#venue-coverage) for the directory. The table below is the strategy index; every bot runs on the shared engine and [safety layer](#safety), with full dry-run support.

| # | Strategy | Edge in one line | Key spec |
|---|----------|------------------|----------|
| 1 | 🎯 **Copy Trading** | Mirror wallets that already proved they have alpha | Multi-wallet · true FOK · circuit breaker |
| 2 | ⚡ **BTC 5m / 15m / 1hr Arbitrage** | Speed on short-window BTC Up/Down | ~42ms end-to-end · FAK |
| 3 | 💰 **Cross-Market Arbitrage** | Lock the spread, not the direction | Polymarket ↔ Kalshi ↔ PredictIt · hedged legs |
| 4 | 🎯 **Directional Arbitrage** | Arb base (Up + Down < $1), then tilt toward the side with more edge | Hedged base · limit-only |
| 5 | 📈 **Spread Farming** | A thousand 0.5¢ wins compound into one number | Bid-ask capture · per-trade P&L |
| 6 | 🏆 **Sports Execution** | Click. Filled. Done — under 50ms | NBA / NFL / Soccer · &lt;50ms FAK |
| 7 | 🎯 **Resolution Sniper** | 95¢ near-certainty → guaranteed $1.00 payout | Certainty scan · hold to resolution |
| 8 | 📊 **Orderbook Imbalance** | The signal *is* the order book — no external feeds | Live OBI · 500ms refresh |
| 9 | 💰 **Market Making** | Be the house, not the gambler | Two-sided GTD · inventory skew |
| 10 | ⚡ **On-Chain Whale Signal** | 3–30s ahead of the public positions API | Polygon block sub · ABI calldata decode |

<details>
<summary><b>How the flagship edges actually work</b> (click to expand)</summary>

<br/>

**🎯 Copy Trading —** Point the bot at one or more wallets with a proven on-chain record. It mirrors their fills at your chosen scale, with per-wallet caps, true FOK order types, and a circuit breaker that halts on abnormal bursts. Pair it with the [on-chain leaderboard](#-managed--copy-trading--early-access) to pick who to follow.

**💰 Cross-Market Arbitrage —** The same real-world question is often listed on Polymarket, Kalshi *and* PredictIt at slightly different prices. The engine matches the same contract across venues (strict matching — no fuzzy false pairs), and captures the gap **only when it beats round-trip fees**. Cross-listed markets are mostly efficient, so this is a patience game: it waits for a real dislocation instead of forcing trades.

**🎯 Directional Arbitrage —** Buy the Yes + No basket while it costs under \$1 (a structural arb base), then tilt extra size onto the side with more upside. Limit-only, hedged base — structure improves expected value instead of betting on a hunch.

**🎯 Resolution Sniper —** Scan for near-certainty contracts (e.g. 95¢+) where the market has effectively resolved but hasn't paid out, and hold to \$1.00. High win-rate, low per-trade return — it compounds on volume, not on swings.

**📊 Orderbook Imbalance —** No external feeds, no oracle: the signal *is* the book. Near-touch bid/ask depth skew becomes a short-term directional read, refreshed every 500ms.

</details>

<div align="center">

💬 **Want a strategy explained for your venue or capital size?** → **[t.me/HarrierOnChain](https://t.me/HarrierOnChain)**

</div>

---

## 💼 Managed & Copy-Trading — Early Access

**Don't want to run infrastructure?** Trade the same engine as a service. Open a managed account, pick a proven leader or a strategy, and let the hosted bots run — you watch balance, P&L, and fees update on a live dashboard.

> 🧪 **Status: early-access beta — paper trading (simulated funds).** You can explore the full product, the leaderboard, and the economics today with zero capital at risk. **Managed *live* trading with real funds is gated behind the waitlist** and is not open yet — custody, security audit, and licensing come first. We will not touch real money before that's done.

### What you get

| | |
|---|---|
| 📈 **On-chain leaderboard** | Real Polymarket wallets ranked by verifiable **on-chain P&L** (profit or volume, 1d/7d/30d/all-time). One click to copy a proven trader. |
| 🤖 **Hosted strategy bots** | The same 10-strategy engine, run for you. No keys, no servers, no ops. |
| 💰 **Cross-venue arbitrage** | Live pricing across **Polymarket ↔ Kalshi ↔ PredictIt**, with Manifold as a play-money consensus signal. |
| 🛡️ **Same safety layer** | Circuit breaker, depth guard, trade floor — the guardrails from the open-source engine, applied to every managed account. |

### Early-access plans

| Plan | Price | Performance fee | Best for |
|---|---|---|---|
| 🆓 **Starter** | Free | — | Learn the bots in **paper mode**, zero risk |
| 🔥 **Pro** | \$49 / mo | 10% (high-water mark) | Self-directed traders who want hosted bots + more strategies |
| 💎 **Managed** | \$199 / mo | 20% (high-water mark) | Full copy-trading across all strategies, hands-off |

*Performance fees use a **high-water mark** — you're only charged on new profit above your prior peak, never on your own deposits or on recovering a drawdown. Pricing shown is early-access and paper-beta.*

<div align="center">

[![Join the waitlist](https://img.shields.io/badge/🚀_Join_the_Early--Access_Waitlist-Telegram-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

</div>

---

## Venue Coverage

The engine is venue-agnostic: any platform exposing an order book or position
feed plugs in through a single adapter. Seven venues are **live in production**;
the rest of the prediction-market landscape is on the adapter-driven roadmap.

**Legend:** 🟢 Live · 🟡 Beta (adapter in testing) · ⚪ Roadmap (adapter-driven)

> 🟡 **Beta = live, verified price data wired into the cross-venue arbitrage engine;
> full strategy execution still in testing.** PredictIt (real-money) and Manifold
> (play-money consensus signal) now feed live prices alongside Polymarket and Kalshi.

### 🟢 Live today

| Venue | Type | Strategies running |
|---|---|---|
| [**Polymarket**](https://github.com/HarrierOnChain/Polymarket) | Decentralized (Polygon / pUSD) | All 10 — full coverage |
| [**Kalshi**](https://github.com/HarrierOnChain/Kalshi) | CFTC-regulated (US) | Cross-arb · Resolution Sniper · OBI · Market Making · Directional Arb · Spread · Sports |
| [**Limitless**](https://github.com/HarrierOnChain/Limitless-Exchange) | On-chain order book | Resolution Sniper · OBI · Spread Farming |
| [**Drift BET**](https://github.com/HarrierOnChain/Drift-BET) | Solana | BTC Arb · OBI · Market Making · Whale Signal |
| [**Augur**](https://github.com/HarrierOnChain/Augur) | Ethereum | Resolution Sniper · OBI |
| [**Azuro**](https://github.com/HarrierOnChain/Azuro) | Decentralized protocol | Sports · OBI |
| [**Myriad Markets**](https://github.com/HarrierOnChain/Myriad-Markets) | Crypto | OBI · Directional Arb |

### Traditional / Regulated

| Venue | Type | Status | Best-fit strategies |
|---|---|---|---|
| [**Robinhood Predictions**](https://github.com/HarrierOnChain/Robinhood-Predictions) | Brokerage-integrated | ⚪ Roadmap | Directional Arb · Sports |
| [**Crypto.com Predictions**](https://github.com/HarrierOnChain/Crypto.com-Predictions) | Crypto-integrated | ⚪ Roadmap | BTC Arb · Directional Arb |
| [**OG.com**](https://github.com/HarrierOnChain/OG.com) | Social / multi-outcome | ⚪ Roadmap | Sports · OBI · Market Making |
| [**DraftKings Predictions**](https://github.com/HarrierOnChain/DraftKings-Predictions) | Sports | ⚪ Roadmap | Sports Execution |
| [**FanDuel Predicts**](https://github.com/HarrierOnChain/FanDuel-Predicts) | Sports | ⚪ Roadmap | Sports Execution |
| [**Fanatics Markets**](https://github.com/HarrierOnChain/Fanatics-Markets) | Sports / entertainment | ⚪ Roadmap | Sports Execution |
| [**Interactive Brokers ForecastTrader**](https://github.com/HarrierOnChain/Interactive-Brokers-ForecastTrader) | Financial events | ⚪ Roadmap | Resolution Sniper · Spread · Market Making |
| [**PredictIt**](https://github.com/HarrierOnChain/PredictIt) | Academic / US politics | 🟡 Beta | **Cross-Venue Arb — live price data** · Resolution Sniper (research-only, bet caps) |

### Crypto / Decentralized

| Venue | Chain / Type | Status | Best-fit strategies |
|---|---|---|---|
| [**Hedgehog Markets**](https://github.com/HarrierOnChain/Hedgehog-Markets) | Solana / social | ⚪ Roadmap | Copy Trading · Directional Arb |
| [**Zeitgeist**](https://github.com/HarrierOnChain/Zeitgeist) | Polkadot | ⚪ Roadmap | OBI · Market Making |
| [**Projection Finance**](https://github.com/HarrierOnChain/Projection-Finance) | Volatility / sims | ⚪ Roadmap | Directional Arb · Spread |
| [**Better Fan**](https://github.com/HarrierOnChain/Better-Fan) | Sports / esports | ⚪ Roadmap | Sports Execution |
| [**Manifold Markets**](https://github.com/HarrierOnChain/Manifold-Markets) | Play-money | 🟡 Beta | **Consensus signal — live probability feed** · Directional Arb backtest |

> **Want a venue prioritized?** Adapter work is demand-driven — if you trade a
> platform not yet live, [reach out on Telegram](https://t.me/HarrierOnChain) and it can move
> up the queue.

---

## Engine

Rust, async on Tokio, one execution core behind every strategy and venue. The adapter stack means a new market is one adapter — not a new bot.

### Performance

| | |
|---|---|
| **Event processing** | < 1ms per event |
| **Order execution** | < 100ms end-to-end |
| **Position polling** | ~200ms per wallet |
| **Memory** | ~50MB baseline |
| **CPU** | < 5% on modern hardware |
| **Concurrency** | Semaphore-based rate limiting (default: 25 req / 10s) |

---

## Safety

| | |
|---|---|
| **Circuit Breaker** | Auto-halts after N consecutive large trades inside a configurable window |
| **Depth Guard** | Validates orderbook liquidity before every order |
| **Dry Run** | Full execution path runs without placing real orders |
| **Trade Floor** | Minimum size enforcement against negative-EV micro-trades |

The circuit breaker fires when consecutive large trades exceed the configured threshold, or when orderbook depth falls below the minimum. Once tripped, execution is blocked for the cooldown duration. Trip state and cooldown are logged and visible in the TUI.

**Recommendations:**

| Stage | Action |
|-------|--------|
| Initial setup | Run with `enable_trading: false` for a full session |
| First real trades | Keep `copy_percentage` at 5–10% until you trust the signal |
| Ongoing | Watch circuit breaker trips — they surface execution anomalies |
| Production | Dedicated wallet with only the capital you intend to deploy |

---

## Contact

Built and maintained actively. Whether you want to **run the bots**, **join the managed early-access waitlist**, request a **new venue adapter**, or just talk Polymarket tooling and algorithmic strategies — reach out.

<div align="center">

[![Chat on Telegram](https://img.shields.io/badge/💬_Telegram-@HarrierOnChain-229ED9?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

| Platform | Link |
|----------|------|
| **Telegram** | [t.me/HarrierOnChain](https://t.me/HarrierOnChain) |
| **Discussions** | [GitHub Discussions](../../discussions) |

*Response time is typically within a few hours. Open to questions, feedback, venue requests, and serious collaborations.*

</div>

---

## Disclaimer

> Trading prediction markets involves real financial risk. This software is provided as-is, without warranty or guarantee of any outcome. It is not financial advice. Always test with `enable_trading: false` before deploying real capital. The **managed / copy-trading service is in early-access beta and operates in paper mode (simulated funds)** — it does not custody real money, and any live-trading rollout will follow proper custody, audit, and licensing. Ensure compliance with each venue's terms of service and applicable regulations in your jurisdiction.

---

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)
[![Telegram](https://img.shields.io/badge/💬_Telegram-@HarrierOnChain-229ED9?style=flat-square&logo=telegram&logoColor=white)](https://t.me/HarrierOnChain)

**Built for the Prediction Markets including Polymarket, Kalshi, Limitless etc**

[Back to top](#prediction-market-toolkits)

</div>

[Power of Bot](http://x.com/theparuchh/status/2053766299281416621)
