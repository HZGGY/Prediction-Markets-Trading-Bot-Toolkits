//! Polymarket Toolkits — shared library.
//!
//! Multi-venue prediction-market trading engine. The copy-trading surface is a
//! phase-2 offline/non-live implementation; phase-3 reconciliation and account
//! capability work is required before any live-trading evaluation. Other
//! strategies expose typed stubs over the same engine and risk layer.

pub mod bot;
pub mod config;
pub mod models;
pub mod recovery_cli;
pub mod service;
pub mod ui;
pub mod utils;
