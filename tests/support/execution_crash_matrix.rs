use std::{
    fs::File,
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};

use super::{CrashPoint, ExecutionCircuitBreaker};
use crate::{
    models::{OrderType, PlannedOrder, Side, VenueId},
    service::{
        execution_ledger::{
            ExecutionLedger, IntentId, IntentPurpose, LedgerCrashPoint, LedgerEvent, LedgerPayload,
            OrderId, OrderSide, OrderType as LedgerOrderType, PositionClose, PositionSeed,
            PreparedIntent, SnapshotDurability, TokenId, Venue, ORDER_PROTOCOL_VERSION,
        },
        order_gateway::{
            OrderGateway, OrderReceipt, OrderSubmitError, PrePostJournal, PreparedOrderIdentity,
        },
        position_store::{OpenPosition, PositionStore},
    },
};

#[tokio::test]
async fn accepted_entry_commits_exact_sequence_and_reopens_without_gateway_call() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    let gateway = AcceptedGateway::new();
    let planned = planned_entry();
    let purpose = entry_purpose();

    let receipt = breaker
        .submit_fok(&gateway, &planned, purpose, |receipt| {
            let (intent_id, position_id) = positions
                .pending_entry_identity(&receipt.order_id)
                .ok_or(())?;
            positions
                .apply_open(OpenPosition {
                    position_id,
                    opening_intent_id: intent_id,
                    opening_order_id: receipt.order_id.clone(),
                    venue: Venue::PolymarketClob,
                    token_id: TokenId::from_decimal("12345").unwrap(),
                    slug: "task-8-entry".to_owned(),
                    category: "testing".to_owned(),
                    tags: vec!["offline".to_owned()],
                    neg_risk: false,
                    side: OrderSide::Buy,
                    shares_micros: receipt.filled_shares_micros,
                    usd_notional_micros: receipt.filled_usd_micros,
                    take_profit_bps: 1_000,
                    stop_loss_bps: 500,
                    opened_at: Utc
                        .with_ymd_and_hms(2026, 8, 18, 12, 0, 0)
                        .single()
                        .unwrap(),
                })
                .map(|_| ())
                .map_err(|_| ())
        })
        .await
        .unwrap();

    assert_eq!(receipt, gateway.receipt);
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    assert!(!breaker.is_halted());
    assert!(!marker_path.exists());
    drop(breaker);
    drop(positions);
    drop(ledger);

    let events = std::fs::read_to_string(&ledger_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        [
            "intent_prepared",
            "submit_started",
            "remote_matched",
            "position_opened",
            "submission_committed",
        ]
    );
    let intent_id = events[0].intent_id;
    assert!(events.iter().all(|event| event.intent_id == intent_id));
    let crate::service::execution_ledger::LedgerPayload::IntentPrepared(PreparedIntent {
        order_id,
        ..
    }) = &events[0].payload
    else {
        unreachable!()
    };
    assert_eq!(order_id, &gateway.receipt.order_id);

    let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let reopened_positions = PositionStore::from_ledger(reopened_ledger).unwrap();
    let reopened = reopened_positions.snapshot();

    assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened[0].opening_intent_id, intent_id);
    assert_eq!(reopened[0].opening_order_id, gateway.receipt.order_id);
    assert_eq!(reopened[0].shares_micros, 39_000_000);
    assert_eq!(reopened[0].usd_notional_micros, 19_500_000);
}

#[tokio::test]
async fn rejected_submission_commits_no_fill_and_reopens_without_gateway_call() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    let gateway = RejectedGateway::new();

    let error = breaker
        .submit_fok(&gateway, &planned_entry(), entry_purpose(), |_| Ok(()))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        OrderSubmitError::Rejected {
            code: crate::service::order_gateway::OrderErrorCode::ServerRejected,
            ..
        }
    ));
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    assert!(!breaker.is_halted());
    assert!(!marker_path.exists());
    assert!(positions.is_empty());
    drop(breaker);
    drop(positions);
    drop(ledger);

    let events = std::fs::read_to_string(&ledger_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        [
            "intent_prepared",
            "submit_started",
            "remote_rejected",
            "submission_committed_no_fill",
        ]
    );

    let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let reopened_positions = PositionStore::from_ledger(reopened_ledger).unwrap();
    assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert!(reopened_positions.is_empty());
}

#[tokio::test]
async fn uncertain_submission_records_active_halt_and_reopens_without_gateway_call() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    let gateway = UncertainGateway::new();

    let error = breaker
        .submit_fok(&gateway, &planned_entry(), entry_purpose(), |_| Ok(()))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        OrderSubmitError::Uncertain {
            code: crate::service::order_gateway::OrderErrorCode::PostTransport,
        }
    ));
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    assert!(breaker.is_halted());
    assert!(marker_path.exists());
    assert!(positions.is_empty());
    drop(breaker);
    drop(positions);
    drop(ledger);

    let events = std::fs::read_to_string(&ledger_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        ["intent_prepared", "submit_started", "remote_uncertain"]
    );

    let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let reopened_positions = PositionStore::from_ledger(Arc::clone(&reopened_ledger)).unwrap();
    assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert!(reopened_positions.is_empty());
    assert!(matches!(
        ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
        Err(OrderSubmitError::Halted {
            code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
        })
    ));
}

#[tokio::test]
async fn accepted_named_exit_commits_close_and_reopens_without_gateway_call() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let position = committed_entry(&ledger, &positions);
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    let gateway = AcceptedExitGateway::new();

    let receipt = breaker
        .submit_fok(
            &gateway,
            &planned_exit(),
            IntentPurpose::Exit {
                position_id: position.position_id,
            },
            |receipt| {
                let closing_intent_id = positions
                    .pending_exit_intent(&receipt.order_id, position.position_id)
                    .ok_or(())?;
                positions
                    .apply_close(PositionClose {
                        position_id: position.position_id,
                        closing_intent_id,
                        closing_order_id: receipt.order_id.clone(),
                        shares_micros: receipt.filled_shares_micros,
                        usd_micros: receipt.filled_usd_micros,
                        closed_at: Utc
                            .with_ymd_and_hms(2026, 8, 18, 12, 5, 0)
                            .single()
                            .unwrap(),
                    })
                    .map(|_| ())
                    .map_err(|_| ())
            },
        )
        .await
        .unwrap();

    assert_eq!(receipt, gateway.receipt);
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    assert!(!breaker.is_halted());
    assert!(!marker_path.exists());
    assert!(positions.is_empty());
    drop(breaker);
    drop(positions);
    drop(ledger);

    let events = std::fs::read_to_string(&ledger_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events[5..]
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        [
            "intent_prepared",
            "submit_started",
            "remote_matched",
            "position_closed",
            "submission_committed",
        ]
    );
    let closing_intent_id = events[5].intent_id;
    assert!(events[5..]
        .iter()
        .all(|event| event.intent_id == closing_intent_id));

    let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let reopened_positions = PositionStore::from_ledger(reopened_ledger).unwrap();
    assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert!(reopened_positions.is_empty());
}

#[tokio::test]
async fn crash_after_prepared_append_reopens_as_active_not_sent_without_post() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    breaker.install_crash_hook(Arc::new(|point| {
        if point == CrashPoint::AfterPreparedAppend {
            panic!("injected crash after prepared append");
        }
    }));
    let gateway = Arc::new(AcceptedGateway::new());

    let task_breaker = Arc::clone(&breaker);
    let task_gateway = Arc::clone(&gateway);
    let task = tokio::spawn(async move {
        task_breaker
            .submit_fok(
                task_gateway.as_ref(),
                &planned_entry(),
                entry_purpose(),
                |_| Ok(()),
            )
            .await
    });
    assert!(task.await.unwrap_err().is_panic());
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 0);
    assert!(positions.is_empty());
    drop(breaker);
    drop(positions);
    drop(ledger);

    let events = std::fs::read_to_string(&ledger_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        ["intent_prepared"]
    );

    let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    let reopened_positions = PositionStore::from_ledger(Arc::clone(&reopened_ledger)).unwrap();
    assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert!(reopened_positions.is_empty());
    assert!(matches!(
        ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
        Err(OrderSubmitError::Halted {
            code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
        })
    ));
}

#[tokio::test]
async fn journal_append_crashes_preserve_only_the_last_durable_state_without_post() {
    for (point, expected, active) in [
        (CrashPoint::BeforePreparedAppend, vec![], false),
        (
            CrashPoint::BeforeSubmitStartedAppend,
            vec!["intent_prepared"],
            true,
        ),
        (
            CrashPoint::AfterSubmitStartedAppend,
            vec!["intent_prepared", "submit_started"],
            true,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let marker_path = dir.path().join("execution-halt.json");
        let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let breaker =
            ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
        breaker.install_crash_hook(Arc::new(move |observed| {
            if observed == point {
                panic!("injected journal append crash");
            }
        }));
        let gateway = Arc::new(AcceptedGateway::new());
        let task_breaker = Arc::clone(&breaker);
        let task_gateway = Arc::clone(&gateway);
        let task = tokio::spawn(async move {
            task_breaker
                .submit_fok(
                    task_gateway.as_ref(),
                    &planned_entry(),
                    entry_purpose(),
                    |_| Ok(()),
                )
                .await
        });

        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(gateway.calls.load(Ordering::SeqCst), 0);
        assert!(positions.is_empty());
        drop(breaker);
        drop(positions);
        drop(ledger);

        let events = std::fs::read_to_string(&ledger_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| event.payload.kind())
                .collect::<Vec<_>>(),
            expected
        );

        let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
        let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
        assert_eq!(reopened_ledger.projection().active.is_some(), active);
        if active {
            assert!(matches!(
                ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
                Err(OrderSubmitError::Halted {
                    code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
                })
            ));
        } else {
            assert!(ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path).is_ok());
        }
    }
}

#[tokio::test]
async fn accepted_completion_crashes_preserve_the_last_durable_event_without_repost() {
    for (point, expected, positions_after_reopen, active) in [
        (
            CrashPoint::BeforeRemoteEvidenceAppend,
            vec!["intent_prepared", "submit_started"],
            0,
            true,
        ),
        (
            CrashPoint::AfterRemoteEvidenceAppend,
            vec!["intent_prepared", "submit_started", "remote_matched"],
            0,
            true,
        ),
        (
            CrashPoint::BeforePositionEvent,
            vec!["intent_prepared", "submit_started", "remote_matched"],
            0,
            true,
        ),
        (
            CrashPoint::AfterPositionEvent,
            vec![
                "intent_prepared",
                "submit_started",
                "remote_matched",
                "position_opened",
            ],
            1,
            true,
        ),
        (
            CrashPoint::BeforeTerminalAppend,
            vec![
                "intent_prepared",
                "submit_started",
                "remote_matched",
                "position_opened",
            ],
            1,
            true,
        ),
        (
            CrashPoint::AfterTerminalAppend,
            vec![
                "intent_prepared",
                "submit_started",
                "remote_matched",
                "position_opened",
                "submission_committed",
            ],
            1,
            false,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let marker_path = dir.path().join("execution-halt.json");
        let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let breaker =
            ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
        breaker.install_crash_hook(Arc::new(move |observed| {
            if observed == point {
                panic!("injected accepted completion crash");
            }
        }));
        let gateway = Arc::new(AcceptedGateway::new());
        let task_breaker = Arc::clone(&breaker);
        let task_gateway = Arc::clone(&gateway);
        let task_positions = Arc::clone(&positions);
        let task = tokio::spawn(async move {
            task_breaker
                .submit_fok(
                    task_gateway.as_ref(),
                    &planned_entry(),
                    entry_purpose(),
                    |receipt| apply_pending_entry(task_positions.as_ref(), receipt),
                )
                .await
        });

        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
        drop(breaker);
        drop(positions);
        drop(ledger);

        let events = std::fs::read_to_string(&ledger_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| event.payload.kind())
                .collect::<Vec<_>>(),
            expected
        );

        let calls_before_reopen = gateway.calls.load(Ordering::SeqCst);
        let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        let reopened_positions = PositionStore::from_ledger(Arc::clone(&reopened_ledger)).unwrap();
        assert_eq!(gateway.calls.load(Ordering::SeqCst), calls_before_reopen);
        assert_eq!(reopened_positions.len(), positions_after_reopen);
        assert_eq!(reopened_ledger.projection().active.is_some(), active);
        if active {
            assert!(matches!(
                ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
                Err(OrderSubmitError::Halted {
                    code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
                })
            ));
        } else {
            assert!(ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path).is_ok());
        }
    }
}

#[tokio::test]
async fn post_invocation_crashes_reopen_from_submit_started_without_repost() {
    for (point, expected_calls) in [
        (CrashPoint::BeforePostInvocation, 0),
        (CrashPoint::AfterPostInvocation, 1),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let marker_path = dir.path().join("execution-halt.json");
        let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let breaker =
            ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
        breaker.install_crash_hook(Arc::new(move |observed| {
            if observed == point {
                panic!("injected post invocation crash");
            }
        }));
        let gateway = Arc::new(CrashPostGateway::new(Arc::clone(&breaker), point));
        let task_breaker = Arc::clone(&breaker);
        let task_gateway = Arc::clone(&gateway);
        let task = tokio::spawn(async move {
            task_breaker
                .submit_fok(
                    task_gateway.as_ref(),
                    &planned_entry(),
                    entry_purpose(),
                    |_| Ok(()),
                )
                .await
        });

        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(gateway.calls.load(Ordering::SeqCst), expected_calls);
        assert!(positions.is_empty());
        drop(gateway);
        drop(breaker);
        drop(positions);
        drop(ledger);

        let events = std::fs::read_to_string(&ledger_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<LedgerEvent>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| event.payload.kind())
                .collect::<Vec<_>>(),
            ["intent_prepared", "submit_started"]
        );

        let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        assert!(matches!(
            ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
            Err(OrderSubmitError::Halted {
                code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
            })
        ));
    }
}

#[test]
fn snapshot_replace_crashes_reopen_safely_without_gateway_or_repost() {
    for point in [
        LedgerCrashPoint::BeforeSnapshotReplace,
        LedgerCrashPoint::AfterSnapshotReplace,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let marker_path = dir.path().join("execution-halt.json");
        let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        ledger.install_crash_hook(Arc::new(move |observed| {
            if observed == point {
                panic!("injected snapshot replace crash");
            }
        }));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ledger
                .append(
                    IntentId(uuid::Uuid::from_u128(0x77)),
                    LedgerPayload::IntentPrepared(PreparedIntent {
                        order_id: order_id(0x77),
                        protocol_version: ORDER_PROTOCOL_VERSION,
                        venue: Venue::PolymarketClob,
                        token_id: TokenId::from_decimal("12345").unwrap(),
                        neg_risk: false,
                        side: OrderSide::Buy,
                        order_type: LedgerOrderType::Fok,
                        expected_maker_micros: 19_500_000,
                        expected_taker_micros: 39_000_000,
                        source_hash: None,
                        purpose: entry_purpose(),
                    }),
                )
                .unwrap();
        }));
        assert!(result.is_err());
        drop(ledger);

        if point == LedgerCrashPoint::BeforeSnapshotReplace {
            assert!(ExecutionLedger::open_live(&ledger_path).is_err());
        } else {
            let reopened = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
            assert!(reopened.projection().active.is_some());
            assert!(matches!(
                ExecutionCircuitBreaker::new_live(reopened, marker_path),
                Err(OrderSubmitError::Halted {
                    code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
                })
            ));
        }
    }
}

#[tokio::test]
async fn remote_evidence_persistence_failure_halts_before_a_second_gateway_call() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("execution-ledger.jsonl");
    let marker_path = dir.path().join("execution-halt.json");
    let durability = Arc::new(FailThirdAppend::default());
    let ledger =
        Arc::new(ExecutionLedger::open_live_with_ops(&ledger_path, durability.clone()).unwrap());
    let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
    let breaker =
        ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker_path.clone()).unwrap();
    let gateway = AcceptedGateway::new();

    let error = breaker
        .submit_fok(&gateway, &planned_entry(), entry_purpose(), |receipt| {
            apply_pending_entry(positions.as_ref(), receipt)
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        OrderSubmitError::Halted {
            code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
        }
    ));
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    assert!(breaker.is_halted());
    assert!(marker_path.exists());
    assert!(positions.is_empty());
    let second = breaker
        .submit_fok(&gateway, &planned_entry(), entry_purpose(), |_| Ok(()))
        .await
        .unwrap_err();
    assert!(matches!(second, OrderSubmitError::Halted { .. }));
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    drop(breaker);
    drop(positions);
    drop(ledger);

    let reopened_ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
    assert!(reopened_ledger.projection().active.is_some());
    assert!(matches!(
        ExecutionCircuitBreaker::new_live(reopened_ledger, marker_path),
        Err(OrderSubmitError::Halted {
            code: crate::service::order_gateway::OrderErrorCode::ExecutionHalted,
        })
    ));
}

struct AcceptedGateway {
    receipt: OrderReceipt,
    calls: AtomicUsize,
}

impl AcceptedGateway {
    fn new() -> Self {
        Self {
            receipt: OrderReceipt {
                order_id: order_id(0x44),
                filled_shares_micros: 39_000_000,
                filled_usd_micros: 19_500_000,
            },
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OrderGateway for AcceptedGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        journal.before_post(&prepared_identity())?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.receipt.clone())
    }
}

struct RejectedGateway {
    calls: AtomicUsize,
}

impl RejectedGateway {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OrderGateway for RejectedGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        journal.before_post(&prepared_identity())?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(OrderSubmitError::Rejected {
            http_status: Some(400),
            code: crate::service::order_gateway::OrderErrorCode::ServerRejected,
        })
    }
}

struct UncertainGateway {
    calls: AtomicUsize,
}

impl UncertainGateway {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OrderGateway for UncertainGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        journal.before_post(&prepared_identity())?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(OrderSubmitError::Uncertain {
            code: crate::service::order_gateway::OrderErrorCode::PostTransport,
        })
    }
}

struct AcceptedExitGateway {
    receipt: OrderReceipt,
    calls: AtomicUsize,
}

impl AcceptedExitGateway {
    fn new() -> Self {
        Self {
            receipt: OrderReceipt {
                order_id: order_id(0x66),
                filled_shares_micros: 39_000_000,
                filled_usd_micros: 23_400_000,
            },
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OrderGateway for AcceptedExitGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        journal.before_post(&prepared_exit_identity())?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.receipt.clone())
    }
}

struct CrashPostGateway {
    breaker: Arc<ExecutionCircuitBreaker>,
    point: CrashPoint,
    calls: AtomicUsize,
}

impl CrashPostGateway {
    fn new(breaker: Arc<ExecutionCircuitBreaker>, point: CrashPoint) -> Self {
        Self {
            breaker,
            point,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OrderGateway for CrashPostGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        journal.before_post(&prepared_identity())?;
        if self.point == CrashPoint::BeforePostInvocation {
            self.breaker.crash(self.point);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.point == CrashPoint::AfterPostInvocation {
            self.breaker.crash(self.point);
        }
        unreachable!("post crash must interrupt before a receipt is returned")
    }
}

#[derive(Default)]
struct FailThirdAppend {
    append_calls: AtomicUsize,
}

impl SnapshotDurability for FailThirdAppend {
    fn create_snapshot_temp(&self, parent: &Path) -> io::Result<tempfile::NamedTempFile> {
        tempfile::Builder::new()
            .prefix(".execution-active-")
            .tempfile_in(parent)
    }

    fn write_snapshot(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn flush_snapshot(&self, file: &mut File) -> io::Result<()> {
        file.flush()
    }

    fn sync_snapshot(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn persist_snapshot(&self, temp: tempfile::NamedTempFile, target: &Path) -> io::Result<()> {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }

    fn sync_snapshot_directory(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

impl crate::service::execution_ledger::DurabilityOps for FailThirdAppend {
    fn append(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        if self.append_calls.fetch_add(1, Ordering::SeqCst) == 2 {
            return Err(io::Error::other("injected remote evidence append failure"));
        }
        file.write_all(bytes)
    }

    fn flush(&self, file: &mut File) -> io::Result<()> {
        file.flush()
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn persist(&self, temp: tempfile::NamedTempFile, target: &Path) -> io::Result<()> {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }

    fn sync_directory(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

fn order_id(byte: u8) -> OrderId {
    OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
}

fn planned_entry() -> PlannedOrder {
    PlannedOrder {
        venue: VenueId::Polymarket,
        token_id: "12345".to_owned(),
        neg_risk: false,
        side: Side::Buy,
        shares: 39.0,
        limit_price: 0.5,
        usd_notional: 19.5,
        order_type: OrderType::Fok,
        source_trade_hash: None,
    }
}

fn planned_exit() -> PlannedOrder {
    PlannedOrder {
        venue: VenueId::Polymarket,
        token_id: "12345".to_owned(),
        neg_risk: false,
        side: Side::Sell,
        shares: 39.0,
        limit_price: 0.6,
        usd_notional: 23.4,
        order_type: OrderType::Fok,
        source_trade_hash: None,
    }
}

fn entry_purpose() -> IntentPurpose {
    IntentPurpose::Entry(PositionSeed {
        slug: "task-8-entry".to_owned(),
        category: "testing".to_owned(),
        tags: vec!["offline".to_owned()],
        take_profit_bps: 1_000,
        stop_loss_bps: 500,
    })
}

fn prepared_identity() -> PreparedOrderIdentity {
    PreparedOrderIdentity {
        order_id: order_id(0x44),
        protocol_version: ORDER_PROTOCOL_VERSION,
        venue: Venue::PolymarketClob,
        token_id: TokenId::from_decimal("12345").unwrap(),
        neg_risk: false,
        side: OrderSide::Buy,
        order_type: LedgerOrderType::Fok,
        expected_maker_micros: 19_500_000,
        expected_taker_micros: 39_000_000,
    }
}

fn prepared_exit_identity() -> PreparedOrderIdentity {
    PreparedOrderIdentity {
        order_id: order_id(0x66),
        protocol_version: ORDER_PROTOCOL_VERSION,
        venue: Venue::PolymarketClob,
        token_id: TokenId::from_decimal("12345").unwrap(),
        neg_risk: false,
        side: OrderSide::Sell,
        order_type: LedgerOrderType::Fok,
        expected_maker_micros: 39_000_000,
        expected_taker_micros: 23_400_000,
    }
}

fn committed_entry(ledger: &ExecutionLedger, positions: &PositionStore) -> OpenPosition {
    let opening_intent_id = IntentId(uuid::Uuid::from_u128(0x55));
    let position = OpenPosition {
        position_id: crate::service::execution_ledger::PositionId(opening_intent_id.0),
        opening_intent_id,
        opening_order_id: order_id(0x55),
        venue: Venue::PolymarketClob,
        token_id: TokenId::from_decimal("12345").unwrap(),
        slug: "task-8-entry".to_owned(),
        category: "testing".to_owned(),
        tags: vec!["offline".to_owned()],
        neg_risk: false,
        side: OrderSide::Buy,
        shares_micros: 39_000_000,
        usd_notional_micros: 19_500_000,
        take_profit_bps: 1_000,
        stop_loss_bps: 500,
        opened_at: Utc
            .with_ymd_and_hms(2026, 8, 18, 12, 0, 0)
            .single()
            .unwrap(),
    };
    ledger
        .append(
            opening_intent_id,
            LedgerPayload::IntentPrepared(PreparedIntent {
                order_id: position.opening_order_id.clone(),
                protocol_version: ORDER_PROTOCOL_VERSION,
                venue: position.venue,
                token_id: position.token_id,
                neg_risk: position.neg_risk,
                side: position.side,
                order_type: LedgerOrderType::Fok,
                expected_maker_micros: position.usd_notional_micros,
                expected_taker_micros: position.shares_micros,
                source_hash: None,
                purpose: entry_purpose(),
            }),
        )
        .unwrap();
    ledger
        .append(opening_intent_id, LedgerPayload::SubmitStarted)
        .unwrap();
    ledger
        .append(
            opening_intent_id,
            LedgerPayload::RemoteMatched(crate::service::execution_ledger::MatchedAmounts {
                shares_micros: position.shares_micros,
                usd_micros: position.usd_notional_micros,
            }),
        )
        .unwrap();
    positions.apply_open(position.clone()).unwrap();
    ledger
        .append(opening_intent_id, LedgerPayload::SubmissionCommitted)
        .unwrap();
    position
}

fn apply_pending_entry(positions: &PositionStore, receipt: &OrderReceipt) -> Result<(), ()> {
    let (intent_id, position_id) = positions
        .pending_entry_identity(&receipt.order_id)
        .ok_or(())?;
    positions
        .apply_open(OpenPosition {
            position_id,
            opening_intent_id: intent_id,
            opening_order_id: receipt.order_id.clone(),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345").ok_or(())?,
            slug: "task-8-entry".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned()],
            neg_risk: false,
            side: OrderSide::Buy,
            shares_micros: receipt.filled_shares_micros,
            usd_notional_micros: receipt.filled_usd_micros,
            take_profit_bps: 1_000,
            stop_loss_bps: 500,
            opened_at: Utc
                .with_ymd_and_hms(2026, 8, 18, 12, 0, 0)
                .single()
                .ok_or(())?,
        })
        .map(|_| ())
        .map_err(|_| ())
}
