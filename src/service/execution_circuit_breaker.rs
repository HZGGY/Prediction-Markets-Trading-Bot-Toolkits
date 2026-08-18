use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    models::PlannedOrder,
    service::execution_ledger::ExecutionLedger,
    service::order_gateway::{
        OrderErrorCode, OrderGateway, OrderReceipt, OrderStage, OrderSubmitError,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHaltMarker {
    pub schema_version: u32,
    pub halted_at: DateTime<Utc>,
    pub reason_code: String,
    pub stage: String,
    pub token_id: String,
    pub side: String,
    pub order_id_hint: Option<String>,
}

pub struct ExecutionCircuitBreaker {
    halted: AtomicBool,
    _ledger: Arc<ExecutionLedger>,
    path: PathBuf,
    submit_lock: tokio::sync::Mutex<()>,
}

impl ExecutionCircuitBreaker {
    pub fn new_live(
        ledger: Arc<ExecutionLedger>,
        path: PathBuf,
    ) -> Result<Arc<Self>, OrderSubmitError> {
        if ledger.projection().active.is_some() {
            return Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted,
            });
        }
        if path.exists() {
            return Err(OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerPresent,
            });
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir()
            || tempfile::Builder::new()
                .prefix(".execution-halt-probe-")
                .tempfile_in(parent)
                .is_err()
        {
            return Err(OrderSubmitError::Preflight {
                stage: OrderStage::Initialization,
                code: OrderErrorCode::HaltMarkerIo,
            });
        }
        Ok(Arc::new(Self {
            halted: AtomicBool::new(false),
            _ledger: ledger,
            path,
            submit_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<(), OrderSubmitError> {
        if self.is_halted() {
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted,
            })
        } else {
            Ok(())
        }
    }

    pub fn halt_uncertain(
        &self,
        planned: &PlannedOrder,
        reason: OrderErrorCode,
    ) -> Result<(), OrderSubmitError> {
        self.halt_uncertain_with(planned, reason, |temp, target| {
            temp.persist(target)
                .map(|_| ())
                .map_err(|error| error.error)
        })
    }

    fn halt_uncertain_with<F>(
        &self,
        planned: &PlannedOrder,
        reason: OrderErrorCode,
        persist: F,
    ) -> Result<(), OrderSubmitError>
    where
        F: FnOnce(tempfile::NamedTempFile, &Path) -> std::io::Result<()>,
    {
        self.halted.store(true, Ordering::Release);
        let marker = ExecutionHaltMarker {
            schema_version: 1,
            halted_at: Utc::now(),
            reason_code: format!("{reason:?}"),
            stage: "post_or_response".to_owned(),
            token_id: planned.token_id.clone(),
            side: format!("{:?}", planned.side).to_uppercase(),
            order_id_hint: None,
        };
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut temp = tempfile::Builder::new()
            .prefix(".execution-halt-")
            .tempfile_in(parent)
            .map_err(|_| OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerIo,
            })?;
        serde_json::to_writer_pretty(temp.as_file_mut(), &marker).map_err(|_| {
            OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerIo,
            }
        })?;
        temp.as_file_mut()
            .flush()
            .map_err(|_| OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerIo,
            })?;
        temp.as_file()
            .sync_all()
            .map_err(|_| OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerIo,
            })?;
        persist(temp, &self.path).map_err(|_| OrderSubmitError::Halted {
            code: OrderErrorCode::HaltMarkerIo,
        })
    }

    pub async fn submit_fok(
        &self,
        gateway: &dyn OrderGateway,
        planned: &PlannedOrder,
        complete_post_fill: impl FnOnce(&OrderReceipt) -> Result<(), ()> + Send,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        let _submission_guard = self.submit_lock.lock().await;
        self.check()?;
        match gateway.submit_fok(planned).await {
            Ok(receipt) => {
                if complete_post_fill(&receipt).is_err() {
                    let error = self
                        .halt_uncertain(planned, OrderErrorCode::ExecutionHalted)
                        .err()
                        .unwrap_or(OrderSubmitError::Halted {
                            code: OrderErrorCode::ExecutionHalted,
                        });
                    return Err(error);
                }
                Ok(receipt)
            }
            Err(error @ OrderSubmitError::Uncertain { code }) => {
                self.halt_uncertain(planned, code)?;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use async_trait::async_trait;

    use super::{ExecutionCircuitBreaker, ExecutionHaltMarker};
    use crate::{
        models::{OrderType, PlannedOrder, Side, VenueId},
        service::execution_ledger::{AcknowledgeReason, ExecutionLedger, IntentId, LedgerPayload},
        service::order_gateway::{OrderErrorCode, OrderGateway, OrderReceipt, OrderSubmitError},
    };

    #[test]
    fn manual_marker_deletion_cannot_bypass_an_active_ledger_intent() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        ledger
            .append(IntentId(uuid::Uuid::from_u128(1)), prepared_payload())
            .unwrap();
        let marker = dir.path().join("execution-halt.json");
        std::fs::write(&marker, b"{}").unwrap();
        assert!(matches!(
            ExecutionCircuitBreaker::new_live(Arc::clone(&ledger), marker.clone()),
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted
            })
        ));
        std::fs::remove_file(&marker).unwrap();
        assert!(!marker.exists());

        assert!(matches!(
            ExecutionCircuitBreaker::new_live(ledger, marker),
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted
            })
        ));
    }

    #[test]
    fn leftover_marker_with_no_active_ledger_remains_a_compatibility_halt() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent = IntentId(uuid::Uuid::from_u128(2));
        ledger.append(intent, prepared_payload()).unwrap();
        ledger
            .append(
                intent,
                LedgerPayload::Acknowledged {
                    reason: AcknowledgeReason::NotSent,
                },
            )
            .unwrap();
        assert!(ledger.projection().active.is_none());
        let marker = dir.path().join("execution-halt.json");
        std::fs::write(&marker, b"{}").unwrap();

        assert!(matches!(
            ExecutionCircuitBreaker::new_live(ledger, marker.clone()),
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerPresent
            })
        ));
        assert!(marker.is_file());
    }

    #[tokio::test]
    async fn uncertainty_persists_marker_and_blocks_every_later_submission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-halt.json");
        let breaker = test_breaker(path.clone()).unwrap();
        let gateway = FakeGateway::returning(Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTimeout,
        }));
        let planned = fixture_planned_order();

        let first = breaker
            .submit_fok(&gateway, &planned, |_receipt| Ok(()))
            .await
            .unwrap_err();
        assert!(first.is_uncertain());
        assert!(path.is_file());
        assert_eq!(gateway.calls(), 1);

        let second = breaker
            .submit_fok(&gateway, &planned, |_receipt| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(second, OrderSubmitError::Halted { .. }));
        assert_eq!(gateway.calls(), 1);

        let marker: ExecutionHaltMarker =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(marker.schema_version, 1);
        assert_eq!(marker.reason_code, "PostTimeout");
        assert_eq!(marker.token_id, planned.token_id);
    }

    #[test]
    fn existing_marker_blocks_live_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-halt.json");
        std::fs::write(&path, b"{}").unwrap();
        assert!(matches!(
            test_breaker(path),
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerPresent
            })
        ));
    }

    #[test]
    fn persist_failure_leaves_memory_halted() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("execution-halt.json");
        let breaker = test_breaker(target).unwrap();
        let result = breaker.halt_uncertain_with(
            &fixture_planned_order(),
            OrderErrorCode::PostTransport,
            |_temp, _target| Err(std::io::Error::other("simulated persist failure")),
        );
        assert!(matches!(result, Err(OrderSubmitError::Halted { .. })));
        assert!(breaker.is_halted());
    }

    #[tokio::test]
    async fn concurrent_submission_waits_and_never_posts_after_first_uncertainty() {
        let dir = tempfile::tempdir().unwrap();
        let breaker = test_breaker(dir.path().join("execution-halt.json")).unwrap();
        let gateway = FakeGateway::returning_after(
            Err(OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            }),
            Duration::from_millis(20),
        );
        let planned = fixture_planned_order();
        let (first, second) = tokio::join!(
            breaker.submit_fok(&gateway, &planned, |_receipt| Ok(())),
            breaker.submit_fok(&gateway, &planned, |_receipt| Ok(())),
        );
        assert!(first.is_err());
        assert!(second.is_err());
        assert_eq!(gateway.calls(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failing_post_fill_blocks_concurrent_gateway_call_until_halted() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("execution-halt.json");
        let breaker = test_breaker(marker.clone()).unwrap();
        let gateway = Arc::new(ConcurrentAcceptedGateway::default());
        let planned = fixture_planned_order();
        let post_fill_entered = Arc::new(tokio::sync::Notify::new());
        let (release_post_fill, wait_for_release) = std::sync::mpsc::sync_channel(0);

        let first_breaker = Arc::clone(&breaker);
        let first_gateway = Arc::clone(&gateway);
        let first_planned = planned.clone();
        let first_entered = Arc::clone(&post_fill_entered);
        let first = tokio::spawn(async move {
            first_breaker
                .submit_fok(first_gateway.as_ref(), &first_planned, move |_receipt| {
                    first_entered.notify_one();
                    wait_for_release.recv().unwrap();
                    Err(())
                })
                .await
        });

        post_fill_entered.notified().await;
        assert_eq!(gateway.calls(), 1);

        let mut second =
            Box::pin(breaker.submit_fok(gateway.as_ref(), &planned, |_receipt| Ok(())));
        tokio::select! {
            biased;
            _ = &mut second => panic!("second submission completed before post-fill release"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(gateway.calls(), 1);
        release_post_fill.send(()).unwrap();

        let first_result = first.await.unwrap();
        let second_result = second.await;
        assert!(matches!(
            first_result,
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted
            })
        ));
        assert!(matches!(
            second_result,
            Err(OrderSubmitError::Halted { .. })
        ));
        assert!(breaker.is_halted());
        assert!(marker.is_file());
        assert_eq!(gateway.calls(), 1);
    }

    struct FakeGateway {
        result: Result<OrderReceipt, OrderSubmitError>,
        calls: AtomicUsize,
        delay: Duration,
    }

    impl FakeGateway {
        fn returning(result: Result<OrderReceipt, OrderSubmitError>) -> Self {
            Self {
                result,
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            }
        }

        fn returning_after(
            result: Result<OrderReceipt, OrderSubmitError>,
            delay: Duration,
        ) -> Self {
            Self {
                result,
                calls: AtomicUsize::new(0),
                delay,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OrderGateway for FakeGateway {
        async fn submit_fok(
            &self,
            _planned: &PlannedOrder,
        ) -> Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.result.clone()
        }
    }

    #[derive(Default)]
    struct ConcurrentAcceptedGateway {
        calls: AtomicUsize,
    }

    impl ConcurrentAcceptedGateway {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OrderGateway for ConcurrentAcceptedGateway {
        async fn submit_fok(
            &self,
            _planned: &PlannedOrder,
        ) -> Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(OrderReceipt {
                order_id: format!("0x{}", "44".repeat(32)),
                filled_shares_micros: 39_000_000,
                filled_usd_micros: 19_500_000,
            })
        }
    }

    fn fixture_planned_order() -> PlannedOrder {
        PlannedOrder {
            venue: VenueId::Polymarket,
            token_id: "12345".to_owned(),
            neg_risk: false,
            side: Side::Buy,
            shares: 39.0,
            limit_price: 0.505,
            usd_notional: 20.0,
            order_type: OrderType::Fok,
            source_trade_hash: None,
        }
    }

    fn prepared_payload() -> LedgerPayload {
        use crate::service::execution_ledger::{
            IntentPurpose, OrderId, OrderSide, OrderType as LedgerOrderType, PositionSeed,
            PreparedIntent, TokenId, Venue,
        };

        LedgerPayload::IntentPrepared(PreparedIntent {
            order_id: OrderId::from_hex(format!("0x{}", "11".repeat(32))).unwrap(),
            protocol_version: 2,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: LedgerOrderType::Fok,
            expected_maker_micros: 5_000_000,
            expected_taker_micros: 10_000_000,
            source_hash: None,
            purpose: IntentPurpose::Entry(PositionSeed {
                slug: "breaker-fixture".to_owned(),
                category: "testing".to_owned(),
                tags: vec!["offline".to_owned()],
                take_profit_bps: 1_000,
                stop_loss_bps: 500,
            }),
        })
    }

    fn test_breaker(path: PathBuf) -> Result<Arc<ExecutionCircuitBreaker>, OrderSubmitError> {
        let ledger = Arc::new(
            ExecutionLedger::open_live(path.parent().unwrap().join("execution-ledger.jsonl"))
                .unwrap(),
        );
        ExecutionCircuitBreaker::new_live(ledger, path)
    }
}
