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
    path: PathBuf,
    submit_lock: tokio::sync::Mutex<()>,
}

impl ExecutionCircuitBreaker {
    pub fn new_live(path: PathBuf) -> Result<Arc<Self>, OrderSubmitError> {
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
    ) -> Result<OrderReceipt, OrderSubmitError> {
        let _submission_guard = self.submit_lock.lock().await;
        self.check()?;
        match gateway.submit_fok(planned).await {
            Err(error @ OrderSubmitError::Uncertain { code }) => {
                self.halt_uncertain(planned, code)?;
                Err(error)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use async_trait::async_trait;

    use super::{ExecutionCircuitBreaker, ExecutionHaltMarker};
    use crate::{
        models::{OrderType, PlannedOrder, Side, VenueId},
        service::order_gateway::{OrderErrorCode, OrderGateway, OrderReceipt, OrderSubmitError},
    };

    #[tokio::test]
    async fn uncertainty_persists_marker_and_blocks_every_later_submission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-halt.json");
        let breaker = ExecutionCircuitBreaker::new_live(path.clone()).unwrap();
        let gateway = FakeGateway::returning(Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTimeout,
        }));
        let planned = fixture_planned_order();

        let first = breaker.submit_fok(&gateway, &planned).await.unwrap_err();
        assert!(first.is_uncertain());
        assert!(path.is_file());
        assert_eq!(gateway.calls(), 1);

        let second = breaker.submit_fok(&gateway, &planned).await.unwrap_err();
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
            ExecutionCircuitBreaker::new_live(path),
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerPresent
            })
        ));
    }

    #[test]
    fn persist_failure_leaves_memory_halted() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("execution-halt.json");
        let breaker = ExecutionCircuitBreaker::new_live(target).unwrap();
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
        let breaker =
            ExecutionCircuitBreaker::new_live(dir.path().join("execution-halt.json")).unwrap();
        let gateway = FakeGateway::returning_after(
            Err(OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            }),
            Duration::from_millis(20),
        );
        let planned = fixture_planned_order();
        let (first, second) = tokio::join!(
            breaker.submit_fok(&gateway, &planned),
            breaker.submit_fok(&gateway, &planned),
        );
        assert!(first.is_err());
        assert!(second.is_err());
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
}
