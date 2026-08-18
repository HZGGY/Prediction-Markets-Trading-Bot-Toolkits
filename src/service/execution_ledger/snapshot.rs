use std::{fs::File, io, path::Path};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::{
    model::{EventHash, LedgerError, LedgerErrorCode},
    projection::{ActiveIntent, LedgerProjection},
    storage::{reject_existing_target, LedgerPaths},
};

pub const ACTIVE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub head_hash: EventHash,
    pub active_intent: Option<ActiveIntent>,
}

impl ActiveSnapshot {
    pub(crate) fn new(
        sequence: u64,
        head_hash: EventHash,
        active_intent: Option<ActiveIntent>,
    ) -> Self {
        Self {
            schema_version: ACTIVE_SNAPSHOT_SCHEMA_VERSION,
            sequence,
            head_hash,
            active_intent,
        }
    }

    fn from_projection(projection: &LedgerProjection) -> Self {
        Self::new(
            projection.sequence,
            projection.head_hash.clone(),
            projection.active.clone(),
        )
    }
}

pub(crate) trait SnapshotDurability: Send + Sync {
    fn create_snapshot_temp(&self, parent: &Path) -> io::Result<NamedTempFile>;
    fn write_snapshot(&self, file: &mut File, bytes: &[u8]) -> io::Result<()>;
    fn flush_snapshot(&self, file: &mut File) -> io::Result<()>;
    fn sync_snapshot(&self, file: &File) -> io::Result<()>;
    fn persist_snapshot(&self, temp: NamedTempFile, target: &Path) -> io::Result<()>;
    fn sync_snapshot_directory(&self, path: &Path) -> io::Result<()>;
}

pub(crate) fn verify_snapshot(
    bytes: Option<&[u8]>,
    projection: &LedgerProjection,
) -> Result<(), LedgerError> {
    let Some(bytes) = bytes else {
        return if projection.active.is_none() {
            Ok(())
        } else {
            Err(LedgerError::new(LedgerErrorCode::SnapshotMissing))
        };
    };
    let actual: ActiveSnapshot = serde_json::from_slice(bytes)
        .map_err(|_| LedgerError::new(LedgerErrorCode::InvalidSnapshot))?;
    if actual.schema_version != ACTIVE_SNAPSHOT_SCHEMA_VERSION {
        return Err(LedgerError::new(LedgerErrorCode::UnsupportedSnapshotSchema));
    }
    if actual != ActiveSnapshot::from_projection(projection) {
        return Err(LedgerError::new(LedgerErrorCode::SnapshotMismatch));
    }
    Ok(())
}

pub(crate) fn persist_snapshot<D: SnapshotDurability + ?Sized>(
    paths: &LedgerPaths,
    snapshot: &ActiveSnapshot,
    durability: &D,
) -> Result<(), LedgerError> {
    let bytes = serde_json::to_vec(snapshot)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotWriteFailed))?;
    let mut temp = durability
        .create_snapshot_temp(&paths.parent)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotWriteFailed))?;
    durability
        .write_snapshot(temp.as_file_mut(), &bytes)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotWriteFailed))?;
    durability
        .flush_snapshot(temp.as_file_mut())
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotFlushFailed))?;
    durability
        .sync_snapshot(temp.as_file())
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotSyncFailed))?;
    reject_existing_target(&paths.snapshot)?;
    durability
        .persist_snapshot(temp, &paths.snapshot)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotPersistFailed))?;
    reject_existing_target(&paths.snapshot)?;
    durability
        .sync_snapshot_directory(&paths.parent)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SnapshotDirectorySyncFailed))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::{self, Write},
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use chrono::Utc;
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    use super::{ActiveSnapshot, ACTIVE_SNAPSHOT_SCHEMA_VERSION};
    use crate::service::execution_ledger::{
        snapshot::SnapshotDurability, storage::DurabilityOps, AcknowledgeReason, ActiveIntentState,
        AppendOutcome, CancelResponseClass, DurablePosition, EventHash, ExecutionLedger, IntentId,
        IntentPurpose, LedgerErrorCode, LedgerPayload, MatchedAmounts, OrderId, OrderSide,
        OrderType, PositionId, PositionSeed, PreparedIntent, ReconcileUncertainCode,
        TerminalNoFillStatus, TokenId, UncertainCode, Venue,
    };

    #[test]
    fn first_active_append_creates_snapshot_and_later_state_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let snapshot_path = snapshot_path(&ledger_path);
        let ledger = ExecutionLedger::open_live(&ledger_path).unwrap();
        let intent = intent_id(1);

        let AppendOutcome::Appended(prepared) = ledger.append(intent, prepared_payload()).unwrap();
        let first = read_snapshot(&snapshot_path);
        assert_eq!(first.schema_version, ACTIVE_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(first.sequence, 1);
        assert_eq!(first.head_hash, prepared.event_hash);
        assert_eq!(
            first.active_intent.as_ref().unwrap().state,
            ActiveIntentState::NotSent
        );

        let AppendOutcome::Appended(started) =
            ledger.append(intent, LedgerPayload::SubmitStarted).unwrap();
        let replacement = read_snapshot(&snapshot_path);
        assert_eq!(replacement.sequence, 2);
        assert_eq!(replacement.head_hash, started.event_hash);
        assert_eq!(
            replacement.active_intent.as_ref().unwrap().state,
            ActiveIntentState::SubmitStarted
        );
        assert_ne!(first, replacement);
    }

    #[test]
    fn reconciliation_cancel_recovery_and_clear_each_publish_an_exact_mirror() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let mirror = snapshot_path(&path);
        let ledger = ExecutionLedger::open_live(&path).unwrap();
        let uncertain_intent = intent_id(30);

        for payload in [
            prepared_payload(),
            LedgerPayload::SubmitStarted,
            LedgerPayload::RemoteUncertain {
                code: UncertainCode::Transport,
            },
            LedgerPayload::ReconciliationStarted,
            LedgerPayload::ReconciledLive,
            LedgerPayload::CancelStarted,
            LedgerPayload::CancelResponseObserved {
                result: CancelResponseClass::NotCanceled,
            },
            LedgerPayload::ReconciliationStarted,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::NotFound,
            },
            LedgerPayload::ReconciliationStarted,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Canceled,
            },
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::ReconciledNoFill,
            },
        ] {
            append_and_assert_exact_mirror(&ledger, &mirror, uncertain_intent, payload);
        }
        assert!(ledger.projection().active.is_none());

        let recovery_intent = intent_id(31);
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            prepared_payload_with_order(0x22),
        );
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::SubmitStarted,
        );
        let matched = MatchedAmounts {
            shares_micros: 10_000_000,
            usd_micros: 5_000_000,
        };
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::RemoteMatched(matched),
        );
        let position_event = append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::PositionOpened(entry_position(recovery_intent, 0x22)),
        );
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::ReconciliationStarted,
        );
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::ReconciledMatched(matched),
        );
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::RecoveryApplied {
                position_event_id: position_event.event_id,
            },
        );
        append_and_assert_exact_mirror(
            &ledger,
            &mirror,
            recovery_intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::RecoveryApplied,
            },
        );
        assert!(ledger.projection().active.is_none());
        let final_sequence = ledger.projection().sequence;
        drop(ledger);
        let reopened = ExecutionLedger::open_live(&path).unwrap();
        assert_eq!(reopened.projection().sequence, final_sequence);
        assert!(reopened.projection().active.is_none());
    }

    #[test]
    fn absent_snapshot_is_accepted_only_when_replay_has_no_active_intent() {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let snapshot_path = snapshot_path(&ledger_path);

        drop(ExecutionLedger::open_live(&ledger_path).unwrap());
        assert!(!snapshot_path.exists());
        drop(ExecutionLedger::open_live(&ledger_path).unwrap());

        let ledger = ExecutionLedger::open_live(&ledger_path).unwrap();
        ledger.append(intent_id(2), prepared_payload()).unwrap();
        drop(ledger);
        fs::remove_file(snapshot_path).unwrap();

        assert_eq!(
            ExecutionLedger::open_live(&ledger_path).unwrap_err().code(),
            LedgerErrorCode::SnapshotMissing
        );
    }

    #[test]
    fn malformed_unknown_schema_unknown_field_and_duplicate_field_fail_closed() {
        assert!(serde_json::from_str::<ActiveSnapshot>("{").is_err());

        let valid = serde_json::to_value(ActiveSnapshot {
            schema_version: ACTIVE_SNAPSHOT_SCHEMA_VERSION,
            sequence: 0,
            head_hash: EventHash::default(),
            active_intent: None,
        })
        .unwrap();
        let mut unknown = valid.clone();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<ActiveSnapshot>(unknown).is_err());

        let unsupported = serde_json::json!({
            "schema_version": 2,
            "sequence": 0,
            "head_hash": EventHash::default(),
            "active_intent": null
        });
        let parsed: ActiveSnapshot = serde_json::from_value(unsupported).unwrap();
        assert_eq!(parsed.schema_version, 2);

        let duplicate = format!(
            "{{\"schema_version\":1,\"schema_version\":1,\"sequence\":0,\"head_hash\":\"{}\",\"active_intent\":null}}",
            EventHash::default()
        );
        assert!(serde_json::from_str::<ActiveSnapshot>(&duplicate).is_err());
    }

    #[test]
    fn open_rejects_malformed_unknown_and_unsupported_snapshot_documents() {
        assert_snapshot_document_error(b"{", LedgerErrorCode::InvalidSnapshot);
        assert_snapshot_document_error(
            br#"{"schema_version":1,"sequence":1,"head_hash":"0000000000000000000000000000000000000000000000000000000000000000","active_intent":null,"unexpected":true}"#,
            LedgerErrorCode::InvalidSnapshot,
        );
        assert_snapshot_document_error(
            br#"{"schema_version":2,"sequence":1,"head_hash":"0000000000000000000000000000000000000000000000000000000000000000","active_intent":null}"#,
            LedgerErrorCode::UnsupportedSnapshotSchema,
        );
        assert_snapshot_document_error(
            br#"{"schema_version":1,"schema_version":1,"sequence":1,"head_hash":"0000000000000000000000000000000000000000000000000000000000000000","active_intent":null}"#,
            LedgerErrorCode::InvalidSnapshot,
        );
    }

    #[test]
    fn open_requires_exact_sequence_hash_and_full_active_intent() {
        assert_snapshot_mutation_mismatches(|value| value["sequence"] = serde_json::json!(99));
        assert_snapshot_mutation_mismatches(|value| {
            value["head_hash"] = serde_json::json!(EventHash::from_bytes([0x55; 32]))
        });
        assert_snapshot_mutation_mismatches(|value| {
            value["active_intent"]["state"] = serde_json::json!("submit_started")
        });
    }

    #[test]
    fn every_snapshot_durability_failure_poisons_before_projection_publish() {
        let cases = [
            (
                SnapshotFailure::Create,
                LedgerErrorCode::SnapshotWriteFailed,
            ),
            (SnapshotFailure::Write, LedgerErrorCode::SnapshotWriteFailed),
            (SnapshotFailure::Flush, LedgerErrorCode::SnapshotFlushFailed),
            (SnapshotFailure::Sync, LedgerErrorCode::SnapshotSyncFailed),
            (
                SnapshotFailure::PersistOn(1),
                LedgerErrorCode::SnapshotPersistFailed,
            ),
            (
                SnapshotFailure::DirectorySync,
                LedgerErrorCode::SnapshotDirectorySyncFailed,
            ),
        ];

        for (failure, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("execution-ledger.jsonl");
            let ops = Arc::new(FailingSnapshotOps::new(failure));
            let ledger = ExecutionLedger::open_live_with_ops(&path, ops).unwrap();

            assert_eq!(
                ledger
                    .append(intent_id(10), prepared_payload())
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(ledger.projection().sequence, 0);
            assert!(ledger.projection().active.is_none());
            assert_eq!(
                ledger
                    .append(intent_id(10), prepared_payload())
                    .unwrap_err()
                    .code(),
                LedgerErrorCode::Fatal
            );
        }
    }

    #[test]
    fn failed_replacement_leaves_a_crash_visible_exact_head_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingSnapshotOps::new(SnapshotFailure::PersistOn(2)));
        let ledger = ExecutionLedger::open_live_with_ops(&path, ops).unwrap();
        let intent = intent_id(11);
        ledger.append(intent, prepared_payload()).unwrap();

        assert_eq!(
            ledger
                .append(intent, LedgerPayload::SubmitStarted)
                .unwrap_err()
                .code(),
            LedgerErrorCode::SnapshotPersistFailed
        );
        assert_eq!(ledger.projection().sequence, 1);
        drop(ledger);

        assert_eq!(
            ExecutionLedger::open_live(&path).unwrap_err().code(),
            LedgerErrorCode::SnapshotMismatch
        );
    }

    #[test]
    fn directory_sync_failure_poisoning_can_reopen_only_when_mirror_matches_durable_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingSnapshotOps::new(SnapshotFailure::DirectorySync));
        let ledger = ExecutionLedger::open_live_with_ops(&path, ops).unwrap();

        assert_eq!(
            ledger
                .append(intent_id(12), prepared_payload())
                .unwrap_err()
                .code(),
            LedgerErrorCode::SnapshotDirectorySyncFailed
        );
        drop(ledger);

        let reopened = ExecutionLedger::open_live(&path).unwrap();
        assert_eq!(reopened.projection().sequence, 1);
        assert_eq!(
            reopened.projection().active.unwrap().state,
            ActiveIntentState::NotSent
        );
    }

    fn assert_snapshot_document_error(bytes: &[u8], expected: LedgerErrorCode) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ledger = ExecutionLedger::open_live(&path).unwrap();
        ledger.append(intent_id(20), prepared_payload()).unwrap();
        drop(ledger);
        fs::write(snapshot_path(&path), bytes).unwrap();
        assert_eq!(
            ExecutionLedger::open_live(path).unwrap_err().code(),
            expected
        );
    }

    fn assert_snapshot_mutation_mismatches(mutate: impl FnOnce(&mut serde_json::Value)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let mirror = snapshot_path(&path);
        let ledger = ExecutionLedger::open_live(&path).unwrap();
        ledger.append(intent_id(21), prepared_payload()).unwrap();
        drop(ledger);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&mirror).unwrap()).unwrap();
        mutate(&mut value);
        fs::write(mirror, serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            ExecutionLedger::open_live(path).unwrap_err().code(),
            LedgerErrorCode::SnapshotMismatch
        );
    }

    fn read_snapshot(path: &Path) -> ActiveSnapshot {
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    fn append_and_assert_exact_mirror(
        ledger: &ExecutionLedger,
        mirror: &Path,
        intent: IntentId,
        payload: LedgerPayload,
    ) -> crate::service::execution_ledger::LedgerEvent {
        let AppendOutcome::Appended(event) = ledger.append(intent, payload).unwrap();
        let projection = ledger.projection();
        let snapshot = read_snapshot(mirror);
        assert_eq!(snapshot.sequence, projection.sequence);
        assert_eq!(snapshot.head_hash, projection.head_hash);
        assert_eq!(snapshot.active_intent, projection.active);
        event
    }

    fn snapshot_path(ledger: &Path) -> PathBuf {
        let mut name = ledger.file_name().unwrap().to_os_string();
        name.push(".active.json");
        ledger.with_file_name(name)
    }

    fn intent_id(value: u128) -> IntentId {
        IntentId(Uuid::from_u128(value))
    }

    fn prepared_payload() -> LedgerPayload {
        prepared_payload_with_order(0x11)
    }

    fn prepared_payload_with_order(order_byte: u8) -> LedgerPayload {
        LedgerPayload::IntentPrepared(PreparedIntent {
            order_id: OrderId::from_hex(format!("0x{}", format!("{order_byte:02x}").repeat(32)))
                .unwrap(),
            protocol_version: 2,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345678901234567890").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 5_000_000,
            expected_taker_micros: 10_000_000,
            source_hash: None,
            purpose: IntentPurpose::Entry(PositionSeed {
                slug: "snapshot-fixture".to_owned(),
                category: "testing".to_owned(),
                tags: vec!["offline".to_owned()],
                take_profit_bps: 1_000,
                stop_loss_bps: 500,
            }),
        })
    }

    fn entry_position(intent: IntentId, order_byte: u8) -> DurablePosition {
        DurablePosition {
            position_id: PositionId(intent.0),
            opening_intent_id: intent,
            opening_order_id: OrderId::from_hex(format!(
                "0x{}",
                format!("{order_byte:02x}").repeat(32)
            ))
            .unwrap(),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345678901234567890").unwrap(),
            slug: "snapshot-fixture".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned()],
            neg_risk: false,
            side: OrderSide::Buy,
            entry_shares_micros: 10_000_000,
            entry_usd_micros: 5_000_000,
            take_profit_bps: 1_000,
            stop_loss_bps: 500,
            opened_at: Utc::now(),
            closing_intent_id: None,
            closing_order_id: None,
            closing_shares_micros: None,
            closing_usd_micros: None,
            closed_at: None,
        }
    }

    #[derive(Clone, Copy)]
    enum SnapshotFailure {
        Create,
        Write,
        Flush,
        Sync,
        PersistOn(usize),
        DirectorySync,
    }

    struct FailingSnapshotOps {
        failure: SnapshotFailure,
        persist_calls: AtomicUsize,
    }

    impl FailingSnapshotOps {
        fn new(failure: SnapshotFailure) -> Self {
            Self {
                failure,
                persist_calls: AtomicUsize::new(0),
            }
        }
    }

    impl DurabilityOps for FailingSnapshotOps {
        fn append(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            file.write_all(bytes)
        }

        fn flush(&self, file: &mut File) -> io::Result<()> {
            file.flush()
        }

        fn sync_file(&self, file: &File) -> io::Result<()> {
            file.sync_all()
        }

        fn persist(&self, temp: NamedTempFile, target: &Path) -> io::Result<()> {
            temp.persist(target)
                .map(|_| ())
                .map_err(|error| error.error)
        }

        fn sync_directory(&self, _path: &Path) -> io::Result<()> {
            Ok(())
        }
    }

    impl SnapshotDurability for FailingSnapshotOps {
        fn create_snapshot_temp(&self, parent: &Path) -> io::Result<NamedTempFile> {
            if matches!(self.failure, SnapshotFailure::Create) {
                return Err(io::Error::other("injected snapshot create"));
            }
            tempfile::Builder::new()
                .prefix(".execution-active-")
                .tempfile_in(parent)
        }

        fn write_snapshot(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            if matches!(self.failure, SnapshotFailure::Write) {
                return Err(io::Error::other("injected snapshot write"));
            }
            file.write_all(bytes)
        }

        fn flush_snapshot(&self, file: &mut File) -> io::Result<()> {
            if matches!(self.failure, SnapshotFailure::Flush) {
                return Err(io::Error::other("injected snapshot flush"));
            }
            file.flush()
        }

        fn sync_snapshot(&self, file: &File) -> io::Result<()> {
            if matches!(self.failure, SnapshotFailure::Sync) {
                return Err(io::Error::other("injected snapshot sync"));
            }
            file.sync_all()
        }

        fn persist_snapshot(&self, temp: NamedTempFile, target: &Path) -> io::Result<()> {
            let call = self.persist_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.failure, SnapshotFailure::PersistOn(expected) if call == expected) {
                return Err(io::Error::other("injected snapshot persist"));
            }
            temp.persist(target)
                .map(|_| ())
                .map_err(|error| error.error)
        }

        fn sync_snapshot_directory(&self, _path: &Path) -> io::Result<()> {
            if matches!(self.failure, SnapshotFailure::DirectorySync) {
                return Err(io::Error::other("injected snapshot directory sync"));
            }
            Ok(())
        }
    }
}
