use std::{
    fmt,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use uuid::Uuid;

use super::{
    model::*,
    projection::{LedgerProjection, LedgerProjectionSnapshot},
    snapshot::{persist_snapshot, verify_snapshot, ActiveSnapshot, SnapshotDurability},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Appended(LedgerEvent),
}

#[derive(Serialize)]
struct HashMaterial<'a> {
    schema_version: u32,
    sequence: u64,
    event_id: EventId,
    intent_id: IntentId,
    recorded_at: &'a DateTime<Utc>,
    payload: &'a LedgerPayload,
    previous_hash: &'a EventHash,
}

impl<'a> HashMaterial<'a> {
    fn from_event(event: &'a LedgerEvent) -> Self {
        Self {
            schema_version: event.schema_version,
            sequence: event.sequence,
            event_id: event.event_id,
            intent_id: event.intent_id,
            recorded_at: &event.recorded_at,
            payload: &event.payload,
            previous_hash: &event.previous_hash,
        }
    }
}

fn calculate_event_hash(material: &HashMaterial<'_>) -> Result<EventHash, LedgerError> {
    let bytes = serde_json::to_vec(material)
        .map_err(|_| LedgerError::new(LedgerErrorCode::SerializationFailed))?;
    Ok(EventHash::from_bytes(Sha256::digest(bytes)))
}

#[derive(Debug)]
pub(crate) struct LedgerPaths {
    pub(crate) parent: PathBuf,
    pub(crate) ledger: PathBuf,
    pub(crate) snapshot: PathBuf,
    pub(crate) lock: PathBuf,
}

impl LedgerPaths {
    fn derive(configured: &Path) -> Result<Self, LedgerError> {
        let file_name = configured
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::UnsafePath))?;
        let configured_parent = configured.parent().unwrap_or_else(|| Path::new("."));
        reject_unsafe_path_chain(configured_parent)?;
        let parent = fs::canonicalize(configured_parent)
            .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
        reject_unsafe_path_chain(&parent)?;
        if !parent.is_dir() {
            return Err(LedgerError::new(LedgerErrorCode::Unavailable));
        }

        let ledger = parent.join(file_name);
        let snapshot = sibling_with_suffix(&ledger, ".active.json")?;
        let lock = sibling_with_suffix(&ledger, ".lock")?;
        for target in [&ledger, &snapshot, &lock] {
            if target.parent() != Some(parent.as_path()) {
                return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
            }
            reject_existing_target(target)?;
        }

        Ok(Self {
            parent,
            ledger,
            snapshot,
            lock,
        })
    }
}

pub struct ExecutionLedger {
    state: Mutex<LedgerState>,
    lock_file: File,
    #[cfg(unix)]
    parent_lock_file: File,
    durability: Arc<dyn DurabilityOps>,
    paths: LedgerPaths,
    #[cfg(test)]
    crash_hook: Mutex<Option<LedgerCrashHook>>,
}

impl fmt::Debug for ExecutionLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionLedger")
            .field("label", &"execution_ledger")
            .finish_non_exhaustive()
    }
}

struct LedgerState {
    file: File,
    projection: LedgerProjection,
    fatal: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LedgerCrashPoint {
    BeforeSnapshotReplace,
    AfterSnapshotReplace,
}

#[cfg(test)]
type LedgerCrashHook = Arc<dyn Fn(LedgerCrashPoint) + Send + Sync>;

impl ExecutionLedger {
    pub fn open_live(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        Self::open_with_ops(path.as_ref(), Arc::new(SystemDurabilityOps))
    }

    #[cfg(test)]
    pub(crate) fn open_live_with_ops(
        path: impl AsRef<Path>,
        durability: Arc<dyn DurabilityOps>,
    ) -> Result<Self, LedgerError> {
        Self::open_with_ops(path.as_ref(), durability)
    }

    fn open_with_ops(path: &Path, durability: Arc<dyn DurabilityOps>) -> Result<Self, LedgerError> {
        let paths = LedgerPaths::derive(path)?;
        #[cfg(unix)]
        let parent_lock_file = open_parent_lock(&paths.parent)?;
        let OpenedFile {
            file: lock_file, ..
        } = open_restrictive(&paths.lock, true)?;
        acquire_lifetime_lock(&lock_file)?;

        run_durability_probe(&paths, durability.as_ref())?;

        let OpenedFile { mut file, created } = open_restrictive(&paths.ledger, true)?;
        if created {
            durability
                .sync_file(&file)
                .map_err(|_| LedgerError::new(LedgerErrorCode::SyncFailed))?;
            durability
                .sync_directory(&paths.parent)
                .map_err(|_| LedgerError::new(LedgerErrorCode::DirectorySyncFailed))?;
        }
        let projection = replay(&mut file)?;
        let snapshot_bytes = read_snapshot(&paths)?;
        verify_snapshot(snapshot_bytes.as_deref(), &projection)?;
        file.seek(SeekFrom::End(0))
            .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;

        Ok(Self {
            state: Mutex::new(LedgerState {
                file,
                projection,
                fatal: false,
            }),
            lock_file,
            #[cfg(unix)]
            parent_lock_file,
            durability,
            paths,
            #[cfg(test)]
            crash_hook: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub(crate) fn install_crash_hook(&self, hook: LedgerCrashHook) {
        *self.crash_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    fn crash(&self, point: LedgerCrashPoint) {
        if let Some(hook) = self.crash_hook.lock().as_ref() {
            hook(point);
        }
    }

    pub fn append(
        &self,
        intent_id: IntentId,
        payload: LedgerPayload,
    ) -> Result<AppendOutcome, LedgerError> {
        let mut state = self.state.lock();
        if state.fatal {
            return Err(LedgerError::new(LedgerErrorCode::Fatal));
        }

        let sequence = state
            .projection
            .sequence
            .checked_add(1)
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::SequenceExhausted))?;
        let mut event = LedgerEvent {
            schema_version: LEDGER_SCHEMA_VERSION,
            sequence,
            event_id: EventId(Uuid::new_v4()),
            intent_id,
            recorded_at: Utc::now(),
            payload,
            previous_hash: state.projection.head_hash.clone(),
            event_hash: EventHash::default(),
        };
        event.event_hash = calculate_event_hash(&HashMaterial::from_event(&event))?;
        let mut bytes = serde_json::to_vec(&event)
            .map_err(|_| LedgerError::new(LedgerErrorCode::SerializationFailed))?;
        bytes.push(b'\n');
        let staged = state.projection.stage_next(&event)?;
        let pending_snapshot = staged.snapshot_changed().then(|| {
            ActiveSnapshot::new(
                event.sequence,
                event.event_hash.clone(),
                staged.active_after(&state.projection.active),
            )
        });

        if self.durability.append(&mut state.file, &bytes).is_err() {
            state.fatal = true;
            return Err(LedgerError::new(LedgerErrorCode::AppendFailed));
        }
        if self.durability.flush(&mut state.file).is_err() {
            state.fatal = true;
            return Err(LedgerError::new(LedgerErrorCode::FlushFailed));
        }
        if self.durability.sync_file(&state.file).is_err() {
            state.fatal = true;
            return Err(LedgerError::new(LedgerErrorCode::SyncFailed));
        }
        let mut snapshot_guard = if let Some(snapshot) = pending_snapshot {
            #[cfg(test)]
            self.crash(LedgerCrashPoint::BeforeSnapshotReplace);
            match persist_snapshot(&self.paths, &snapshot, self.durability.as_ref()) {
                Ok(guard) => {
                    #[cfg(test)]
                    self.crash(LedgerCrashPoint::AfterSnapshotReplace);
                    Some(guard)
                }
                Err(error) => {
                    state.fatal = true;
                    return Err(error);
                }
            }
        } else {
            None
        };
        if let Some(guard) = snapshot_guard.as_mut() {
            if let Err(error) = guard.revalidate() {
                state.fatal = true;
                return Err(error);
            }
        }

        state.projection.publish_staged(&event, staged);
        drop(snapshot_guard);
        Ok(AppendOutcome::Appended(event))
    }

    pub fn projection(&self) -> LedgerProjectionSnapshot {
        self.state.lock().projection.snapshot()
    }

    pub(crate) fn event(&self, event_id: EventId) -> Option<LedgerEvent> {
        self.state
            .lock()
            .projection
            .event_ids
            .get(&event_id)
            .cloned()
    }
}

impl Drop for ExecutionLedger {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
        #[cfg(unix)]
        let _ = self.parent_lock_file.unlock();
    }
}

fn replay(file: &mut File) -> Result<LedgerProjection, LedgerError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
    if bytes.is_empty() {
        return Ok(LedgerProjection::default());
    }
    if bytes.last() != Some(&b'\n') {
        return Err(LedgerError::new(LedgerErrorCode::TruncatedTail));
    }

    let mut projection = LedgerProjection::default();
    for (line_index, line) in bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .enumerate()
    {
        let event: LedgerEvent = serde_json::from_slice(line).map_err(classify_json_error)?;
        let physical_sequence = u64::try_from(line_index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::SequenceExhausted))?;
        if event.sequence != physical_sequence {
            return Err(LedgerError::new(LedgerErrorCode::SequenceMismatch));
        }
        if event.schema_version != LEDGER_SCHEMA_VERSION {
            return Err(LedgerError::new(LedgerErrorCode::UnsupportedSchema));
        }
        let calculated = calculate_event_hash(&HashMaterial::from_event(&event))?;
        if calculated != event.event_hash {
            return Err(LedgerError::new(LedgerErrorCode::EventHashMismatch));
        }
        projection.validate_and_apply(&event)?;
    }
    Ok(projection)
}

fn read_snapshot(paths: &LedgerPaths) -> Result<Option<Vec<u8>>, LedgerError> {
    match fs::symlink_metadata(&paths.snapshot) {
        Ok(metadata) => reject_metadata(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(LedgerError::new(LedgerErrorCode::Unavailable)),
    }
    let mut file = open_existing_restrictive(&paths.snapshot, false)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
    Ok(Some(bytes))
}

fn classify_json_error(error: serde_json::Error) -> LedgerError {
    let code = if error.to_string().contains("unknown ledger event kind") {
        LedgerErrorCode::UnsupportedEventKind
    } else {
        LedgerErrorCode::InvalidJson
    };
    LedgerError::new(code)
}

fn sibling_with_suffix(path: &Path, suffix: &str) -> Result<PathBuf, LedgerError> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| LedgerError::new(LedgerErrorCode::UnsafePath))?
        .to_os_string();
    file_name.push(suffix);
    Ok(path.with_file_name(file_name))
}

fn reject_unsafe_path_chain(path: &Path) -> Result<(), LedgerError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => continue,
            Component::Prefix(_) => {
                current.push(component.as_os_str());
                continue;
            }
            _ => current.push(component.as_os_str()),
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => reject_metadata(&metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(LedgerError::new(LedgerErrorCode::Unavailable)),
        }
    }
    Ok(())
}

pub(crate) fn reject_existing_target(path: &Path) -> Result<(), LedgerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => reject_metadata(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(LedgerError::new(LedgerErrorCode::Unavailable)),
    }
}

fn reject_metadata(metadata: &fs::Metadata) -> Result<(), LedgerError> {
    if metadata.file_type().is_symlink() || metadata_is_reparse_point(metadata) {
        return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.is_file() && metadata.nlink() > 1 {
            return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
        }
    }
    Ok(())
}

#[cfg(windows)]
fn metadata_is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn metadata_is_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

#[derive(Debug)]
struct OpenedFile {
    file: File,
    created: bool,
}

fn open_restrictive(path: &Path, append: bool) -> Result<OpenedFile, LedgerError> {
    let mut create_options = restrictive_open_options(append)?;
    create_options.create_new(true);
    match create_options.open(path) {
        Ok(file) => {
            validate_opened_target(&file, path)?;
            return Ok(OpenedFile {
                file,
                created: true,
            });
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(classify_open_error(path, error)),
    }

    let file = open_existing_restrictive(path, append)?;
    Ok(OpenedFile {
        file,
        created: false,
    })
}

pub(crate) fn open_existing_restrictive(path: &Path, append: bool) -> Result<File, LedgerError> {
    let file = restrictive_open_options(append)?
        .open(path)
        .map_err(|error| classify_open_error(path, error))?;
    validate_opened_target(&file, path)?;
    Ok(file)
}

pub(crate) fn open_snapshot_guard_restrictive(path: &Path) -> Result<File, LedgerError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(unix_no_follow_flag()?);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_SHARE_READ: u32 = 0x1;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(not(any(unix, windows)))]
    return Err(LedgerError::new(LedgerErrorCode::UnsafePath));

    let file = options
        .open(path)
        .map_err(|error| classify_open_error(path, error))?;
    validate_opened_target(&file, path)?;
    Ok(file)
}

fn acquire_lifetime_lock(file: &File) -> Result<(), LedgerError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(LedgerError::new(LedgerErrorCode::Locked)),
        Err(TryLockError::Error(_)) => Err(LedgerError::new(LedgerErrorCode::Unavailable)),
    }
}

#[cfg(unix)]
fn open_parent_lock(path: &Path) -> Result<File, LedgerError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options.read(true).custom_flags(unix_no_follow_flag()?);
    let file = options
        .open(path)
        .map_err(|error| classify_open_error(path, error))?;
    validate_opened_directory(&file, path)?;
    acquire_lifetime_lock(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn validate_opened_directory(file: &File, path: &Path) -> Result<(), LedgerError> {
    use std::os::unix::fs::MetadataExt;

    let opened = file
        .metadata()
        .map_err(|_| LedgerError::new(LedgerErrorCode::UnsafePath))?;
    let named =
        fs::symlink_metadata(path).map_err(|_| LedgerError::new(LedgerErrorCode::UnsafePath))?;
    reject_metadata(&opened)?;
    reject_metadata(&named)?;
    if !opened.is_dir()
        || !named.is_dir()
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
    {
        return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
    }
    Ok(())
}

fn restrictive_open_options(append: bool) -> Result<OpenOptions, LedgerError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).append(append);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(unix_no_follow_flag()?);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_SHARE_READ: u32 = 0x1;
        const FILE_SHARE_WRITE: u32 = 0x2;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(not(any(unix, windows)))]
    return Err(LedgerError::new(LedgerErrorCode::UnsafePath));

    Ok(options)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn unix_no_follow_flag() -> Result<i32, LedgerError> {
    Ok(0o400_000)
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn unix_no_follow_flag() -> Result<i32, LedgerError> {
    Ok(0x100)
}

#[cfg(any(target_os = "solaris", target_os = "illumos"))]
fn unix_no_follow_flag() -> Result<i32, LedgerError> {
    Ok(0x20_000)
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "solaris",
        target_os = "illumos"
    ))
))]
fn unix_no_follow_flag() -> Result<i32, LedgerError> {
    Err(LedgerError::new(LedgerErrorCode::UnsafePath))
}

fn classify_open_error(path: &Path, _error: io::Error) -> LedgerError {
    if fs::symlink_metadata(path)
        .map(|metadata| reject_metadata(&metadata).is_err())
        .unwrap_or(false)
    {
        LedgerError::new(LedgerErrorCode::UnsafePath)
    } else {
        LedgerError::new(LedgerErrorCode::Unavailable)
    }
}

pub(crate) fn validate_opened_target(file: &File, path: &Path) -> Result<(), LedgerError> {
    let opened = file
        .metadata()
        .map_err(|_| LedgerError::new(LedgerErrorCode::UnsafePath))?;
    let named =
        fs::symlink_metadata(path).map_err(|_| LedgerError::new(LedgerErrorCode::UnsafePath))?;
    reject_metadata(&opened)?;
    reject_metadata(&named)?;
    if !opened.is_file() || !named.is_file() {
        return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if opened.dev() != named.dev() || opened.ino() != named.ino() {
            return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        // Denying FILE_SHARE_DELETE keeps this directory entry bound to the
        // handle. Stable std does not yet expose Windows file IDs, so compare
        // the stable handle/path fields and fail closed on any mismatch.
        if opened.file_attributes() != named.file_attributes()
            || opened.creation_time() != named.creation_time()
            || opened.file_size() != named.file_size()
        {
            return Err(LedgerError::new(LedgerErrorCode::UnsafePath));
        }
    }
    Ok(())
}

fn run_durability_probe(
    paths: &LedgerPaths,
    durability: &dyn DurabilityOps,
) -> Result<(), LedgerError> {
    let mut temp = NamedTempFile::new_in(&paths.parent)
        .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
    temp.write_all(b"execution-ledger-durability-probe\n")
        .and_then(|_| temp.flush())
        .and_then(|_| temp.as_file().sync_all())
        .map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))?;
    let target = paths
        .parent
        .join(format!(".execution-ledger-probe-{}", Uuid::new_v4()));
    durability
        .persist(temp, &target)
        .map_err(|_| LedgerError::new(LedgerErrorCode::PersistFailed))?;
    let sync_result = durability.sync_directory(&paths.parent);
    let remove_result = fs::remove_file(&target);
    if sync_result.is_err() {
        return Err(LedgerError::new(LedgerErrorCode::DirectorySyncFailed));
    }
    remove_result.map_err(|_| LedgerError::new(LedgerErrorCode::Unavailable))
}

pub(crate) trait DurabilityOps: SnapshotDurability + Send + Sync {
    fn append(&self, file: &mut File, bytes: &[u8]) -> io::Result<()>;
    fn flush(&self, file: &mut File) -> io::Result<()>;
    fn sync_file(&self, file: &File) -> io::Result<()>;
    fn persist(&self, temp: NamedTempFile, target: &Path) -> io::Result<()>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
}

struct SystemDurabilityOps;

impl SnapshotDurability for SystemDurabilityOps {
    fn create_snapshot_temp(&self, parent: &Path) -> io::Result<NamedTempFile> {
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

    fn persist_snapshot(&self, temp: NamedTempFile, target: &Path) -> io::Result<()> {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }

    fn sync_snapshot_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory_supported(path)
    }
}

impl DurabilityOps for SystemDurabilityOps {
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

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory_supported(path)
    }
}

#[cfg(windows)]
fn sync_directory_supported(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    match directory.sync_all() {
        Ok(()) => Ok(()),
        Err(error) if matches!(error.raw_os_error(), Some(1 | 5 | 6 | 87)) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn sync_directory_supported(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self, File},
        io::{self, Write},
        path::{Path, PathBuf},
        process::{Child, Command},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        thread,
        time::{Duration, Instant},
    };

    use chrono::{TimeZone, Utc};
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    use super::*;
    use crate::service::execution_ledger::{
        EventHash, EventId, IntentId, IntentPurpose, LedgerErrorCode, LedgerEvent, LedgerPayload,
        OrderId, OrderSide, OrderType, PositionSeed, PreparedIntent, TokenId, Venue,
        LEDGER_SCHEMA_VERSION, ORDER_PROTOCOL_VERSION,
    };

    const CHILD_LEDGER_ENV: &str = "POLYMARKET_LEDGER_LOCK_CHILD_PATH";
    const CHILD_READY_ENV: &str = "POLYMARKET_LEDGER_LOCK_CHILD_READY";
    const CHILD_RELEASE_ENV: &str = "POLYMARKET_LEDGER_LOCK_CHILD_RELEASE";
    const CHILD_PROJECTION_ENV: &str = "POLYMARKET_LEDGER_PROJECTION_CHILD_PATH";

    fn intent_id(value: u128) -> IntentId {
        IntentId(Uuid::from_u128(value))
    }

    fn event_id(value: u128) -> EventId {
        EventId(Uuid::from_u128(value))
    }

    fn recorded_at() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 18, 12, 34, 56)
            .single()
            .unwrap()
    }

    fn order_id(byte: u8) -> OrderId {
        OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
    }

    fn prepared_payload() -> LedgerPayload {
        LedgerPayload::IntentPrepared(PreparedIntent {
            order_id: order_id(0x11),
            protocol_version: ORDER_PROTOCOL_VERSION,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("123456789").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 500_000,
            expected_taker_micros: 1_000_000,
            source_hash: None,
            purpose: IntentPurpose::Entry(PositionSeed {
                slug: "storage-fixture".to_owned(),
                category: "tests".to_owned(),
                tags: vec!["offline".to_owned()],
                take_profit_bps: 1_000,
                stop_loss_bps: 500,
            }),
        })
    }

    fn fixed_event(
        sequence: u64,
        event_id: EventId,
        intent_id: IntentId,
        payload: LedgerPayload,
        previous_hash: EventHash,
    ) -> LedgerEvent {
        let mut event = LedgerEvent {
            schema_version: LEDGER_SCHEMA_VERSION,
            sequence,
            event_id,
            intent_id,
            recorded_at: recorded_at(),
            payload,
            previous_hash,
            event_hash: EventHash::default(),
        };
        event.event_hash = calculate_event_hash(&HashMaterial::from_event(&event)).unwrap();
        event
    }

    fn write_events(path: &Path, events: &[LedgerEvent]) {
        let mut bytes = Vec::new();
        for event in events {
            serde_json::to_writer(&mut bytes, event).unwrap();
            bytes.push(b'\n');
        }
        fs::write(path, bytes).unwrap();
    }

    fn derived(path: &Path, suffix: &str) -> PathBuf {
        let mut name = path.file_name().unwrap().to_os_string();
        name.push(suffix);
        path.with_file_name(name)
    }

    #[test]
    fn first_append_reopens_with_the_same_projection_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let intent = intent_id(1);

        let ledger = ExecutionLedger::open_live(&path).unwrap();
        let AppendOutcome::Appended(appended) = ledger.append(intent, prepared_payload()).unwrap();
        assert_eq!(appended.sequence, 1);
        assert_eq!(appended.previous_hash, EventHash::default());
        assert_eq!(ledger.projection().head_hash, appended.event_hash);
        drop(ledger);

        let reopened = ExecutionLedger::open_live(&path).unwrap();
        let projection = reopened.projection();
        assert_eq!(projection.sequence, 1);
        assert_eq!(projection.head_hash, appended.event_hash);
        assert_eq!(projection.active.as_ref().unwrap().intent_id, intent);
    }

    #[test]
    fn ordered_multi_event_replay_applies_each_line_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let intent = intent_id(2);
        let ledger = ExecutionLedger::open_live(&path).unwrap();
        ledger.append(intent, prepared_payload()).unwrap();
        let AppendOutcome::Appended(second) =
            ledger.append(intent, LedgerPayload::SubmitStarted).unwrap();
        drop(ledger);

        let reopened = ExecutionLedger::open_live(&path).unwrap();
        let projection = reopened.projection();
        assert_eq!(projection.sequence, 2);
        assert_eq!(projection.head_hash, second.event_hash);
        assert_eq!(projection.event_count, 2);
    }

    #[test]
    fn concurrent_duplicate_appends_are_serialized_without_duplicate_sequences() {
        const CONTENDERS: usize = 8;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let intent = intent_id(200);
        let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
        ledger.append(intent, prepared_payload()).unwrap();
        let barrier = Arc::new(Barrier::new(CONTENDERS));

        let handles: Vec<_> = (0..CONTENDERS)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    ledger.append(intent, LedgerPayload::SubmitStarted)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert!(outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().err())
            .all(|error| error.code() == LedgerErrorCode::IllegalTransition));
        assert_eq!(ledger.projection().sequence, 2);
        drop(ledger);

        let reopened = ExecutionLedger::open_live(&path).unwrap();
        assert_eq!(reopened.projection().event_count, 2);
    }

    #[test]
    fn fixed_field_hash_material_has_a_stable_sha256_golden() {
        let event = LedgerEvent {
            schema_version: 1,
            sequence: 1,
            event_id: event_id(1_000),
            intent_id: intent_id(1),
            recorded_at: recorded_at(),
            payload: LedgerPayload::SubmitStarted,
            previous_hash: EventHash::default(),
            event_hash: EventHash::default(),
        };

        assert_eq!(
            calculate_event_hash(&HashMaterial::from_event(&event))
                .unwrap()
                .to_string(),
            "638774253072d88018fb7b68ed0599536ce9041485ce2d329d3a8be51a36365a"
        );
    }

    #[test]
    fn a_physically_duplicated_jsonl_line_fails_before_logical_idempotency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let event = fixed_event(
            1,
            event_id(1_001),
            intent_id(3),
            prepared_payload(),
            EventHash::default(),
        );
        write_events(&path, &[event.clone(), event]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::SequenceMismatch);
    }

    #[test]
    fn a_conflicting_duplicate_event_id_fails_after_physical_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let first = fixed_event(
            1,
            event_id(1_002),
            intent_id(4),
            prepared_payload(),
            EventHash::default(),
        );
        let second = fixed_event(
            2,
            first.event_id,
            first.intent_id,
            LedgerPayload::SubmitStarted,
            first.event_hash.clone(),
        );
        write_events(&path, &[first, second]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::IdempotencyConflict);
    }

    #[test]
    fn skipped_sequence_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let event = fixed_event(
            2,
            event_id(1_003),
            intent_id(5),
            prepared_payload(),
            EventHash::default(),
        );
        write_events(&path, &[event]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::SequenceMismatch);
    }

    #[test]
    fn duplicate_sequence_with_distinct_content_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let first = fixed_event(
            1,
            event_id(1_004),
            intent_id(6),
            prepared_payload(),
            EventHash::default(),
        );
        let second = fixed_event(
            1,
            event_id(1_005),
            first.intent_id,
            LedgerPayload::SubmitStarted,
            first.event_hash.clone(),
        );
        write_events(&path, &[first, second]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::SequenceMismatch);
    }

    #[test]
    fn broken_previous_hash_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let event = fixed_event(
            1,
            event_id(1_006),
            intent_id(7),
            prepared_payload(),
            EventHash::from_bytes([0x55; 32]),
        );
        write_events(&path, &[event]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::PreviousHashMismatch);
    }

    #[test]
    fn broken_current_hash_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let mut event = fixed_event(
            1,
            event_id(1_007),
            intent_id(8),
            prepared_payload(),
            EventHash::default(),
        );
        event.event_hash = EventHash::from_bytes([0xaa; 32]);
        write_events(&path, &[event]);

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::EventHashMismatch);
    }

    #[test]
    fn invalid_json_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&path, b"{not-json}\n").unwrap();

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::InvalidJson);
    }

    #[test]
    fn unknown_schema_fails_before_transition_logic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let event = fixed_event(
            1,
            event_id(1_008),
            intent_id(9),
            prepared_payload(),
            EventHash::default(),
        );
        let mut value = serde_json::to_value(event).unwrap();
        value["schema_version"] = serde_json::json!(99);
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&value).unwrap()),
        )
        .unwrap();

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsupportedSchema);
    }

    #[test]
    fn unknown_event_kind_is_not_accepted_as_generic_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let event = fixed_event(
            1,
            event_id(1_009),
            intent_id(10),
            prepared_payload(),
            EventHash::default(),
        );
        let mut value = serde_json::to_value(event).unwrap();
        value["kind"] = serde_json::json!("future_event");
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&value).unwrap()),
        )
        .unwrap();

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsupportedEventKind);
    }

    #[test]
    fn truncated_tail_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&path, br#"{"schema_version":1}"#).unwrap();

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::TruncatedTail);
    }

    #[test]
    fn non_directory_parent_is_unavailable_without_creating_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("not-a-directory");
        fs::write(&parent, b"sentinel").unwrap();
        let path = parent.join("execution-ledger.jsonl");

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::Unavailable);
        assert_eq!(fs::read(parent).unwrap(), b"sentinel");
    }

    #[test]
    fn configured_ledger_symlink_is_rejected_where_supported() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.jsonl");
        let link = dir.path().join("execution-ledger.jsonl");
        fs::write(&target, b"").unwrap();
        if create_file_symlink(&target, &link).is_err() {
            return;
        }

        let error = ExecutionLedger::open_live(&link).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
    }

    #[test]
    fn restrictive_open_does_not_follow_a_swapped_symlink_target_where_supported() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside.jsonl");
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&outside, b"").unwrap();
        if create_file_symlink(&outside, &path).is_err() {
            return;
        }

        let error = open_restrictive(&path, true).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
        assert_eq!(fs::read(&outside).unwrap(), b"");
    }

    #[cfg(windows)]
    #[test]
    fn opened_windows_handle_denies_path_replacement_for_its_lifetime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&path, b"").unwrap();

        let opened = open_restrictive(&path, true).unwrap();
        assert!(fs::remove_file(&path).is_err());
        drop(opened);
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn derived_lock_symlink_is_rejected_where_supported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let target = dir.path().join("target.lock");
        fs::write(&target, b"").unwrap();
        if create_file_symlink(&target, &derived(&path, ".lock")).is_err() {
            return;
        }

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
    }

    #[test]
    fn derived_snapshot_symlink_is_rejected_where_supported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let target = dir.path().join("target.active.json");
        fs::write(&target, b"").unwrap();
        if create_file_symlink(&target, &derived(&path, ".active.json")).is_err() {
            return;
        }

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
    }

    #[test]
    fn configured_parent_symlink_or_reparse_point_is_rejected_where_supported() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target-parent");
        let link = dir.path().join("linked-parent");
        fs::create_dir(&target).unwrap();
        if create_dir_symlink(&target, &link).is_err() {
            return;
        }

        let error = ExecutionLedger::open_live(link.join("execution-ledger.jsonl")).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_ledger_target_is_rejected_where_link_count_is_available() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.jsonl");
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&target, b"").unwrap();
        if fs::hard_link(&target, &path).is_err() {
            return;
        }

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::UnsafePath);
    }

    #[test]
    fn lock_is_held_by_the_file_handle_for_the_complete_ledger_lifetime() {
        if env::var_os(CHILD_LEDGER_ENV).is_some() {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ready = dir.path().join("child.ready");
        let release = dir.path().join("child.release");
        let mut child = spawn_lock_holder(&path, &ready, &release);

        if !wait_for_path(&ready, Duration::from_secs(10)) {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("lock holder did not become ready: {output:?}");
        }

        let error = ExecutionLedger::open_live(&path).unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::Locked);
        fs::write(&release, b"release").unwrap();
        assert!(child.wait().unwrap().success());

        ExecutionLedger::open_live(&path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_lifetime_lock_survives_sibling_lock_unlink_and_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ready = dir.path().join("child.ready");
        let release = dir.path().join("child.release");
        let mut child = spawn_lock_holder(&path, &ready, &release);

        if !wait_for_path(&ready, Duration::from_secs(10)) {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("lock holder did not become ready: {output:?}");
        }

        let lock_path = derived(&path, ".lock");
        fs::remove_file(&lock_path).unwrap();
        fs::write(&lock_path, b"replacement").unwrap();

        let second_open_code = match ExecutionLedger::open_live(&path) {
            Ok(second) => {
                drop(second);
                None
            }
            Err(error) => Some(error.code()),
        };

        fs::write(&release, b"release").unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(second_open_code, Some(LedgerErrorCode::Locked));
        ExecutionLedger::open_live(&path).unwrap();
    }

    #[test]
    fn lock_holder_child_process() {
        let Some(path) = env::var_os(CHILD_LEDGER_ENV).map(PathBuf::from) else {
            return;
        };
        let ready = PathBuf::from(env::var_os(CHILD_READY_ENV).unwrap());
        let release = PathBuf::from(env::var_os(CHILD_RELEASE_ENV).unwrap());

        let _ledger = ExecutionLedger::open_live(path).unwrap();
        fs::write(ready, b"ready").unwrap();
        assert!(wait_for_path(&release, Duration::from_secs(10)));
    }

    #[test]
    fn partial_append_failure_poisons_instance_and_replay_rejects_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingDurabilityOps::new(FailurePoint::AppendPartial));
        let ledger = ExecutionLedger::open_live_with_ops(&path, ops).unwrap();

        let error = ledger
            .append(intent_id(20), prepared_payload())
            .unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::AppendFailed);
        assert_eq!(
            ledger
                .append(intent_id(20), prepared_payload())
                .unwrap_err()
                .code(),
            LedgerErrorCode::Fatal
        );
        assert_eq!(ledger.projection().sequence, 0);
        drop(ledger);

        assert_eq!(
            ExecutionLedger::open_live(&path).unwrap_err().code(),
            LedgerErrorCode::TruncatedTail
        );
    }

    #[test]
    fn flush_failure_poisons_instance_and_complete_line_without_snapshot_fails_closed() {
        assert_complete_line_failure_is_poisoned(FailurePoint::Flush, LedgerErrorCode::FlushFailed);
    }

    #[test]
    fn sync_failure_poisons_instance_and_complete_line_without_snapshot_fails_closed() {
        assert_complete_line_failure_is_poisoned(FailurePoint::Sync, LedgerErrorCode::SyncFailed);
    }

    #[test]
    fn fresh_ledger_file_sync_failure_is_typed_and_releases_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingDurabilityOps::new(FailurePoint::Sync));

        assert_eq!(
            ExecutionLedger::open_live_with_ops(&path, ops)
                .unwrap_err()
                .code(),
            LedgerErrorCode::SyncFailed
        );
        assert_eq!(
            ExecutionLedger::open_live(&path)
                .unwrap()
                .projection()
                .sequence,
            0
        );
    }

    #[test]
    fn fresh_ledger_parent_sync_failure_is_typed_and_releases_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingDurabilityOps::new(FailurePoint::DirectorySyncCall(
            2,
        )));

        assert_eq!(
            ExecutionLedger::open_live_with_ops(&path, ops)
                .unwrap_err()
                .code(),
            LedgerErrorCode::DirectorySyncFailed
        );
        assert_eq!(
            ExecutionLedger::open_live(&path)
                .unwrap()
                .projection()
                .sequence,
            0
        );
    }

    #[test]
    fn durability_probe_persist_failure_is_typed_and_releases_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingDurabilityOps::new(FailurePoint::Persist));

        assert_eq!(
            ExecutionLedger::open_live_with_ops(&path, ops)
                .unwrap_err()
                .code(),
            LedgerErrorCode::PersistFailed
        );
        ExecutionLedger::open_live(&path).unwrap();
    }

    #[test]
    fn durability_probe_directory_sync_failure_is_typed_and_releases_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ops = Arc::new(FailingDurabilityOps::new(FailurePoint::DirectorySync));

        assert_eq!(
            ExecutionLedger::open_live_with_ops(&path, ops)
                .unwrap_err()
                .code(),
            LedgerErrorCode::DirectorySyncFailed
        );
        ExecutionLedger::open_live(&path).unwrap();
    }

    #[test]
    fn illegal_transition_is_rejected_before_any_bytes_are_appended() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let ledger = ExecutionLedger::open_live(&path).unwrap();

        let error = ledger
            .append(intent_id(30), LedgerPayload::SubmitStarted)
            .unwrap_err();
        assert_eq!(error.code(), LedgerErrorCode::IllegalTransition);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);

        ledger.append(intent_id(30), prepared_payload()).unwrap();
    }

    #[test]
    fn projection_read_is_owned_and_does_not_block_a_later_append() {
        if env::var_os(CHILD_PROJECTION_ENV).is_some() {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let mut child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("service::execution_ledger::storage::tests::projection_read_append_child_process")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_PROJECTION_ENV, &path)
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("projection read retained the ledger mutex across append");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn projection_read_append_child_process() {
        let Some(path) = env::var_os(CHILD_PROJECTION_ENV).map(PathBuf::from) else {
            return;
        };

        let ledger = ExecutionLedger::open_live(path).unwrap();
        let before = ledger.projection();
        ledger.append(intent_id(31), prepared_payload()).unwrap();
        assert_eq!(before.sequence, 0);
        assert_eq!(ledger.projection().sequence, 1);
    }

    fn assert_complete_line_failure_is_poisoned(point: FailurePoint, code: LedgerErrorCode) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        fs::write(&path, b"").unwrap();
        let ops = Arc::new(FailingDurabilityOps::new(point));
        let ledger = ExecutionLedger::open_live_with_ops(&path, ops).unwrap();

        assert_eq!(
            ledger
                .append(intent_id(21), prepared_payload())
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            ledger
                .append(intent_id(21), prepared_payload())
                .unwrap_err()
                .code(),
            LedgerErrorCode::Fatal
        );
        assert_eq!(ledger.projection().sequence, 0);
        drop(ledger);

        assert_eq!(
            ExecutionLedger::open_live(&path).unwrap_err().code(),
            LedgerErrorCode::SnapshotMissing
        );
    }

    fn spawn_lock_holder(path: &Path, ready: &Path, release: &Path) -> Child {
        Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("service::execution_ledger::storage::tests::lock_holder_child_process")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_LEDGER_ENV, path)
            .env(CHILD_READY_ENV, ready)
            .env(CHILD_RELEASE_ENV, release)
            .spawn()
            .unwrap()
    }

    fn wait_for_path(path: &Path, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    #[cfg(windows)]
    fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    #[cfg(windows)]
    fn create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(unix)]
    fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(unix)]
    fn create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[derive(Clone, Copy)]
    enum FailurePoint {
        AppendPartial,
        Flush,
        Sync,
        Persist,
        DirectorySync,
        DirectorySyncCall(usize),
    }

    struct FailingDurabilityOps {
        point: FailurePoint,
        directory_sync_calls: AtomicUsize,
    }

    impl FailingDurabilityOps {
        fn new(point: FailurePoint) -> Self {
            Self {
                point,
                directory_sync_calls: AtomicUsize::new(0),
            }
        }
    }

    impl DurabilityOps for FailingDurabilityOps {
        fn append(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            if matches!(self.point, FailurePoint::AppendPartial) {
                file.write_all(&bytes[..bytes.len() / 2])?;
                return Err(io::Error::other("injected append"));
            }
            file.write_all(bytes)
        }

        fn flush(&self, file: &mut File) -> io::Result<()> {
            if matches!(self.point, FailurePoint::Flush) {
                return Err(io::Error::other("injected flush"));
            }
            file.flush()
        }

        fn sync_file(&self, file: &File) -> io::Result<()> {
            if matches!(self.point, FailurePoint::Sync) {
                return Err(io::Error::other("injected sync"));
            }
            file.sync_all()
        }

        fn persist(&self, temp: NamedTempFile, target: &Path) -> io::Result<()> {
            if matches!(self.point, FailurePoint::Persist) {
                return Err(io::Error::other("injected persist"));
            }
            temp.persist(target)
                .map(|_| ())
                .map_err(|error| error.error)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            let call = self.directory_sync_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.point, FailurePoint::DirectorySync)
                || matches!(self.point, FailurePoint::DirectorySyncCall(expected) if call == expected)
            {
                return Err(io::Error::other("injected directory sync"));
            }
            sync_directory_supported(path)
        }
    }

    impl SnapshotDurability for FailingDurabilityOps {
        fn create_snapshot_temp(&self, parent: &Path) -> io::Result<NamedTempFile> {
            SystemDurabilityOps.create_snapshot_temp(parent)
        }

        fn write_snapshot(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            SystemDurabilityOps.write_snapshot(file, bytes)
        }

        fn flush_snapshot(&self, file: &mut File) -> io::Result<()> {
            SystemDurabilityOps.flush_snapshot(file)
        }

        fn sync_snapshot(&self, file: &File) -> io::Result<()> {
            SystemDurabilityOps.sync_snapshot(file)
        }

        fn persist_snapshot(&self, temp: NamedTempFile, target: &Path) -> io::Result<()> {
            SystemDurabilityOps.persist_snapshot(temp, target)
        }

        fn sync_snapshot_directory(&self, path: &Path) -> io::Result<()> {
            SystemDurabilityOps.sync_snapshot_directory(path)
        }
    }
}
