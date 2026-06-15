pub(crate) mod helpers;
pub mod process;
pub(crate) mod runtime;
pub mod sys;
mod system_info;

use std::fmt::Debug;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use async_hybrid_fs::HybridDir;
use async_hybrid_fs::HybridRead;
use async_hybrid_fs::OpenOptions;
use helpers::AsyncProcfsIo;
use procfs_core::FromRead;
use procfs_core::FromReadSI;
use procfs_core::IoErrorWrapper;
use procfs_core::SystemInfoInterface;
use sys::AsyncProcfsSys;
use tokio::fs::File;

pub use procfs_core::ProcError;
pub use procfs_core::ProcErrorExt;
pub use procfs_core::ProcResult;

use crate::helpers::wrap_io_error;
use crate::system_info::AsyncSystemInfo;

#[derive(Debug)]
struct AsyncProcfsState {
    file: File,
    root: PathBuf,
}

/// A handle to the procfs instance
#[derive(Clone)]
pub struct AsyncProcfs {
    inner: Arc<AsyncProcfsState>,
}

impl AsyncProcfs {
    pub(crate) fn io(&self) -> AsyncProcfsIo<'_> {
        AsyncProcfsIo::new(self)
    }

    pub fn sys(&self) -> AsyncProcfsSys<'_> {
        AsyncProcfsSys::new(self)
    }
}

impl Debug for AsyncProcfs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncProcfs").finish()
    }
}

#[async_trait::async_trait]
pub trait AsyncCurrent: FromRead {
    const RELATIVE_PATH: &'static str;

    async fn current(procfs: &AsyncProcfs) -> ProcResult<Self> {
        let mut file = wrap_io_error!(
            procfs.inner.root.join(Self::RELATIVE_PATH),
            procfs
                .inner
                .file
                .open_at(Self::RELATIVE_PATH, OpenOptions::new().read(true))
                .await
        )?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        let cursor = Cursor::new(buffer);
        Self::from_read(cursor)
    }
}

#[async_trait::async_trait]
pub trait AsyncCurrentSI: FromReadSI {
    const RELATIVE_PATH: &'static str;

    async fn current(procfs: &AsyncProcfs) -> ProcResult<Self> {
        let si = system_info::current_system_info(procfs);
        Self::current_with_system_info(procfs, &si).await
    }

    async fn current_with_system_info(
        procfs: &AsyncProcfs,
        si: &AsyncSystemInfo,
    ) -> ProcResult<Self> {
        let mut file = wrap_io_error!(
            procfs.inner.root.join(Self::RELATIVE_PATH),
            procfs
                .inner
                .file
                .open_at(Self::RELATIVE_PATH, OpenOptions::new().read(true))
                .await
        )?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        let cursor = Cursor::new(buffer);
        let si_ll: &dyn SystemInfoInterface = si;
        let res = FromReadSI::from_read(cursor, si_ll)?;
        Ok(res)
    }
}
