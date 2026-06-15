use async_hybrid_fs::{HybridDir, HybridRead, OpenOptions};
use procfs_core::{
    FromRead, ProcError, ProcResult,
    process::{Io, Schedstat, Stat, Status, Syscall},
};
use tokio::fs::File;

use super::Process;
use crate::{AsyncProcfs, helpers::wrap_io_error};
use nix::{fcntl::OFlag, sys::stat::Mode};
use std::{
    io::Cursor,
    path::{Path, PathBuf},
};

/// A task (aka Thread) inside of a [`Process`](crate::process::Process)
///
/// Created by [`Process::tasks`](crate::process::Process::tasks), tasks in
/// general are similar to Processes and should have mostly the same fields.
#[derive(Debug)]
pub struct Task {
    fd: File,
    /// The ID of the process that this task belongs to
    pub pid: i32,
    /// The task ID
    pub tid: i32,
    /// Task path: `/proc/<pid>/task/<tid>`
    pub(crate) path: PathBuf,
    /// The procfs instance
    pub(crate) _procfs: AsyncProcfs,
}

impl Task {
    /// Create a new `Task` inside of the process
    ///
    /// This API is designed to be ergonomic from inside of [`TasksIter`](super::TasksIter)
    pub(crate) async fn from_process_at(process: &Process, tid: i32) -> ProcResult<Task> {
        let relative_path = PathBuf::from("task").join(tid.to_string());
        let path = process.path.join(&relative_path);
        let fd = wrap_io_error!(
            path.to_owned(),
            process
                .fd
                .open_at(
                    &relative_path,
                    &OpenOptions::from_flag_and_mode(
                        OFlag::O_PATH | OFlag::O_DIRECTORY,
                        Mode::empty()
                    )
                )
                .await
        )?;
        Ok(Task {
            fd,
            pid: process.pid,
            tid,
            path: path.to_owned(),
            _procfs: process.procfs.clone(),
        })
    }

    async fn read_with_cursor<T: FromRead>(&self, relpath: impl AsRef<Path>) -> ProcResult<T> {
        let mut file = wrap_io_error!(
            self.path.join(relpath.as_ref()),
            self.fd
                .open_at(relpath.as_ref(), OpenOptions::new().read(true))
                .await
        )?;
        // Read everything then convert to bytes, since async files do not implement the Read trait
        let mut buffer = Vec::new();
        let _bytes_read =
            wrap_io_error!(self.path.join("io"), file.read_to_end(&mut buffer).await)?;
        let cursor = Cursor::new(buffer);
        FromRead::from_read(cursor)
    }

    /// Thread info from `/proc/<pid>/task/<tid>/stat`
    ///
    /// Many of the returned fields will be the same as the parent process, but some fields like `utime` and `stime` will be per-task
    pub async fn stat(&self) -> ProcResult<Stat> {
        self.read_with_cursor("stat").await
    }

    /// Thread info from `/proc/<pid>/task/<tid>/status`
    ///
    /// Many of the returned fields will be the same as the parent process
    pub async fn status(&self) -> ProcResult<Status> {
        self.read_with_cursor("status").await
    }

    /// Thread IO info from `/proc/<pid>/task/<tid>/io`
    ///
    /// This data will be unique per task.
    pub async fn io(&self) -> ProcResult<Io> {
        self.read_with_cursor("io").await
    }

    /// Thread scheduler info from `/proc/<pid>/task/<tid>/schedstat`
    ///
    /// This data will be unique per task.
    pub async fn schedstat(&self) -> ProcResult<Schedstat> {
        self.read_with_cursor("schedstat").await
    }

    /// Returns the status info from `/proc/<pid>/task/<tid>/syscall`.
    pub async fn syscall(&self) -> ProcResult<Syscall> {
        self.read_with_cursor("syscall").await
    }

    /// Thread children from `/proc/<pid>/task/<tid>/children`
    ///
    /// WARNING:
    /// This interface is not reliable unless all the child processes are stoppped or frozen.
    /// If a child task exits while the file is being read, non-exiting children may be omitted.
    /// See the procfs(5) man page for more information.
    ///
    /// This data will be unique per task.
    pub async fn children(&self) -> ProcResult<Vec<u32>> {
        let mut buf = String::new();
        let mut file = wrap_io_error!(
            self.path.join("children"),
            self.fd
                .open_at("children", OpenOptions::new().read(true))
                .await
        )?;
        file.read_to_string(&mut buf).await?;
        buf.split_whitespace()
            .map(|child| {
                child
                    .parse()
                    .map_err(|_| ProcError::Other("Failed to parse task's child PIDs".to_string()))
            })
            .collect()
    }

    /// Deliberately generate an IO error
    #[allow(unused)]
    #[cfg(test)]
    pub(crate) async fn generate_error(&self) -> ProcResult<()> {
        let _ = wrap_io_error!(
            self.path.join("does_not_exist"),
            self.fd
                .open_at("does_not_exist", OpenOptions::new().read(true))
                .await
        )?;
        Ok(())
    }
}
