pub mod task;

use async_hybrid_fs::{HybridDir, HybridFile, HybridRead, Metadata, OpenOptions, Permissions};
use futures::{Stream, StreamExt};
use nix::fcntl::{AtFlags, OFlag};
use nix::sys::stat::Mode;
use procfs_core::net::{TcpNetEntry, UdpNetEntry};
use procfs_core::process::{
    ClearRefs, CoredumpFlags, FDTarget, Io, Limits, MemoryMaps, MountInfos, MountStats, Schedstat,
    SmapsRollup, Stat, StatM, Status, Syscall,
};
use procfs_core::{FromRead, FromReadSI, ProcError, ProcResult, expect, from_str, net};
use rustix::fs::OFlags;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::Cursor;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use task::Task;
use tokio::fs::File;

use crate::helpers::wrap_io_error;
use crate::system_info;
use crate::{AsyncProcfs, sys::kernel::Version};

pub struct FDInfo {
    // The file descriptor
    pub raw_fd: RawFd,
    /// The permission bits for this FD
    ///
    /// **Note**: this field is only the owner read/write/execute bits.  All the other bits
    /// (include filetype bits) are masked out.
    pub mode: Permissions,
    pub target: FDTarget,
}

impl FDInfo {
    pub async fn from_process_at(process: &Process, fd: RawFd) -> ProcResult<Self> {
        let procfs = process.procfs.clone();
        let path_from_process_fd = PathBuf::from("fd").join(fd.to_string());
        let path_from_fs_root = process.path.join(&path_from_process_fd);
        // for 2.6.39 <= kernel < 3.6 fstat doesn't support O_PATH see https://github.com/eminence/procfs/issues/265
        let flags = match Version::cached(&procfs).await {
            Ok(v) if v < Version::new(3, 6, 0) => OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Ok(_) => OFlag::O_NOFOLLOW | OFlag::O_PATH | OFlag::O_CLOEXEC,
            Err(_) => OFlag::O_NOFOLLOW | OFlag::O_PATH | OFlag::O_CLOEXEC,
        };
        let file = wrap_io_error!(
            path_from_fs_root.to_owned(),
            process
                .fd
                .open_at(
                    &path_from_process_fd,
                    &OpenOptions::from_flag_and_mode(flags, Mode::empty())
                )
                .await
        )?;
        let link = wrap_io_error!(path_from_fs_root.to_owned(), file.read_link().await)?;
        let metadata = wrap_io_error!(
            path_from_fs_root.to_owned(),
            file.hybrid_metadata_at("", AtFlags::AT_SYMLINK_NOFOLLOW | AtFlags::AT_EMPTY_PATH)
                .await
        )?;
        let link_os: &OsStr = link.as_ref();
        let target = expect!(FDTarget::from_str(expect!(link_os.to_str())));
        Ok(Self {
            raw_fd: fd,
            mode: Permissions::from_mode(metadata.mode() & Mode::S_IRWXU.bits()),
            target,
        })
    }
}

#[derive(Debug)]
pub struct Process {
    fd: File,
    pid: i32,
    /// The path relative to the root directory, used for debugging purposes (e.g. "/proc/9" or "/proc/self")
    path: PathBuf,
    /// The procfs instance
    procfs: AsyncProcfs,
}

impl Process {
    /// Returns a `Process` based on a specified PID.
    ///
    /// This can fail if the process doesn't exist, or if you don't have permission to access it.
    async fn new_with_path(
        procfs: &AsyncProcfs,
        relative_path: impl AsRef<Path>,
    ) -> ProcResult<Self> {
        let relative_path = relative_path.as_ref();
        let flags = match Version::cached(procfs).await {
            Ok(v) if v < Version::new(3, 6, 0) => OFlags::DIRECTORY | OFlags::CLOEXEC,
            Ok(_) => OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Err(_) => OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        };
        let file = wrap_io_error!(
            relative_path,
            procfs
                .inner
                .file
                .open_at(
                    relative_path,
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(flags.bits() as i32),
                )
                .await
        )?;
        // Resolve /proc/self
        let pidres = match relative_path
            .components()
            .next_back()
            .and_then(|c| match c {
                std::path::Component::Normal(s) => Some(s),
                _ => None,
            })
            .and_then(|s| s.to_string_lossy().parse::<i32>().ok())
        {
            Some(pid) => Some(pid),
            None => procfs
                .inner
                .file
                .read_link_at(relative_path)
                .await
                .ok()
                .and_then(|s| s.to_string_lossy().parse::<i32>().ok()),
        };
        let pid = match pidres {
            Some(pid) => pid,
            None => return Err(ProcError::NotFound(Some(relative_path.to_owned()))),
        };
        let path = procfs.inner.root.join(relative_path);
        Ok(Self {
            fd: file,
            pid,
            path: path.to_owned(),
            procfs: procfs.clone(),
        })
    }

    /// Returns a `Process` based on a specified `/proc/<pid>` path.
    pub async fn new(procfs: &AsyncProcfs, pid: i32) -> ProcResult<Self> {
        let root = PathBuf::from(pid.to_string());
        Self::new_with_path(procfs, root).await
    }

    /// Returns a `Process` for the currently running process.
    ///
    /// This is done by using the `/proc/self` symlink
    pub async fn myself(procfs: &AsyncProcfs) -> ProcResult<Self> {
        Self::new_with_path(procfs, "self").await
    }
}

impl Process {
    /// Returns the complete command line for the process, unless the process is a zombie.
    pub async fn cmdline(&self) -> ProcResult<Vec<String>> {
        let mut buf = String::new();
        let mut file = wrap_io_error!(
            self.path.join("cmdline"),
            self.fd
                .open_at("cmdline", OpenOptions::new().read(true))
                .await
        )?;
        wrap_io_error!(
            self.path.join("cmdline"),
            file.read_to_string(&mut buf).await
        )?;
        Ok(buf
            .split('\0')
            .filter_map(|s| {
                if !s.is_empty() {
                    Some(s.to_string())
                } else {
                    None
                }
            })
            .collect())
    }

    /// Returns the process ID for this process, if the process was created from an ID. Otherwise
    /// use stat().pid.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Is this process still alive?
    ///
    /// Processes in the Zombie or Dead state are not considered alive.
    pub async fn is_alive(&self) -> bool {
        if let Ok(stat) = self.stat().await {
            stat.state != 'Z' && stat.state != 'X'
        } else {
            false
        }
    }

    /// What user owns this process?
    pub async fn uid(&self) -> ProcResult<u32> {
        Ok(self.metadata().await?.uid())
    }

    async fn metadata(&self) -> ProcResult<Metadata> {
        Ok(self.fd.hybrid_metadata().await?)
    }

    /// Retrieves current working directory of the process by dereferencing `/proc/<pid>/cwd` symbolic link.
    ///
    /// This method has the following caveats:
    ///
    /// * if the pathname has been unlinked, the symbolic link will contain the string " (deleted)"
    ///   appended to the original pathname
    ///
    /// * in a multithreaded process, the contents of this symbolic link are not available if the
    ///   main thread has already terminated (typically by calling `pthread_exit(3)`)
    ///
    /// * permission to dereference or read this symbolic link is governed by a
    ///   `ptrace(2)` access mode `PTRACE_MODE_READ_FSCREDS` check
    pub async fn cwd(&self) -> ProcResult<PathBuf> {
        Ok(wrap_io_error!(
            self.path.join("cwd"),
            self.fd.read_link_at("cwd").await
        )?)
    }

    /// Retrieves current root directory of the process by dereferencing `/proc/<pid>/root` symbolic link.
    ///
    /// This method has the following caveats:
    ///
    /// * if the pathname has been unlinked, the symbolic link will contain the string " (deleted)"
    ///   appended to the original pathname
    ///
    /// * in a multithreaded process, the contents of this symbolic link are not available if the
    ///   main thread has already terminated (typically by calling `pthread_exit(3)`)
    ///
    /// * permission to dereference or read this symbolic link is governed by a
    ///   `ptrace(2)` access mode `PTRACE_MODE_READ_FSCREDS` check
    pub async fn root(&self) -> ProcResult<PathBuf> {
        Ok(wrap_io_error!(
            self.path.join("root"),
            self.fd.read_link_at("root").await
        )?)
    }

    /// Gets the current environment for the process.  This is done by reading the
    /// `/proc/pid/environ` file.
    pub async fn environ(&self) -> ProcResult<HashMap<OsString, OsString>> {
        use std::os::unix::ffi::OsStrExt;

        let mut map = HashMap::new();
        let mut file = wrap_io_error!(
            self.path.join("environ"),
            self.fd
                .open_at("environ", OpenOptions::new().read(true))
                .await
        )?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        for slice in buf.split(|b| *b == 0) {
            // slice will be in the form key=var, so split on the first equals sign
            let mut split = slice.splitn(2, |b| *b == b'=');
            if let (Some(k), Some(v)) = (split.next(), split.next()) {
                map.insert(
                    OsStr::from_bytes(k).to_os_string(),
                    OsStr::from_bytes(v).to_os_string(),
                );
            };
        }

        Ok(map)
    }

    /// Retrieves the actual path of the executed command by dereferencing `/proc/<pid>/exe` symbolic link.
    ///
    /// This method has the following caveats:
    ///
    /// * if the pathname has been unlinked, the symbolic link will contain the string " (deleted)"
    ///   appended to the original pathname
    ///
    /// * in a multithreaded process, the contents of this symbolic link are not available if the
    ///   main thread has already terminated (typically by calling `pthread_exit(3)`)
    ///
    /// * permission to dereference or read this symbolic link is governed by a
    ///   `ptrace(2)` access mode `PTRACE_MODE_READ_FSCREDS` check
    pub async fn exe(&self) -> ProcResult<PathBuf> {
        Ok(wrap_io_error!(
            self.path.join("exe"),
            self.fd.read_link_at("exe").await
        )?)
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

    async fn read_si_with_cursor<T: FromReadSI>(&self, relpath: impl AsRef<Path>) -> ProcResult<T> {
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
        let si = system_info::current_system_info(&self.procfs);
        FromReadSI::from_read(cursor, &si)
    }

    /// Return the Io stats for this process, based on the `/proc/pid/io` file.
    ///
    /// (since kernel 2.6.20)
    pub async fn io(&self) -> ProcResult<Io> {
        self.read_with_cursor("io").await
    }

    /// Return a list of the currently mapped memory regions and their access permissions, based on
    /// the `/proc/pid/maps` file.
    pub async fn maps(&self) -> ProcResult<MemoryMaps> {
        self.read_with_cursor("maps").await
    }

    /// Returns a list of currently mapped memory regions and verbose information about them,
    /// such as memory consumption per mapping, based on the `/proc/pid/smaps` file.
    ///
    /// (since Linux 2.6.14 and requires CONFIG_PROG_PAGE_MONITOR)
    pub async fn smaps(&self) -> ProcResult<MemoryMaps> {
        self.read_with_cursor("smaps").await
    }

    /// This is the sum of all the smaps data but it is much more performant to get it this way.
    ///
    /// Since 4.14 and requires CONFIG_PROC_PAGE_MONITOR.
    pub async fn smaps_rollup(&self) -> ProcResult<SmapsRollup> {
        self.read_with_cursor("smaps_rollup").await
    }

    /// Returns the [MountStat] data for this process's mount namespace.
    pub async fn mountstats(&self) -> ProcResult<MountStats> {
        self.read_with_cursor("mountstats").await
    }

    /// Returns info about the mountpoints in this this process's mount namespace.
    ///
    /// This data is taken from the `/proc/[pid]/mountinfo` file
    ///
    /// (Since Linux 2.6.26)
    pub async fn mountinfo(&self) -> ProcResult<MountInfos> {
        self.read_with_cursor("mountinfo").await
    }

    /// Gets the number of open file descriptors for a process
    ///
    /// Calling this function is more efficient than calling `fd().unwrap().count()`
    pub async fn fd_count(&self) -> ProcResult<usize> {
        // Use fast path if available (Linux v6.2): https://github.com/torvalds/linux/commit/f1f1f2569901
        let stat = wrap_io_error!(
            self.path.join("fd"),
            self.fd
                .hybrid_metadata_at("fd", AtFlags::AT_EMPTY_PATH)
                .await
        )?;
        if stat.size() > 0 {
            return Ok(stat.size() as usize);
        }

        let stream = self.fds().await?;
        Ok(stream.count().await)
    }

    /// Gets an asynchronous iterator over the open file descriptors for this process
    pub async fn fds(&self) -> ProcResult<impl Stream<Item = ProcResult<FDInfo>> + Send> {
        let rel_fd_path = self.path.join("fd");
        let fds_dir = wrap_io_error!(
            rel_fd_path.to_owned(),
            self.fd
                .open_at(
                    "fd",
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(OFlags::DIRECTORY.bits() as i32)
                )
                .await
        )?
        .try_into_std()
        .expect("should have no inflight operation");
        let fds_dir = wrap_io_error!(rel_fd_path, async_hybrid_fs::Dir::new(fds_dir))?;
        let dir_stream = fds_dir.into_stream();
        let stream = async_stream::stream! {
            for await fd_entry in dir_stream {
                let entry = fd_entry?;
                let name = entry.file_name().to_string_lossy();
                if let Ok(fd) = RawFd::from_str(&name) && let Ok(info) = FDInfo::from_process_at(self, fd).await {
                    yield Ok(info);
                }
            }
        };
        Ok(stream)
    }

    /// Gets the information of a process-owned file descriptor
    pub async fn fd_from_fd(&self, fd: RawFd) -> ProcResult<FDInfo> {
        FDInfo::from_process_at(self, fd).await
    }

    /// Lists which memory segments are written to the core dump in the event that a core dump is performed.
    ///
    /// By default, the following bits are set:
    /// 0, 1, 4 (if the CONFIG_CORE_DUMP_DEFAULT_ELF_HEADERS kernel configuration option is enabled), and 5.
    /// This default can be modified at boot time using the core dump_filter boot option.
    ///
    /// This function will return `Err(ProcError::NotFound)` if the `coredump_filter` file can't be
    /// found.  If it returns `Ok(None)` then the process has no coredump_filter
    pub async fn coredump_filter(&self) -> ProcResult<Option<CoredumpFlags>> {
        let path = self.path.join("coredump_filter");
        let mut file = wrap_io_error!(
            path.to_owned(),
            self.fd
                .open_at("coredump_filter", OpenOptions::new().read(true))
                .await
        )?;
        let mut s = String::new();
        file.read_to_string(&mut s).await?;
        if s.trim().is_empty() {
            return Ok(None);
        }
        let flags = from_str!(u32, &s.trim(), 16, pid: self.pid);
        Ok(Some(expect!(CoredumpFlags::from_bits(flags))))
    }

    /// Gets the process's autogroup membership
    ///
    /// (since Linux 2.6.38 and requires CONFIG_SCHED_AUTOGROUP)
    pub async fn autogroup(&self) -> ProcResult<String> {
        let mut s = String::new();
        let mut file = wrap_io_error!(
            self.path.join("autogroup"),
            self.fd
                .open_at("autogroup", OpenOptions::new().read(true))
                .await
        )?;
        file.read_to_string(&mut s).await?;
        Ok(s)
    }

    /// Get the process's auxiliary vector
    ///
    /// (since 2.6.0-test7)
    pub async fn auxv(&self) -> ProcResult<HashMap<u64, u64>> {
        let path_from_procfs_root = self.path.join("auxv");
        let mut file = wrap_io_error!(
            path_from_procfs_root.to_owned(),
            self.fd.open_at("auxv", OpenOptions::new().read(true)).await
        )?;
        let mut map = HashMap::new();

        let mut buf = Vec::new();
        let bytes_read = file.read_to_end(&mut buf).await?;
        if bytes_read == 0 {
            // some kernel processes won't have any data for their auxv file
            return Ok(map);
        }
        buf.truncate(bytes_read);
        let mut cursor = std::io::Cursor::new(buf);

        let mut buf = 0usize.to_ne_bytes();
        loop {
            use std::io::Read;

            cursor.read_exact(&mut buf)?;
            let key = usize::from_ne_bytes(buf) as u64;
            cursor.read_exact(&mut buf)?;
            let value = usize::from_ne_bytes(buf) as u64;
            if key == 0 && value == 0 {
                break;
            }
            map.insert(key, value);
        }

        Ok(map)
    }

    /// Gets the symbolic name corresponding to the location in the kernel where the process is sleeping.
    ///
    /// (since Linux 2.6.0)
    pub async fn wchan(&self) -> ProcResult<String> {
        let mut s = String::new();
        let mut file = wrap_io_error!(
            self.path.join("wchan"),
            self.fd
                .open_at("wchan", OpenOptions::new().read(true))
                .await
        )?;
        file.read_to_string(&mut s).await?;
        Ok(s)
    }

    /// Return the `Status` for this process, based on the `/proc/[pid]/status` file.
    pub async fn status(&self) -> ProcResult<Status> {
        self.read_with_cursor("status").await
    }

    /// Returns the status info from `/proc/[pid]/stat`.
    pub async fn stat(&self) -> ProcResult<Stat> {
        self.read_with_cursor("stat").await
    }

    /// Return the limits for this process
    pub async fn limits(&self) -> ProcResult<Limits> {
        self.read_with_cursor("limits").await
    }

    /// Gets the process' login uid. May not be available.
    pub async fn loginuid(&self) -> ProcResult<u32> {
        let mut uid = String::new();
        let mut file = wrap_io_error!(
            self.path.join("loginuid"),
            self.fd
                .open_at("loginuid", OpenOptions::new().read(true))
                .await
        )?;
        file.read_to_string(&mut uid).await?;
        Status::parse_uid_gid(&uid, 0)
    }

    /// The current score that the kernel gives to this process for the purpose of selecting a
    /// process for the OOM-killer
    ///
    /// A higher score means that the process is more likely to be selected by the OOM-killer.
    /// The basis for this score is the amount of memory used by the process, plus other factors.
    ///
    /// Values range from 0 (never kill) to 1000 (always kill) inclusive.
    ///
    /// (Since linux 2.6.11)
    pub async fn oom_score(&self) -> ProcResult<u16> {
        let mut file = wrap_io_error!(
            self.path.join("oom_score"),
            self.fd
                .open_at("oom_score", OpenOptions::new().read(true))
                .await
        )?;
        let mut oom = String::new();
        file.read_to_string(&mut oom).await?;
        Ok(from_str!(u16, oom.trim()))
    }

    /// Adjust score value is added to the oom score before choosing processes to kill.
    ///
    /// Values range from -1000 (never kill) to 1000 (always kill) inclusive.
    pub async fn oom_score_adj(&self) -> ProcResult<i16> {
        let mut file = wrap_io_error!(
            self.path.join("oom_score_adj"),
            self.fd
                .open_at("oom_score_adj", OpenOptions::new().read(true))
                .await
        )?;
        let mut oom = String::new();
        file.read_to_string(&mut oom).await?;
        Ok(from_str!(i16, oom.trim()))
    }

    pub async fn set_oom_score_adj(&self, new_oom_score_adj: i16) -> ProcResult<()> {
        let mut file = wrap_io_error!(
            self.path.join("oom_score_adj"),
            self.fd
                .open_at("oom_score_adj", OpenOptions::new().write(true))
                .await
        )?;
        self.procfs
            .io()
            .write_value_to(&mut file, new_oom_score_adj)
            .await
    }

    /// Set process memory information
    ///
    /// Much of this data is the same as the data from `stat()` and `status()`
    pub async fn statm(&self) -> ProcResult<StatM> {
        self.read_with_cursor("statm").await
    }

    /// Return a task for the main thread of this process
    pub async fn task_main_thread(&self) -> ProcResult<Task> {
        self.task_from_tid(self.pid).await
    }

    /// Return a task for the thread based on a specified TID
    pub async fn task_from_tid(&self, tid: i32) -> ProcResult<Task> {
        Task::from_process_at(self, tid).await
    }

    /// Return the `Schedstat` for this process, based on the `/proc/<pid>/schedstat` file.
    ///
    /// (Requires CONFIG_SCHED_INFO)
    pub async fn schedstat(&self) -> ProcResult<Schedstat> {
        self.read_with_cursor("schedstat").await
    }

    /// Returns the status info from `/proc/[pid]/syscall`.
    pub async fn syscall(&self) -> ProcResult<Syscall> {
        self.read_with_cursor("syscall").await
    }

    /// Iterate over all the [`Task`]s (aka Threads) in this process
    ///
    /// Note that the iterator does not receive a snapshot of tasks, it is a
    /// lazy iterator over whatever happens to be running when the iterator
    /// gets there, see the examples below.
    pub async fn tasks(&self) -> ProcResult<impl Stream<Item = ProcResult<Task>> + Send> {
        let task_path = self.path.join("task");
        let dir_fd = wrap_io_error!(
            &task_path.to_owned(),
            self.fd
                .open_at(
                    "task",
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(OFlags::DIRECTORY.bits() as i32)
                )
                .await
        )?
        .try_into_std()
        .expect("should have no inflight operation");
        let dir_stream =
            wrap_io_error!(&task_path, async_hybrid_fs::Dir::new(dir_fd))?.into_stream();
        let stream = async_stream::stream! {
            for await entry in dir_stream {
                let tp = entry?;
                if let Ok(tid) = i32::from_str(&tp.file_name().to_string_lossy())
                    && let Ok(task) = Task::from_process_at(self, tid).await
                    {
                        yield Ok(task);
                    }
            }
        };
        Ok(stream)
    }

    /// Reads the tcp socket table from the process net namespace
    pub async fn tcp(&self) -> ProcResult<Vec<TcpNetEntry>> {
        self.read_si_with_cursor("net/tcp")
            .await
            .map(|net::TcpNetEntries(e)| e)
    }

    /// Reads the tcp6 socket table from the process net namespace
    pub async fn tcp6(&self) -> ProcResult<Vec<TcpNetEntry>> {
        self.read_si_with_cursor("net/tcp6")
            .await
            .map(|net::TcpNetEntries(e)| e)
    }

    /// Reads the udp socket table from the process net namespace
    pub async fn udp(&self) -> ProcResult<Vec<UdpNetEntry>> {
        self.read_si_with_cursor("net/udp")
            .await
            .map(|net::UdpNetEntries(e)| e)
    }

    /// Reads the udp6 socket table from the process net namespace
    pub async fn udp6(&self) -> ProcResult<Vec<UdpNetEntry>> {
        self.read_si_with_cursor("net/udp6")
            .await
            .map(|net::UdpNetEntries(e)| e)
    }

    /// Returns basic network device statistics for all interfaces in the process net namespace
    ///
    /// See also the [dev_status()](crate::net::dev_status()) function.
    pub async fn dev_status(&self) -> ProcResult<HashMap<String, net::DeviceStatus>> {
        self.read_with_cursor("net/dev")
            .await
            .map(|net::InterfaceDeviceStatus(e)| e)
    }

    /// Reads the unix socket table
    pub async fn unix(&self) -> ProcResult<Vec<net::UnixNetEntry>> {
        self.read_with_cursor("net/unix")
            .await
            .map(|net::UnixNetEntries(e)| e)
    }

    /// Reads the ARP table from the process net namespace
    pub async fn arp(&self) -> ProcResult<Vec<net::ARPEntry>> {
        self.read_with_cursor("net/arp")
            .await
            .map(|net::ArpEntries(e)| e)
    }

    /// Reads the ipv4 route table from the process net namespace
    pub async fn route(&self) -> ProcResult<Vec<net::RouteEntry>> {
        self.read_with_cursor("net/route")
            .await
            .map(|net::RouteEntries(e)| e)
    }

    /// Reads the network management information by Simple Network Management Protocol from the
    /// process net namespace
    pub async fn snmp(&self) -> ProcResult<net::Snmp> {
        self.read_with_cursor("net/snmp").await
    }

    /// Reads the network management information of IPv6 by Simple Network Management Protocol from
    /// the process net namespace
    pub async fn snmp6(&self) -> ProcResult<net::Snmp6> {
        self.read_with_cursor("net/snmp6").await
    }

    /// Opens a file to the process's memory (`/proc/<pid>/mem`).
    ///
    /// Note: you cannot start reading from the start of the file.  You must first seek to
    /// a mapped page.  See [Process::maps].
    ///
    /// Permission to access this file is governed by a ptrace access mode PTRACE_MODE_ATTACH_FSCREDS check
    pub async fn mem(&self) -> ProcResult<File> {
        let file = wrap_io_error!(
            self.path.join("mem"),
            self.fd.open_at("mem", OpenOptions::new().read(true)).await
        )?;
        Ok(file)
    }

    /// Returns a file which is part of the process proc structure
    pub async fn open_relative<P>(&self, path: P) -> ProcResult<File>
    where
        P: AsRef<Path>,
    {
        let file = wrap_io_error!(
            self.path.join(path.as_ref()),
            self.fd
                .open_at(path.as_ref(), OpenOptions::new().read(true))
                .await
        )?;
        Ok(file)
    }

    /// Returns a file which is part of the process proc structure
    pub async fn open_relative_flags<P>(&self, path: P, flags: OFlags) -> ProcResult<File>
    where
        P: AsRef<Path>,
    {
        let open_opts = OpenOptions::from_flag_and_mode(
            OFlag::from_bits_retain(flags.bits() as i32),
            Mode::empty(),
        );
        let file = wrap_io_error!(
            self.path.join(path.as_ref()),
            self.fd.open_at(path.as_ref(), &open_opts).await
        )?;
        Ok(file)
    }

    /// Clear reference bits
    ///
    /// See [ClearRefs] and [Process::pagemap()]
    pub async fn clear_refs(&self, clear: ClearRefs) -> ProcResult<()> {
        let mut file = wrap_io_error!(
            self.path.join("clear_refs"),
            self.fd
                .open_at("clear_refs", OpenOptions::new().write(true))
                .await
        )?;
        self.procfs.io().write_value_to(&mut file, clear).await
    }
}

/// Return a iterator of all processes
///
/// If a process can't be constructed for some reason, it will be returned as an `Err(ProcError)`
///
/// See also some important docs on the [`procfs::ProcessesIter`] struct.
pub async fn all_processes(
    procfs: &AsyncProcfs,
) -> ProcResult<impl Stream<Item = ProcResult<Process>> + Send> {
    let dir_clone = wrap_io_error!(
        procfs.inner.root.join("."),
        procfs
            .inner
            .file
            .open_at(
                ".",
                OpenOptions::new()
                    .read(true)
                    .custom_flags(OFlag::O_DIRECTORY.bits()),
            )
            .await
    )?
    .try_into_std()
    .expect("should have no inflight operation");
    let dir_clone = wrap_io_error!(
        procfs.inner.root.join("."),
        async_hybrid_fs::Dir::new(dir_clone)
    )?;
    let dir_stream = dir_clone.into_stream();
    let stream = async_stream::stream! {
        for await entry in dir_stream {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy();
            if let Ok(pid) = i32::from_str(&name) && let Ok(process) = Process::new(procfs, pid).await {
                yield Ok(process);
            }
        }
    };
    Ok(stream)
}
