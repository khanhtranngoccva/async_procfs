use crate::AsyncProcfs;
use crate::{AsyncCurrent, AsyncCurrentSI, runtime};
#[cfg(feature = "chrono")]
use chrono::DateTime;
use procfs_core::{KernelStats, LoadAverage, ProcResult, SystemInfoInterface};

/// A type for accessing data about the currently running machine asynchronously.
///
/// For more details, see the [SystemInfoInterface] trait.
pub struct AsyncLocalSystemInfo {
    inner: AsyncProcfs,
}

#[async_trait::async_trait]
pub trait AsyncSystemInfoInterface: SystemInfoInterface {
    async fn async_boot_time_secs(&self) -> ProcResult<u64>;
}

impl SystemInfoInterface for AsyncLocalSystemInfo {
    fn boot_time_secs(&self) -> ProcResult<u64> {
        runtime::execute_future_from_sync(async { self.async_boot_time_secs().await })
    }

    fn ticks_per_second(&self) -> u64 {
        ticks_per_second()
    }

    fn page_size(&self) -> u64 {
        page_size()
    }

    fn is_little_endian(&self) -> bool {
        u16::from_ne_bytes([0, 1]).to_le_bytes() == [0, 1]
    }
}

#[async_trait::async_trait]
impl AsyncSystemInfoInterface for AsyncLocalSystemInfo {
    async fn async_boot_time_secs(&self) -> ProcResult<u64> {
        boot_time_secs(&self.inner).await
    }
}

pub fn current_system_info(procfs: &AsyncProcfs) -> AsyncLocalSystemInfo {
    AsyncLocalSystemInfo {
        inner: procfs.clone(),
    }
}

/// Auxiliary system information.
pub type AsyncSystemInfo = dyn AsyncSystemInfoInterface + Sync;

/// Return the number of ticks per second.
///
/// This isn't part of the proc file system, but it's a useful thing to have, since several fields
/// count in ticks.  This is calculated from `sysconf(_SC_CLK_TCK)`.
pub fn ticks_per_second() -> u64 {
    rustix::param::clock_ticks_per_second()
}

/// The boot time of the system, as a `DateTime` object.
///
/// This is calculated from `/proc/stat`.
///
/// This function requires the "chrono" features to be enabled (which it is by default).
#[cfg(feature = "chrono")]
pub async fn boot_time(procfs: &AsyncProcfs) -> ProcResult<DateTime<chrono::Local>> {
    use chrono::TimeZone;
    use procfs_core::expect;
    let secs = boot_time_secs(procfs).await?;

    let date_time = expect!(chrono::Local.timestamp_opt(secs as i64, 0).single());

    Ok(date_time)
}

/// The boottime of the system, in seconds since the epoch
///
/// This is calculated from `/proc/stat`.
///
#[cfg_attr(
    not(feature = "chrono"),
    doc = "If you compile with the optional `chrono` feature, you can use the `boot_time()` method to get the boot time as a `DateTime` object."
)]
#[cfg_attr(
    feature = "chrono",
    doc = "See also [boot_time()] to get the boot time as a `DateTime`"
)]
pub async fn boot_time_secs(procfs: &AsyncProcfs) -> ProcResult<u64> {
    let stat = KernelStats::current(procfs).await?;
    Ok(stat.btime)
}

/// Memory page size, in bytes.
///
/// This is calculated from `sysconf(_SC_PAGESIZE)`.
pub fn page_size() -> u64 {
    rustix::param::page_size() as u64
}

impl AsyncCurrentSI for KernelStats {
    const RELATIVE_PATH: &'static str = "stat";
}

impl AsyncCurrent for LoadAverage {
    const RELATIVE_PATH: &'static str = "loadavg";
}
