use crate::AsyncProcfs;
pub mod kernel;
use kernel::AsyncProcfsSysKernel;

pub struct AsyncProcfsSys<'a> {
    inner: &'a AsyncProcfs,
}

impl<'a> AsyncProcfsSys<'a> {
    pub(crate) fn new(procfs: &'a AsyncProcfs) -> Self {
        Self { inner: procfs }
    }

    pub fn kernel(&self) -> AsyncProcfsSysKernel<'_> {
        AsyncProcfsSysKernel::new(self.inner)
    }
}
