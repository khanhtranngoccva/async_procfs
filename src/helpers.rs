use crate::AsyncProcfs;
use async_hybrid_fs::{HybridDir, HybridRead, HybridWrite, OpenOptions};
use procfs_core::{ProcError, ProcErrorExt, ProcResult};
use std::{fmt, path::Path, str::FromStr};

pub struct AsyncProcfsIo<'a> {
    inner: &'a AsyncProcfs,
}

impl<'a> AsyncProcfsIo<'a> {
    pub fn new(procfs: &'a AsyncProcfs) -> Self {
        Self { inner: procfs }
    }

    pub async fn read_file<P: AsRef<Path>>(&self, path: P) -> ProcResult<String> {
        let mut file = self
            .inner
            .inner
            .file
            .open_at(path.as_ref(), OpenOptions::new().read(true))
            .await
            .map_err(|e| e.into())
            .error_path(&self.inner.inner.root.join(path.as_ref()))?;
        let mut string = String::new();
        file.hybrid_read_to_string(&mut string)
            .await
            .map_err(|e| e.into())
            .error_path(&self.inner.inner.root.join(path.as_ref()))?;
        Ok(string)
    }

    pub async fn write_file<P: AsRef<Path>>(
        &self,
        path: P,
        data: impl AsRef<[u8]>,
    ) -> ProcResult<()> {
        let mut file = self
            .inner
            .inner
            .file
            .open_at(
                path.as_ref(),
                OpenOptions::new().write(true).create(true).truncate(true),
            )
            .await
            .map_err(|e| e.into())
            .error_path(&self.inner.inner.root.join(path.as_ref()))?;
        file.write_all(data.as_ref())
            .await
            .map_err(|e| e.into())
            .error_path(&self.inner.inner.root.join(path.as_ref()))?;
        Ok(())
    }

    pub async fn read_value<P, T, E>(&self, path: P) -> ProcResult<T>
    where
        P: AsRef<Path>,
        T: FromStr<Err = E>,
        ProcError: From<E>,
    {
        self.read_file(path)
            .await
            .and_then(|s| s.trim().parse().map_err(ProcError::from))
    }

    #[allow(unused)]
    pub async fn read_value_from<F, T, E>(&self, file: &mut F) -> ProcResult<T>
    where
        F: HybridRead,
        T: FromStr<Err = E>,
        ProcError: From<E>,
    {
        let mut string = String::new();
        file.hybrid_read_to_string(&mut string).await?;
        string.trim().parse().map_err(ProcError::from)
    }

    pub async fn write_value<P, T>(&self, path: P, value: T) -> ProcResult<()>
    where
        P: AsRef<Path>,
        T: fmt::Display,
    {
        self.write_file(path, value.to_string().as_bytes()).await
    }

    pub async fn write_value_to<F, T>(&self, file: &mut F, value: T) -> ProcResult<()>
    where
        F: HybridWrite,
        T: fmt::Display,
    {
        Ok(file.write_all(value.to_string().as_bytes()).await?)
    }
}

#[doc(hidden)]
#[allow(unused)]
pub trait IntoOption<T> {
    fn into_option(t: Self) -> Option<T>;
}

impl<T> IntoOption<T> for Option<T> {
    fn into_option(t: Option<T>) -> Option<T> {
        t
    }
}

impl<T, R> IntoOption<T> for Result<T, R> {
    fn into_option(t: Result<T, R>) -> Option<T> {
        t.ok()
    }
}

#[doc(hidden)]
#[allow(unused)]
pub trait IntoResult<T, E> {
    fn into(t: Self) -> Result<T, E>;
}

macro_rules! wrap_io_error {
    ($path:expr, $expr:expr) => {
        match $expr {
            Ok(v) => Ok(v),
            Err(e) => {
                let kind = e.kind();
                Err(::std::io::Error::new(
                    kind,
                    crate::IoErrorWrapper {
                        path: ::std::path::PathBuf::from($path),
                        inner: e.into(),
                    },
                ))
            }
        }
    };
}

pub(crate) use wrap_io_error;
