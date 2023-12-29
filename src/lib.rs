use std::{
    fmt::Debug,
    io::SeekFrom::*,
    path::{Path, PathBuf},
};

use tokio::{
    fs::File,
    io,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[derive(Debug)]
pub struct Metadata {
    pub hash:   String,
    pub size:   u64,
    pub offset: u64,
    pub len:    u64,
}

impl Metadata {
    pub fn new(hash: impl Into<String>, size: u64) -> Self {
        let hash = hash.into();
        let len = size + 40 + hash.len() as u64;
        Self { hash, size, offset: 0, len }
    }
    pub async fn from_file(file: &mut File) -> io::Result<Self> {
        let len = file.metadata().await?.len();
        if len < 40 {
            return Err(io::Error::other("文件不包含元数据"));
        }

        file.seek(End(-40)).await?;
        let mut buf = [0; 40];
        file.read_exact(&mut buf).await?;
        let error = |_| io::Error::other("解析 downloading 元数据失败");
        let size: u64 = String::from_utf8_lossy(&buf[..20]).parse().map_err(error)?;
        let offset: u64 = String::from_utf8_lossy(&buf[20..]).parse().map_err(error)?;

        let mut buf = vec![0; (len - size - 40) as usize];
        file.seek(Start(size)).await?;
        file.read_exact(&mut buf).await?;
        let hash = String::from_utf8_lossy(&buf).to_string();
        Ok(Self { hash, size, offset, len })
    }

    pub async fn update(&self, file: &mut File) -> io::Result<()> {
        let meta = format!("{}{:020}{:020}", self.hash, self.size, self.offset);
        file.set_len(self.len).await?;
        file.seek(Start(self.size)).await?;
        file.write_all(meta.as_bytes()).await
    }

    /// hash 和 size 一致保留下载进度 否则重置下载进度并更新
    pub fn amend(mut self, hash: &str, size: u64) -> Self {
        if self.hash != hash && self.size != size {
            self.offset = 0;
            self.size = size;
            self.hash.truncate(0);
            self.hash.push_str(hash);
            self.len = self.size + 40 + self.hash.len() as u64;
        }
        self
    }
}

#[derive(Debug)]
pub struct Downloading {
    path: PathBuf,
    file: File,
    meta: Metadata,
}

impl Downloading {
    /// downloading 文件不存在创建并写入元数据
    ///
    /// 存在读取元数据 存在但信息不一致覆盖原来下载进度
    pub async fn new<P, H>(path: P, hash: H, size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
        H: Into<String>,
    {
        if path.as_ref().exists() {
            return Err(io::Error::other("要下载的文件已存在"));
        }

        let ext = path.as_ref().extension().unwrap_or_default();
        let ext = format!("{}.downloading", ext.to_string_lossy());
        let path = path.as_ref().with_extension(ext);
        let mut file = File::options().create(true).write(true).read(true).open(&path).await?;
        let len = file.metadata().await?.len();
        let hash = hash.into();

        let meta = if len < 40 {
            Metadata::new(hash, size)
        } else {
            Metadata::from_file(&mut file).await?.amend(&hash, size)
        };
        meta.update(&mut file).await?;

        Ok(Self { path: path.to_path_buf(), file, meta })
    }

    /// 写入成功后返回当前位置 Some(offset)
    ///
    /// 完整写入后返回 None
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<Option<u64>> {
        let offset = self.meta.offset + buf.len() as u64;
        if offset > self.meta.size {
            return Err(io::Error::other("写入的文本长度超过文件长度"));
        }

        self.file.seek(Start(self.meta.offset)).await?;
        self.file.write_all(buf).await?;
        self.file.seek(End(-20)).await?;
        self.file.write_all(format!("{:020}", offset).as_bytes()).await?;
        self.meta.offset = offset;

        if offset != self.meta.size {
            Ok(Some(offset))
        } else {
            Ok(None)
        }
    }

    /// 完成下载
    pub async fn complete(mut self, verify: impl Fn(&mut File) -> String) -> io::Result<()> {
        if self.meta.offset != self.meta.size {
            return Err(io::Error::other("文件还未下载完成"));
        }
        self.file.seek(Start(0)).await?;
        self.file.set_len(self.meta.size).await?;

        if verify(&mut self.file) != self.meta.hash {
            self.meta.update(&mut self.file).await?;
            return Err(io::Error::other("文件检验失败"));
        }

        tokio::fs::rename(&self.path, self.path.with_extension("")).await?;
        Ok(())
    }

    /// 查看元数据
    pub fn meta(&self) -> &Metadata {
        &self.meta
    }
}
