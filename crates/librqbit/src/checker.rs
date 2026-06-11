//! Standalone, read-only verification of torrent data on disk.
//!
//! Unlike the init-time check that runs when a torrent is added to a session,
//! this opens existing files read-only, never creates or resizes anything,
//! and reports per-file and per-piece results.

use std::{
    fs::File,
    io::IoSlice,
    ops::Range,
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use buffers::ByteBufOwned;
use librqbit_core::{lengths::ValidPieceIndex, torrent_metainfo::ValidatedTorrentMetaV1Info};
use tracing::debug;

use crate::{
    file_info::FileInfo,
    file_ops::FileOps,
    storage::{TorrentStorage, filesystem::OurFileExt},
    torrent_state::{ManagedTorrentShared, TorrentMetadata},
    type_aliases::FileInfos,
};

#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("error verifying torrent data: {0:#}")]
    Verify(#[source] anyhow::Error),
}

/// Per-file outcome of a [`check_torrent`] run.
#[derive(Debug)]
pub struct FileCheckResult {
    pub file_index: usize,
    pub relative_filename: PathBuf,
    pub len: u64,
    /// Bytes of this file covered by hash-verified pieces.
    pub have_bytes: u64,
    pub piece_range: Range<u32>,
    /// Verified pieces overlapping this file (a piece spanning two files
    /// counts for both).
    pub pieces_have: u32,
    pub padding: bool,
    /// False if the file could not be opened (e.g. doesn't exist).
    pub found_on_disk: bool,
}

impl FileCheckResult {
    pub fn total_pieces(&self) -> u32 {
        self.piece_range.end - self.piece_range.start
    }

    pub fn progress_percent(&self) -> f64 {
        if self.len == 0 {
            100.0
        } else {
            self.have_bytes as f64 * 100.0 / self.len as f64
        }
    }
}

/// Outcome of a [`check_torrent`] run.
#[derive(Debug)]
pub struct CheckResult {
    pub total_bytes: u64,
    /// Bytes covered by hash-verified pieces.
    pub have_bytes: u64,
    pub total_pieces: u32,
    pub have_piece_count: u32,
    /// Per-piece verification status, indexed by piece id.
    pub have_pieces: Vec<bool>,
    pub files: Vec<FileCheckResult>,
}

impl CheckResult {
    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            100.0
        } else {
            self.have_bytes as f64 * 100.0 / self.total_bytes as f64
        }
    }
}

/// Read-only storage over whatever already exists on disk. Files that are
/// missing (or shorter than expected) produce read errors, which
/// initial_check() treats as "pieces not present".
struct ReadOnlyFilesystemStorage {
    // None = padding file (never read) or file that could not be opened.
    files: Vec<Option<File>>,
}

impl TorrentStorage for ReadOnlyFilesystemStorage {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        // Files are opened in check_torrent(); this storage never goes
        // through a storage factory.
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.files
            .get(file_id)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| anyhow::anyhow!("file {file_id} not present on disk"))?
            .pread_exact(offset, buf)
    }

    fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("read-only storage")
    }

    fn pwrite_all_vectored(
        &self,
        _file_id: usize,
        _offset: u64,
        _bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        anyhow::bail!("read-only storage")
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        anyhow::bail!("read-only storage")
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        anyhow::bail!("read-only storage")
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        anyhow::bail!("read-only storage")
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        anyhow::bail!("read-only storage")
    }

    fn on_piece_completed(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        anyhow::bail!("read-only storage")
    }
}

/// Verify the data in output_folder against the torrent's piece hashes.
///
/// Read-only: existing files are opened without write access, missing files
/// are not created, and nothing is resized. This is CPU/IO-heavy (it hashes
/// every piece that's readable) - call from a blocking context.
///
/// `progress_bytes` is incremented as pieces are processed, up to the
/// torrent's total length - poll it from another thread to report progress.
pub fn check_torrent(
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    output_folder: &Path,
    progress_bytes: &AtomicU64,
) -> Result<CheckResult, CheckError> {
    let file_infos: FileInfos = info
        .iter_file_details_ext()
        .map(|fd| FileInfo {
            relative_filename: fd.details.filename.to_pathbuf(),
            offset_in_torrent: fd.offset,
            piece_range: fd.pieces,
            len: fd.details.len,
            attrs: fd.details.attrs(),
        })
        .collect();

    let mut files = Vec::with_capacity(file_infos.len());
    for fi in &file_infos {
        if fi.attrs.padding {
            files.push(None);
            continue;
        }
        let full_path = output_folder.join(&fi.relative_filename);
        match File::open(&full_path) {
            Ok(f) => files.push(Some(f)),
            Err(e) => {
                debug!("could not open {full_path:?} for checking: {e:#}");
                files.push(None);
            }
        }
    }
    let found_on_disk: Vec<bool> = file_infos
        .iter()
        .zip(files.iter())
        .map(|(fi, f)| fi.attrs.padding || f.is_some())
        .collect();

    let storage = ReadOnlyFilesystemStorage { files };
    let have_pieces = FileOps::new(info, &storage, &file_infos)
        .initial_check(progress_bytes)
        .map_err(CheckError::Verify)?;

    let lengths = info.lengths();
    let mut have_bytes = 0u64;
    let mut have_piece_count = 0u32;
    for piece_info in lengths.iter_piece_infos() {
        if have_pieces[piece_info.piece_index.get() as usize] {
            have_bytes += piece_info.len as u64;
            have_piece_count += 1;
        }
    }

    let files = file_infos
        .iter()
        .enumerate()
        .map(|(idx, fi)| {
            let mut file_have_bytes = 0u64;
            let mut pieces_have = 0u32;
            for piece_id in fi.piece_range.clone() {
                if have_pieces[piece_id as usize] {
                    pieces_have += 1;
                    file_have_bytes +=
                        lengths.size_of_piece_in_file(piece_id, fi.offset_in_torrent, fi.len);
                }
            }
            FileCheckResult {
                file_index: idx,
                relative_filename: fi.relative_filename.clone(),
                len: fi.len,
                have_bytes: file_have_bytes,
                piece_range: fi.piece_range.clone(),
                pieces_have,
                padding: fi.attrs.padding,
                found_on_disk: found_on_disk[idx],
            }
        })
        .collect();

    Ok(CheckResult {
        total_bytes: lengths.total_length(),
        have_bytes,
        total_pieces: lengths.total_pieces(),
        have_piece_count,
        // The bitfield is byte-padded; cut it down to the real piece count.
        have_pieces: have_pieces
            .iter()
            .map(|b| *b)
            .take(lengths.total_pieces() as usize)
            .collect(),
        files,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{
        create_torrent, create_torrent_file::CreateTorrentOptions, spawn_utils::BlockingSpawner,
        tests::test_util::create_default_random_dir_with_torrents,
    };

    #[tokio::test]
    async fn test_check_torrent() {
        const FILE_LEN: usize = 1000 * 1000;
        let dir = create_default_random_dir_with_torrents(3, FILE_LEN, Some("rqbit_check_test"));
        let torrent = create_torrent(
            dir.path(),
            CreateTorrentOptions {
                // Small pieces that don't divide the file length evenly, so
                // some pieces span two files.
                piece_length: Some(32768),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await
        .unwrap();
        let info = torrent.meta.info.data.clone().validate().unwrap();

        // All data present: everything verifies.
        let progress = AtomicU64::new(0);
        let r = super::check_torrent(&info, dir.path(), &progress).unwrap();
        assert_eq!(r.have_bytes, r.total_bytes);
        assert_eq!(r.have_piece_count, r.total_pieces);
        assert!(r.have_pieces.iter().all(|p| *p));
        assert_eq!(r.files.len(), 3);
        for f in &r.files {
            assert!(f.found_on_disk);
            assert_eq!(f.have_bytes, f.len);
            assert_eq!(f.pieces_have, f.total_pieces());
        }
        assert_eq!(progress.load(Ordering::Relaxed), r.total_bytes);
        assert_eq!(
            r.files.iter().map(|f| f.have_bytes).sum::<u64>(),
            // Pieces spanning two files count for both, but byte counts don't double.
            r.total_bytes
        );

        // Corrupt one byte in the middle of the second file: exactly one piece
        // fails verification.
        let corrupt_path = dir.path().join("1.data");
        {
            use std::{fs::OpenOptions, io::*};
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&corrupt_path)
                .unwrap();
            f.seek(SeekFrom::Start(FILE_LEN as u64 / 2)).unwrap();
            let mut b = [0u8];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(FILE_LEN as u64 / 2)).unwrap();
            f.write_all(&[!b[0]]).unwrap();
        }
        let r = super::check_torrent(&info, dir.path(), &AtomicU64::new(0)).unwrap();
        assert_eq!(r.have_piece_count, r.total_pieces - 1);
        assert!(r.have_bytes < r.total_bytes);
        assert_eq!(r.files[0].have_bytes, r.files[0].len);
        assert!(r.files[1].have_bytes < r.files[1].len);
        assert_eq!(r.files[2].have_bytes, r.files[2].len);

        // Delete the second file entirely: all its pieces are missing, the
        // other files only lose pieces they share with it.
        std::fs::remove_file(&corrupt_path).unwrap();
        let r = super::check_torrent(&info, dir.path(), &AtomicU64::new(0)).unwrap();
        assert!(r.files[0].found_on_disk);
        assert!(!r.files[1].found_on_disk);
        assert!(r.files[2].found_on_disk);
        assert_eq!(r.files[1].pieces_have, 0);
        assert_eq!(r.files[1].have_bytes, 0);
        assert!(r.files[0].have_bytes > 0);
        assert!(r.files[2].have_bytes > 0);
        // The deleted file must not have been re-created by the check.
        assert!(!corrupt_path.exists());
    }
}
