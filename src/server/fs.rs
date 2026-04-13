use rayon::prelude::*;
use serde::{Deserialize, Serialize};
// use serde_binary::{Deserialize as DeserializeBinary, Serialize as SerializeBinary};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

#[derive(Serialize, Deserialize, Debug)]
enum SpecialField {
    Dir { children: Vec<usize> },
    File { size: usize },
}

#[derive(Serialize, Deserialize, Debug)]
struct FileOrDir {
    path: PathBuf,
    md5: String,
    special_fields: SpecialField,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum LookupDirOrFile {
    Dir {
        name: String,
        children: Vec<LookupDirOrFile>,
    },
    File {
        name: String,
        md5: String,
        size: usize,
    },
}

impl PartialEq for LookupDirOrFile {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                LookupDirOrFile::Dir {
                    name: name1,
                    children: children1,
                },
                LookupDirOrFile::Dir {
                    name: name2,
                    children: children2,
                },
            ) => name1 == name2 && children1 == children2,
            (
                LookupDirOrFile::File {
                    name: name1,
                    md5: md51,
                    size: size1,
                },
                LookupDirOrFile::File {
                    name: name2,
                    md5: md52,
                    size: size2,
                },
            ) => name1 == name2 && md51 == md52 && size1 == size2,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct VirtualFileSystem {
    items: Vec<FileOrDir>,
    md5_to_id: HashMap<String, usize>,
}

fn resolve_symlink(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_symlink() {
        std::fs::read_link(path)
    } else {
        Ok(path)
    }
}

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB buffer for better IO throughput on large files
const MAX_IO_PARALLELISM: usize = 16; // Limit parallel IO to avoid cephfs bandwidth contention

impl VirtualFileSystem {
    pub fn new() -> Self {
        VirtualFileSystem {
            items: Vec::new(),
            md5_to_id: HashMap::new(),
        }
    }

    pub fn lookup(&self, md5: &str) -> Option<LookupDirOrFile> {
        self.md5_to_id.get(md5).map(|id| self.id_to_lookup(*id))
    }

    pub fn file_path(&self, md5: &str) -> Result<PathBuf, io::Error> {
        match self.md5_to_id.get(md5) {
            Some(id) => match &self.items[*id].special_fields {
                SpecialField::File { .. } => Ok(self.items[*id].path.clone()),
                _ => Err(io::Error::new(io::ErrorKind::InvalidFilename, "Is Dir")),
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File not found")),
        }
    }

    fn file_name(&self, id: usize) -> String {
        self.items[id]
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    fn id_to_lookup(&self, id: usize) -> LookupDirOrFile {
        match &self.items[id].special_fields {
            SpecialField::Dir { children } => {
                let dir_children = children
                    .iter()
                    .map(|child_id| self.id_to_lookup(*child_id))
                    .collect::<Vec<_>>();

                LookupDirOrFile::Dir {
                    name: self.file_name(id),
                    children: dir_children,
                }
            }
            SpecialField::File { size } => LookupDirOrFile::File {
                name: self.file_name(id),
                md5: self.items[id].md5.clone(),
                size: size.clone(),
            },
        }
    }

    pub fn seal(&mut self) -> io::Result<()> {
        self.seal_with_options(false)
    }

    pub fn seal_with_options(&mut self, no_checksum: bool) -> io::Result<()> {
        if !self.md5_to_id.is_empty() {
            return Result::Err(io::Error::new(
                io::ErrorKind::Other,
                "VirtualFileSystem has been sealed",
            ));
        }

        // Use a custom thread pool with limited parallelism to avoid IO contention on cephfs
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(MAX_IO_PARALLELISM)
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        pool.install(|| {
            (&mut self.items)
                .into_par_iter()
                .try_for_each(|item| -> io::Result<()> {
                match &item.special_fields {
                    SpecialField::Dir { .. } => Ok(()),
                    SpecialField::File { .. } => {
                        let file_size = fs::metadata(item.path.as_path())?.size();

                        if no_checksum {
                            // Use full path string as the key directly (no file IO needed)
                            item.md5 = item.path.to_string_lossy().to_string();
                        } else {
                            let mut buffer = vec![0u8; BUFFER_SIZE];
                            let mut file = std::fs::File::open(item.path.as_path())?;
                            let begin = SystemTime::now();
                            let mut ctx = md5::Context::new();
                            loop {
                                let n = file.read(&mut buffer)?;
                                if n == 0 {
                                    break;
                                }
                                ctx.consume(&buffer[..n]);
                            }
                            let end = SystemTime::now();
                            let duration = end.duration_since(begin).unwrap();
                            if duration > Duration::from_millis(100) {
                                println!(
                                    "file {:?} read took {:.2} seconds",
                                    item.path.file_name().unwrap(),
                                    duration.as_millis() as f64 / 1000.0
                                );
                            }
                            item.md5 = format!("{:x}", ctx.compute());
                        }

                        item.special_fields = SpecialField::File {
                            size: file_size as usize,
                        };
                        Ok(())
                    }
                }
            })
        })?;

        for index in 0..self.items.len() {
            if let Some((new_children, new_md5)) = {
                let item = &self.items[index];

                if let SpecialField::Dir { children } = &item.special_fields {
                    let mut mut_children = children.clone();
                    mut_children.sort_by(|a, b| self.items[*a].md5.cmp(&self.items[*b].md5));

                    let dir_md5 = if no_checksum {
                        // Use full path string as the key for dirs too
                        item.path.to_string_lossy().to_string()
                    } else {
                        let mut md5_ctx = md5::Context::new();
                        for child in mut_children.iter() {
                            md5_ctx.consume(&self.items[*child].md5);
                        }
                        format!("{:x}", md5_ctx.compute())
                    };

                    Some((mut_children, dir_md5))
                } else {
                    None
                }
            } {
                let item = &mut self.items[index];

                if let SpecialField::Dir { children } = &mut item.special_fields {
                    *children = new_children;
                    item.md5 = new_md5;
                }
            }

            self.md5_to_id.insert(self.items[index].md5.clone(), index);
        }

        Ok(())
    }

    pub fn add(&mut self, path: PathBuf) -> io::Result<usize> {
        let path = resolve_symlink(path)?;
        if path.is_dir() {
            self.add_dir(path)
        } else {
            self.add_file(path)
        }
    }

    fn add_dir(&mut self, path: PathBuf) -> io::Result<usize> {
        // must be dir, since add has is_dir
        let mut children = Vec::new();

        for entry in std::fs::read_dir(path.as_path())? {
            let entry = entry?;
            // Skip hidden files/directories (names starting with '.')
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with('.') {
                    continue;
                }
            }
            children.push(self.add(entry.path())?);
        }

        self.items.push(FileOrDir {
            path: path,
            md5: String::new(),
            special_fields: SpecialField::Dir { children: children },
        });

        Ok(self.items.len() - 1)
    }

    fn add_file(&mut self, path: PathBuf) -> io::Result<usize> {
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path is not a file",
            ));
        }

        let id = self.items.len();

        self.items.push(FileOrDir {
            path: path,
            md5: String::new(),
            special_fields: SpecialField::File { size: 0 },
        });

        Ok(id)
    }

    pub fn dump_md5<W: Write>(&self, mut w: W) -> io::Result<()> {
        for item in self.items.iter() {
            match &item.special_fields {
                SpecialField::Dir { .. } => {
                    write!(w, "dir {}: md5 {}\n", item.path.display(), item.md5)?;
                }
                SpecialField::File { size } => {
                    write!(
                        w,
                        "file {}: md5 {} size {}\n",
                        item.path.display(),
                        item.md5,
                        size
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Write a manifest file with tab-separated format: "<full_path>\t<key>\t<size_in_bytes>" per line.
    /// Only includes leaf files (not directories). The key is the md5/path-based identifier.
    /// Tab separator is used to safely handle filenames containing spaces.
    pub fn write_manifest<W: Write>(&self, mut w: W) -> io::Result<()> {
        for item in self.items.iter() {
            if let SpecialField::File { size } = &item.special_fields {
                // Use full path for both display and identification
                let full_path = item.path.to_string_lossy();
                write!(w, "{}\t{}\t{}\n", full_path, item.md5, size)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::VirtualFileSystem;
    use std::io;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_data() -> io::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "Hello, world")?;
        let mut file_data = VirtualFileSystem::new();
        let file_id = file_data.add_file(temp_file.path().to_path_buf())?;
        assert!(file_id == 0);
        Ok(())
    }
}
