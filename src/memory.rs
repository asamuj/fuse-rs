use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::{FileAttr, FileType, Filesystem, Request};
use libc::{c_int, ENOENT};
use log::debug;

const BLOCK_SIZE: u32 = 4096;
const FRSIZE: u32 = BLOCK_SIZE;

/// A simple in-memory filesystem
pub struct MemoryFS {
    max_size: u64,
    inodes_num: u64,
    inodes: HashMap<u64, FileAttr>,   // ino -> FileAttr
    name_inode: HashMap<String, u64>, //  name -> ino
    data: HashMap<u64, Vec<u8>>,      // ino -> data
}

impl Filesystem for MemoryFS {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        let root_file = FileAttr {
            ino: 1,
            kind: FileType::Directory,
            perm: 0o755,
            ..Default::default()
        };

        self.inodes.insert(1, root_file);
        self.name_inode.insert(String::from("."), 1);

        let parent_file = FileAttr {
            ino: 2,
            kind: FileType::Directory,
            perm: 0o755,
            ..Default::default()
        };

        self.inodes.insert(2, parent_file);
        self.name_inode.insert(String::from(".."), 2);

        self.inodes_num = 2;
        Ok(())
    }

    /*
        pub blocks: u64,  // Total blocks (in units of frsize)
        pub bfree: u64,   // Free blocks
        pub bavail: u64,  // Free blocks for unprivileged users
        pub files: u64,   // Total inodes
        pub ffree: u64,   // Free inodes
        pub bsize: u32,   // Filesystem block size
        pub namelen: u32, // Maximum filename length
        pub frsize: u32,  // Fundamental file system block size
    */
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: crate::ReplyStatfs) {
        let blocks = self.max_size / FRSIZE as u64;
        let bfree = blocks;
        let bavail = blocks;
        let files = self.inodes_num;

        reply.statfs(blocks, bfree, bavail, files, 1000, BLOCK_SIZE, 255, FRSIZE)
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: crate::ReplyAttr) {
        if let Some(file_attr) = self.inodes.get(&ino) {
            reply.attr(&Duration::new(1, 0), file_attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: crate::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: crate::ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        for (i, (name, ino)) in self.name_inode.iter().skip(offset as usize).enumerate() {
            if let Some(fiel_attr) = self.inodes.get(ino) {
                reply.add(*ino, (i + 1) as i64, fiel_attr.kind, name);
            } else {
                reply.error(ENOENT);
                return;
            }
        }

        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        reply: crate::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        name: &std::ffi::OsStr,
        reply: crate::ReplyEntry,
    ) {
        if let Some(file_attr) = self.name_inode.get(name.to_str().unwrap()) {
            if let Some(file_attr) = self.inodes.get(file_attr) {
                reply.entry(&Duration::new(1, 0), file_attr, 0);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: crate::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        reply: crate::ReplyData,
    ) {
        if let Some(data) = self.data.get(&ino) {
            reply.data(&data[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: crate::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _flags: u32,
        reply: crate::ReplyCreate,
    ) {
        let ino = self.inodes_num + 1;
        let file_attr = FileAttr {
            ino,
            kind: FileType::RegularFile,
            perm: 0o755,
            ..Default::default()
        };

        self.inodes.insert(ino, file_attr);
        self.name_inode
            .insert(name.to_str().unwrap().to_string(), ino);
        self.inodes_num += 1;
        self.data.insert(ino, Vec::new());
        reply.created(&Duration::new(1, 0), &file_attr, 0, 0, 0);
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: crate::ReplyAttr,
    ) {
        if let Some(file_attr) = self.inodes.get_mut(&ino) {
            if let Some(mode) = mode {
                file_attr.perm = mode as u16;
            }
            if let Some(uid) = uid {
                file_attr.uid = uid;
            }
            if let Some(gid) = gid {
                file_attr.gid = gid;
            }
            if let Some(size) = size {
                file_attr.size = size;
            }
            if let Some(atime) = atime {
                file_attr.atime = atime;
            }
            if let Some(mtime) = mtime {
                file_attr.mtime = mtime;
            }
            if let Some(crtime) = crtime {
                file_attr.crtime = crtime;
            }

            if let Some(flags) = flags {
                file_attr.flags = flags;
            }
            reply.attr(&Duration::new(1, 0), file_attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: crate::ReplyWrite,
    ) {
        debug!(
            "write ino: {}, offset: {}, size: {}",
            ino,
            offset,
            data.len()
        );

        if let Some(file_data) = self.data.get_mut(&ino) {
            if offset as usize > file_data.len() {
                reply.error(ENOENT);
                return;
            }
            let end = offset as usize + data.len();
            if end > file_data.len() {
                file_data.resize(end, 0);
            }
            file_data[offset as usize..end].copy_from_slice(data);
            if let Some(file_attr) = self.inodes.get_mut(&ino) {
                file_attr.size += data.len() as u64;
            }
            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
        }
    }
}

/// Create a new in-memory filesystem
pub fn new(max_size: u64) -> MemoryFS {
    MemoryFS {
        max_size,
        inodes_num: 0,
        inodes: HashMap::new(),
        name_inode: HashMap::new(),
        data: HashMap::new(),
    }
}
