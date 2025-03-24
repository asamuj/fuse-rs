use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::{FileAttr, FileType, Filesystem};
use libc::{c_int, ENOENT};
use log::debug;

const BLOCK_SIZE: u32 = 4096;
const FRSIZE: u32 = BLOCK_SIZE;

/// A simple in-memory filesystem
pub struct MemoryFS {
    max_size: u64,
    inodes_num: u64,
    inodes: HashMap<u64, (String, FileAttr)>, // ino -> FileAttr
    // name_inode: HashMap<String, u64>, //  name -> ino
    data: HashMap<u64, Vec<u8>>, // ino -> data
    // Use a HashMap to represent parent and children relationships
    // parent_ino -> Vec<child_ino>
    parent_children: HashMap<u64, Vec<u64>>,
}

impl Filesystem for MemoryFS {
    fn init(&mut self) -> Result<(), c_int> {
        let root_file = FileAttr {
            ino: 1,
            kind: FileType::Directory,
            ..Default::default()
        };

        self.inodes.insert(1, (".".to_string(), root_file));

        let parent_file = FileAttr {
            ino: 2,
            kind: FileType::Directory,
            ..Default::default()
        };

        self.inodes.insert(2, ("..".to_string(), parent_file));
        self.parent_children.insert(1, vec![]);

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
    fn statfs(&mut self, _ino: u64, reply: crate::ReplyStatfs) {
        let blocks = self.max_size / FRSIZE as u64;
        let bfree = blocks;
        let bavail = blocks;
        let files = self.inodes_num;

        reply.statfs(blocks, bfree, bavail, files, 1000, BLOCK_SIZE, 255, FRSIZE)
    }

    fn getattr(&mut self, ino: u64, reply: crate::ReplyAttr) {
        if let Some((_, file_attr)) = self.inodes.get(&ino) {
            reply.attr(&Duration::new(1, 0), file_attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn opendir(&mut self, _ino: u64, _flags: u32, reply: crate::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(&mut self, ino: u64, _fh: u64, offset: i64, mut reply: crate::ReplyDirectory) {
        if let Some(children) = self.parent_children.get(&ino) {
            for (i, ino) in children.iter().enumerate().skip(offset as usize) {
                if let Some((name, file_attr)) = self.inodes.get(ino) {
                    reply.add(*ino, (i + 1) as i64, file_attr.kind, name);
                } else {
                    reply.error(ENOENT);
                    return;
                }
            }
        }

        reply.ok();
    }

    fn releasedir(&mut self, _ino: u64, _fh: u64, _flags: u32, reply: crate::ReplyEmpty) {
        reply.ok();
    }

    fn rmdir(&mut self, parent: u64, name: &std::ffi::OsStr, reply: crate::ReplyEmpty) {
        if let Some(name_str) = name.to_str() {
            self.get_node_by_name(parent, name_str);
            self.inodes.remove(&parent);
            self.parent_children
                .get_mut(&parent)
                .unwrap()
                .retain(|x| *x != parent);

            reply.ok();
            return;
        }

        reply.error(ENOENT);
    }

    fn lookup(&mut self, parent: u64, name: &std::ffi::OsStr, reply: crate::ReplyEntry) {
        match name
            .to_str()
            .and_then(|name_str| self.get_node_by_name(parent, name_str))
        {
            Some(ino) => {
                if let Some((_, file_attr)) = self.inodes.get(&ino) {
                    reply.entry(&Duration::new(1, 0), file_attr, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            None => {
                reply.error(ENOENT);
            }
        }
    }

    fn open(&mut self, _ino: u64, _flags: u32, reply: crate::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(&mut self, ino: u64, _fh: u64, offset: i64, _size: u32, reply: crate::ReplyData) {
        if let Some(data) = self.data.get(&ino) {
            reply.data(&data[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn release(
        &mut self,

        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: crate::ReplyEmpty,
    ) {
        reply.ok();
    }

    // Create a new file
    fn create(
        &mut self,

        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _flags: u32,
        reply: crate::ReplyCreate,
    ) {
        if !self.inodes.contains_key(&parent) {
            reply.error(ENOENT);
            return;
        }

        let ino = self.inodes_num + 1;
        let file_attr = FileAttr {
            ino,
            kind: FileType::RegularFile,
            perm: 0o755,
            ..Default::default()
        };

        self.inodes
            .insert(ino, (name.to_str().unwrap().to_string(), file_attr));

        self.inodes_num += 1;
        self.data.insert(ino, Vec::new());

        self.parent_children.get_mut(&parent).unwrap().push(ino);
        reply.created(&Duration::new(1, 0), &file_attr, 0, 0, 0);
    }

    // create a directory
    fn mkdir(&mut self, parent: u64, name: &std::ffi::OsStr, _mode: u32, reply: crate::ReplyEntry) {
        let ino = self.inodes_num + 1;
        let file_attr = FileAttr {
            ino,
            kind: FileType::Directory,
            perm: 0o755,
            ..Default::default()
        };

        self.inodes
            .insert(ino, (name.to_str().unwrap().to_string(), file_attr));
        self.inodes_num += 1;
        self.data.insert(ino, Vec::new());
        self.parent_children.insert(ino, vec![]);

        self.parent_children.get_mut(&parent).unwrap().push(ino);
        reply.entry(&Duration::new(1, 0), &file_attr, 0);
    }

    fn setattr(
        &mut self,

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
        let Some((_, file_attr)) = self.inodes.get_mut(&ino) else {
            reply.error(ENOENT);
            return;
        };

        mode.map(|mode| file_attr.perm = mode as u16);
        uid.map(|uid| file_attr.uid = uid);
        gid.map(|gid| file_attr.gid = gid);
        size.map(|size| file_attr.size = size);
        atime.map(|atime| file_attr.atime = atime);
        mtime.map(|mtime| file_attr.mtime = mtime);
        crtime.map(|crtime| file_attr.crtime = crtime);
        flags.map(|flags| file_attr.flags = flags);
        reply.attr(&Duration::new(1, 0), file_attr);
    }

    fn write(
        &mut self,

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
            if let Some((_, file_attr)) = self.inodes.get_mut(&ino) {
                file_attr.size += data.len() as u64;
            }
            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn unlink(&mut self, parent: u64, name: &std::ffi::OsStr, reply: crate::ReplyEmpty) {
        match name
            .to_str()
            .and_then(|name_str| self.get_node_by_name(parent, name_str))
        {
            Some(ino) => {
                self.inodes.remove(&ino);
                self.data.remove(&ino);
                self.parent_children
                    .get_mut(&parent)
                    .unwrap()
                    .retain(|x| *x != ino);
                reply.ok();
            }
            None => {
                reply.error(ENOENT);
            }
        }
    }

    fn flush(&mut self, _ino: u64, _fh: u64, _lock_owner: u64, reply: crate::ReplyEmpty) {
        reply.ok();
    }
}

/// Create a new in-memory filesystem
pub fn new(max_size: u64) -> MemoryFS {
    MemoryFS {
        max_size,
        inodes_num: 0,
        inodes: HashMap::new(),
        data: HashMap::new(),
        parent_children: HashMap::new(),
    }
}

impl MemoryFS {
    pub fn new(max_size: u64) -> MemoryFS {
        MemoryFS {
            max_size,
            inodes_num: 0,
            inodes: HashMap::new(),
            data: HashMap::new(),
            parent_children: HashMap::new(),
        }
    }

    pub fn get_node_by_name(&self, parent: u64, name: &str) -> Option<u64> {
        if let Some(children) = self.parent_children.get(&parent) {
            for child in children {
                if let Some((child_name, _)) = self.inodes.get(child) {
                    if child_name == name {
                        return Some(*child);
                    }
                }
            }
        }
        None
    }
}
