# memory-hole
Memory Hole is a cloud-based archival gateway

====

Memory Hole is a fuse-based file system that can be mounted wherever it is
convenient to keep such things.  Files can be copied into the memory hole;
once there, they are copied into the cloud in immutable data sets.

This is done by creating a new directory in the memory hole:

```bash
/usr/local/memory-hole$ mkdir my_archive
/usr/local/memory-hole$ cd my_archive
/usr/local/memory-hole/my_archive$ ls
data  done  status
```

Files can be copied into the "data" directory.  While being copied, they are
stored locally.  Finally, you can close an archive by removing the "done" file.
This will start the copy into the cloud backend.  The status file contains
information about this process.  The data will be continually available during
and after this process, but the archive will be read-only as soon as the done
file is removed.

TODO: add easy support for rsync
