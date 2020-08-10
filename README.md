# evilgitfs

evilgitfs is a FUSE file system that stores your files on a remote git repository. You can limit the amount of local disk storage used, and evilgitfs uses an LRU cache to make full use of the local disk storage, while allowing you to have a total file storage more than the specified local disk storage.

All commands except read are done in background and non-blocking.

## Installation

```
git clone https://github.com/lohjine/evilgitfs
pip3 install -r requirements.txt
```

## Usage

resilient_code can be used on functions or code blocks.

```
python3 evilgitfs -h

usage: f.py [-h] [--cache-size CACHE_SIZE] [--sync-freq SYNC_FREQ] [--workers WORKERS] [--git-directory GIT_DIRECTORY] username gitrepo mountpoint

evilgitfs is a FUSE file system that stores your files on a remote git repository. You can limit the amount of local disk storage used, and evilgitfs
uses an LRU cache to make full use of the local disk storage, while allowing you to have a total file storage more than the specified local disk storage.
All commands except read are done in background and non-blocking.

positional arguments:
  username              your git username
  gitrepo               target git repository, has to exist
  mountpoint            filepath for local mount point

optional arguments:
  -h, --help            show this help message and exit
  --cache-size CACHE_SIZE
                        cache size on local disk in GB (default=10)
  --sync-freq SYNC_FREQ
                        sync frequency of file listing in minutes (default=5)
  --workers WORKERS     number of threads for git operations (default=5)
  --git-directory GIT_DIRECTORY
                        directory for evilgitfs operations and cache storage (default='~/.evilgitfs')
```

## Unavailable features

* Sanity checking / Error handling for max repo space, max file size
* Support for multiple clients syncing to same repository
* Retrying when git push/pull fails
* Possible race conditions when a file is quickly modified multiple times
* Auto-split large files