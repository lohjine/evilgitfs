import os
import sys
import errno
import csv
import logging
import shutil
import subprocess
import hashlib
import time
import threading
from urllib.parse import urlparse
from glob import glob
from concurrent.futures import ThreadPoolExecutor
import argparse
from collections import OrderedDict
from collections import namedtuple, defaultdict
from pathlib import Path
from fuse import FUSE, FuseOSError, Operations
from errno import ENOENT


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def getFromDict(dataDict, mapList):
    """
    Retrieves value from nested dict (dir_structure) using a list of keys
    
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys
    """
    
    for k in mapList:
        dataDict = dataDict.get(k, None)
        if dataDict is None:
            break
    return dataDict


def deleteFromDict(dataDict, mapList, delete_empty_recursive=False):
    """
    Deletes key from nested dict (dir_structure) using a list of keys, optionally deletes empty dicts as a result of deletion    
    """

    _delfirst(dataDict, mapList)
    if delete_empty_recursive:
        for j in range(1, len(mapList)):
            delete = _delsecond(dataDict, mapList, j)
            if delete:
                _delfirst(dataDict, mapList[:-j])
            else:
                break
    return True


def _delfirst(dataDict, mapList):
    for k in mapList[:-1]:
        dataDict = dataDict.get(k, None)
    del dataDict[mapList[-1]]


def _delsecond(dataDict, mapList, j):
    for k in mapList[:-j]:
        dataDict = dataDict.get(k, None)
    if len(dataDict) == 0:
        return True
    else:
        return False


def nested_set(dic, keys, value):
    """
    Sets key-value in nested dict (dir_structure) using a list of keys
    """
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value


def split_path_all(path):
    """
    Splits a filepath into a list of directories so it can be used to interact with dir_structure using nested dict functions
    """
    # this is here because get burnt by this too much, fusepy's path always start with / but we don't want it
    if path.startswith("/"):  
        path = path[1:]
    partial = path
    
    folders = []
    folder = ' '
    while folder != "":
        path, folder = os.path.split(path)

        if folder != "":
            folders.append(folder)
        else:
            if path != "":
                folders.append(path)

    folders.reverse()

    return partial, folders


class LRU(OrderedDict):
    """
    This keeps track of files on local filesystem and their sizes.
    
    Limit filesize, evicting the least recently looked-up key when full.    
    """

    def __init__(self, data_dir, maxsize=10, *args, **kwds):
        """
        maxsize: GB

        """
        self.maxsize = maxsize * 1e9
        self.filesize_counter = 0
        self.data_dir = data_dir
        super().__init__(*args, **kwds)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        self.filesize_counter += value
        while self.filesize_counter > self.maxsize:
            if len(self) == 1:
                break
            oldest = next(iter(self))
            self.filesize_counter -= self[oldest]
            del self[oldest]

            # delete from FS
            os.remove(os.path.join(self.data_dir, oldest))

    def __delitem__(self, key):
        self.filesize_counter -= self[key]
        super().__delitem__(key)


#####################
##
# Git functions
##
#####################


def git_remove_from_remote(evilgitfs_dir, path_hash):

    dirtydir = pre_git_ops(evilgitfs_dir)

    filelist_path = os.path.join(evilgitfs_dir, 'pure', 'filelist.txt')

    output = subprocess.run(
        f'git push origin --delete {path_hash}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    # remove from pure/filelist
    output = subprocess.run(
        f" sed -i '/{path_hash}/d' {filelist_path}",
        capture_output=True,
        shell=True)  # assume hash don't collide, or we got bigger problems
    logging.debug(output)

    post_git_ops(evilgitfs_dir)

    return True


def git_rename_branch(evilgitfs_dir, path_old, path_new,
                      destination_file_exists, remove_from_remote_func):
    """
    We want to ensure that destination file is removed before renaming, so we block on that here.
    """

    dirtydir = pre_git_ops(evilgitfs_dir)
    filelist_path = os.path.join(evilgitfs_dir, 'pure', 'filelist.txt')
    
    path_hash_old = hashlib.sha1(bytes(path_old, 'utf-8')).hexdigest()[:-1]
    path_hash_new = hashlib.sha1(bytes(path_new, 'utf-8')).hexdigest()[:-1]

    if destination_file_exists:
        # have to delete destination file if it exists, or same path+filename
        # will clash
        remove_from_remote_func(path_old, block=True)

    # Rename git branch remotely to save on 2-way file transfer, unfortunately still have to fetch it first.
    # See https://stackoverflow.com/a/21302474
    output = subprocess.run(
        f'git fetch origin {path_hash_old}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    output = subprocess.run(
        f'git push origin origin/{path_hash_old}:refs/heads/{path_hash_new} :{path_hash_old}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    # edit pure/filelist
    # remove then add
    output = subprocess.run(
        f" sed -i -e '/{path_hash_old}/{{w /dev/stdout' -e 'd}}' {filelist_path}",
        capture_output=True,
        shell=True)  # assume hash don't collide, or we got bigger problems
    logging.debug(output)

    # filepath might contain space, but path_hash and filesize has no space,
    # so we can safely assume last split is filesize
    filesize = output.stdout.strip().decode('utf-8').split(' ')[-1]

#     logging.debug(filesize)

    with open(filelist_path, 'a') as csvfile:
        csvwriter = csv.writer(
            csvfile,
            delimiter=' ',
            quotechar='|',
            quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow([path_new, path_hash_new, filesize])

    post_git_ops(evilgitfs_dir)

    return True


def git_commit_to_remote(evilgitfs_dir, path_hash, full_path, filename, path):

    dirtydir = pre_git_ops(evilgitfs_dir)
    filelist_path = os.path.join(evilgitfs_dir, 'pure', 'filelist.txt')
    dirty_filepath = os.path.join(dirtydir, filename)

    # pull first in case it exists
    # if new file, will error, but doesn't matter
    output = subprocess.run(
        f'git pull origin {path_hash}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    output = subprocess.run(
        f'git checkout -b {path_hash}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    # don't think there is a way to replace without fetching, unless we look for and delete entire branch
    # if we don't care about history, can just blast away remote branch using
    # git ls-remote --heads origin path_hash - if output != None, blast it away

    logging.debug(output)

    # then transfer file
    # now we only transfer the file to base dir, because it will make renaming
    # branch possible without deletebranch/makebranch
    shutil.copy(full_path, dirty_filepath)

    # add + commit + push
    output = subprocess.run(
        f"git add {dirty_filepath} && git commit -m 'a' && git push -u origin {path_hash}",
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    # finally update pure/filelist.txt
    with open(filelist_path, 'a') as csvfile:
        csvwriter = csv.writer(
            csvfile,
            delimiter=' ',
            quotechar='|',
            quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow([path, path_hash, os.stat(full_path).st_size])

    # clean up dirty
    output = subprocess.run(
        f'git checkout master',
        cwd=dirtydir,
        capture_output=True,
        shell=True)

    post_git_ops(evilgitfs_dir)

    return True


def git_retrieve_from_remote(evilgitfs_dir, path_hash, path_file):
    """
    Retrieve is safe for multiple threads to simultaneously use. But we will still use individual dirty directory so we can track
    and clean up filesize.
    """

    dirtydir = pre_git_ops(evilgitfs_dir)

    # i assume is in master branch
    output = subprocess.run(
        f'git fetch origin {path_hash}:{path_hash}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    output = subprocess.run(
        f'git checkout {path_hash} -- {path_file}',
        cwd=dirtydir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    shutil.move(os.path.join(dirtydir, path_file), full_path)

    post_git_ops(evilgitfs_dir)

    return True


def pre_git_ops(evilgitfs_dir):
    """
    Directs thread to correct dirtydirectory to work from, and creates it if missing
    """

    dirtydir = os.path.join(
        evilgitfs_dir,
        'dirty_' +
        threading.current_thread().name)
    puredir = os.path.join(evilgitfs_dir, 'pure')

    # if dir not there, make it
    if not os.path.exists(dirtydir):
        shutil.copytree(puredir, dirtydir)

    return dirtydir


def post_git_ops(evilgitfs_dir):
    """
    If queue empty, checks if filesize of directory is too large, and remakes directory if so.
    """
    # if dir_size > 0.1% of cache size and queue is empty
    # wipe and remake

    dirtydir = os.path.join(
        evilgitfs_dir,
        'dirty_' +
        threading.current_thread().name)
    puredir = os.path.join(evilgitfs_dir, 'pure')

    # https://stackoverflow.com/a/1392549
    if executor._work_queue.qsize() < max_workers:
        dir_size = sum(f.stat().st_size for f in Path(
            dirtydir).glob('/.git/objects/**/*') if f.is_file())
        if dir_size > cache_size * 1e9:
            # only check .git/objects to reduce number of dirs to check

            logging.debug(f'Remaking {dirtydir}, size {dir_size}')

            shutil.rmtree(dirtydir)
            shutil.copytree(puredir, dirtydir)

    return True


def git_sync_filelist(evilgitfs_dir):
    """
    Pull changes, merge and push changes for pure/filelist.txt

    Only handles merge conflicts by accepting both modifications, so only additions + additions can work.

    """

    puredir = os.path.join(evilgitfs_dir, 'pure')
    filelist_path = os.path.join(puredir, 'filelist.txt')

    # if we want multi-client, maybe filelist.txt should be a list of actions.
    # can just prune after a while
    output = subprocess.run(
        f'git commit -a -m "update filelist"',
        cwd=puredir,
        capture_output=True,
        shell=True)
    logging.debug(output)
    output = subprocess.run(
        f'git pull origin master',
        cwd=puredir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    if b"Merge conflict" in output.stdout:
        output = subprocess.run(
            f" sed -i -e '/^<<<<<<</d' -e '/^=======/d' -e '/^>>>>>>>/d' {filelist_path}",
            capture_output=True,
            shell=True)
        logging.debug(output)

        # update dir_structure
        with open(os.path.join(pure_dir, 'filelist.txt'), 'r') as csvfile:
            f = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for filepath, branchname, filesize in f:
                partial, all_paths = split_path_all(filepath)
                # do this way because helps in directory commands like ls
                nested_set(dir_structure, all_paths, int(filesize))
                remote_file_size += int(filesize)

    output = subprocess.run(
        f'git commit -a -m "merge conflict"',
        cwd=puredir,
        capture_output=True,
        shell=True)
    logging.debug(output)
    output = subprocess.run(
        f'git push -u origin master',
        cwd=puredir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    return True


#####################
##
# FUSE class
##
#####################

class Passthrough(Operations):
    def __init__(self, evilgitfs_dir):
        self.evilgitfs_dir = evilgitfs_dir
        self.data_dir = os.path.join(evilgitfs_dir, 'datadir')
        self.actions = defaultdict(set)

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.data_dir, partial)

        return path

    # Filesystem methods
    # ==================================================================
    # ==================================================================
    # ==================================================================

#     def access(self, path, mode):
#         full_path = self._full_path(path)
#         if not os.access(full_path, mode):
#             raise FuseOSError(errno.EACCES)

#     def chmod(self, path, mode):
#         full_path = self._full_path(path)
#         return os.chmod(full_path, mode)

#     def chown(self, path, uid, gid):
#         full_path = self._full_path(path)
#         return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        logging.debug(f'GETATTR {path} {fh}')

        partial, all_paths = split_path_all(path)

#         logging.debug(f'{all_paths} {dir_structure} {getFromDict(dir_structure, all_paths)}')

        # check whether path exists in dir_struct
        if lru_file_cache.get(partial, None) is not None:
            # if in cache, it exists on filesystem, return accurate lstat
            logging.debug(f'{lru_file_cache} {full_path}')
            st = os.lstat(full_path)
            logging.debug(f'present in lru {st}')
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))  # , 'st_blocks'
        elif getFromDict(dir_structure, all_paths) is not None:
            # else if in dir_structure, report accurate size but weird date for label
            logging.debug('present in dir_structure')
            if os.path.exists(
                    full_path):  # if in directory, check if on filesystem
                st = os.lstat(full_path)
                logging.debug(str(st))
                return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                                'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            else:
                if isinstance(getFromDict(dir_structure, all_paths), dict):
                    logging.debug('mirage directory')
                    st_mode = 16893
                    st_size = 4096
                    st_nlink = 2
                else:
                    logging.debug('mirage file')
                    st_mode = 33204
                    st_size = 0
                    st_nlink = 1
                logging.debug(str({'st_mode': st_mode, 'st_uid': 1001, 'st_nlink': st_nlink,
                                   'st_gid': 1001, 'st_size': st_size, 'st_atime': 7226582400, 'st_mtime': 7226582400, 'st_ctime': 7226582400}))
                # if file/dir doesn't exist locally, report year 2199 for them
                return {'st_mode': st_mode, 'st_uid': 1001, 'st_nlink': st_nlink,
                        'st_gid': 1001, 'st_size': st_size, 'st_atime': 7226582400, 'st_mtime': 7226582400, 'st_ctime': 7226582400}

        else:
            raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        dirents = set(['.', '..'])
        # this needs to go through .git, and also show which are cached
        # or rather, go through dir_structure  in memory
        partial, all_paths = split_path_all(path)
        dir_listing = getFromDict(dir_structure, all_paths)

        if dir_listing is None:
            raise FuseOSError(ENOENT)
        if isinstance(dir_listing, dict):
            dirents.update(dir_listing.keys())

        # also add empty directories present in dir_structure
        dir_listing = getFromDict(dir_structure, all_paths)
        if isinstance(dir_listing, dict):
            dirents.update(dir_listing.keys())

        logging.debug(f'READ DIR {path} with {dirents}')

        for r in dirents:
            yield r

# symbolic path thingy
#     def readlink(self, path):
#         pathname = os.readlink(self._full_path(path))
#         if pathname.startswith("/"):
#             # Path name is absolute, sanitize it.
#             return os.path.relpath(pathname, self.data_dir)
#         else:
#             return pathname

#     def mknod(self, path, mode, dev):
#         return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        """
        Only called for empty directories

        """
        full_path = self._full_path(path)

        logging.debug(f'RMDIR {path} {full_path}')

        # remove directory from dir_structure
        partial, all_paths = split_path_all(path)
        deleteFromDict(dir_structure, all_paths, delete_empty_recursive=False)

        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        full_path = self._full_path(path)
        logging.debug(f'MKDIR {path} {full_path}')

        if not os.path.exists(full_path):
            os.mkdir(full_path, mode)

        # update internal listing (only if os.mkdir succeeds)
        partial, all_paths = split_path_all(path)
        # not added to LRU because LRU will evict
        nested_set(dir_structure, all_paths, {})

        return None

#     def statfs(self, path):
#         full_path = self._full_path(path)
#         stv = os.statvfs(full_path)
#         return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
#             'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
#             'f_frsize', 'f_namemax'))

    def unlink(self, path):
        logging.debug(f'UNLINK {path}')

        # for hidden files (.swp / mode 33152 / file~)
        # they won't be present in lru_file_cache or dir_structure
        # so let os delete (if path exists)

        partial, all_paths = split_path_all(path)

        logging.debug(f'{lru_file_cache}')

        if lru_file_cache.get(partial, None) is not None:
            logging.debug('lru delete')
            # delete from lru
            del lru_file_cache[partial]
            # delete from dir_structure
            deleteFromDict(
                dir_structure,
                all_paths,
                delete_empty_recursive=False)
            # remove from remote
            self.remove_from_remote(path, block=False)

            # actually delete from FS
            return os.unlink(self._full_path(path))

        elif getFromDict(dir_structure, all_paths) is not None:
            logging.debug('dir delete')
            # delete from dir_structure
            deleteFromDict(
                dir_structure,
                all_paths,
                delete_empty_recursive=False)
            # remove from remote
            self.remove_from_remote(path, block=False)

            # don't actually try to delete from FS
            return None

        # if doesn't exist anywhere, still return os.unlink for expected error
        return os.unlink(self._full_path(path))

#     def symlink(self, name, target):
#         return os.symlink(target, self._full_path(name))

    def rename(self, old_path, new_path):
        """
        unix mv behaviours are as follows:
        - If target destination already exists
            - If file to file: overwrite file
            - If file to dir: source file gets moved into destination directory
            - If dir to file: mv: cannot overwrite non-directory with directory
            - If dir to dir: source directory gets moved into destination directory

        unix mv does its checks before this function is ran by fusepy, so by the time it reaches this function, the source and destinations are valid. Either file to file, or dir to dir.

        os.rename behaviours are as follows:
        - If target destination already exists
            - If file to file: overwrite file
            - If file to dir: raise [Errno 21] Is a directory
            - If dir to file: raise [Errno 20] Not a directory
            - If dir to dir: destination directory must be empty else raise [Errno 39] Directory not empty

        The following behaviour will hence be as follows:
        - Check if destination is file and already exists:
            - If yes, delete destination file
        - Check if source is directory and contains files:
            - If yes, rename every file within recursively
              (Unfortunately, there doesn't seem to be a way to ask for user input to confirm move of potentially many files)

        """
        logging.debug(f'RENAME {old_path} {new_path}')

        partial_old, all_paths_old = split_path_all(old_path)
        partial_new, all_paths_new = split_path_all(new_path)

        logging.debug(f'{dir_structure}')

        destination_file_exists = False
        if self._isfile(all_paths_old):
            if getFromDict(dir_structure, all_paths_old) is not None:
                if self._isfile(all_paths_new):
                    destination_file_exists = True

        os.rename(self._full_path(old_path), self._full_path(new_path))
        # do os.rename first and only update internal listings if succeed

        if self._isfile(all_paths_old):
            # updating branch for renaming files

            # update dir_structure
            nested_set(
                dir_structure,
                all_paths_new,
                getFromDict(
                    dir_structure,
                    all_paths_old))
            deleteFromDict(dir_structure, all_paths_old)
            # update LRU if present
            if lru_file_cache.get(partial_old, None) is not None:
                lru_file_cache[partial_new] = lru_file_cache.get(
                    partial_old, None)
                del lru_file_cache[partial_old]

            self.rename_branch(
                partial_old,
                partial_new,
                destination_file_exists)

        else:
            # updating branch for renaming directories
            # this transfers each individual file
            # does not retain empty directories

            # since we are storing hashed filepaths as references, we need to
            # autodetect nested files and rename them all

            for root, dirs, files in os.walk(self._full_path(new_path)):
                root = root.replace(self._full_path(new_path), '')
                partial, all_paths = split_path_all(root)

                if files:
                    for file in files:

#                         logging.debug(f'{partial_old} | {partial_new} |{partial} | {file} | {all_paths}')

                        partial_old_internal = os.path.join(
                            partial_old, partial, file)
                        partial_new_internal = os.path.join(
                            partial_new, partial, file)
                        all_paths_old_internal = all_paths_old + \
                            all_paths + [file]
                        all_paths_new_internal = all_paths_new + \
                            all_paths + [file]

#                         logging.debug(f'{partial_old_internal}, {partial_new_internal}, {all_paths_old_internal}, {all_paths_new_internal}')

#                         logging.debug(f'{dir_structure}')
                        # update dir_structure
                        nested_set(
                            dir_structure, all_paths_new_internal, getFromDict(
                                dir_structure, all_paths_old_internal))
                        deleteFromDict(
                            dir_structure,
                            all_paths_old_internal,
                            delete_empty_recursive=True)
                        logging.debug(f'{dir_structure}')

#                         logging.debug(f'{lru_file_cache}')
                        # update LRU if present
                        if lru_file_cache.get(
                                partial_old_internal, None) is not None:
                            lru_file_cache[partial_new_internal] = lru_file_cache.get(
                                partial_old_internal, None)
                            del lru_file_cache[partial_old_internal]
                        logging.debug(f'{lru_file_cache}')

                        self.rename_branch(
                            partial_old_internal,
                            partial_new_internal,
                            destination_file_exists=False)

                else:

                    all_paths_old_internal = all_paths_old + all_paths
                    all_paths_new = all_paths

                    logging.debug(f'{all_paths_old_internal}')

                    # dir_struture may not contain directories? (To think about
                    # it)
                    if getFromDict(dir_structure,
                                   all_paths_old_internal) is not None:
                        nested_set(
                            dir_structure, all_paths_new, getFromDict(
                                dir_structure, all_paths_old_internal))
                        deleteFromDict(dir_structure, all_paths_old_internal)

                   # LRU does not have directories

        return None

#     def link(self, target, name):
#         return os.link(self._full_path(name), self._full_path(target))

# set access modified time
#     def utimens(self, path, times=None):
#         return os.utime(self._full_path(path), times)

    # File methods
    # ==================================================================
    # ==================================================================
    # ==================================================================

    def _isfile(self, all_paths):
        if isinstance(getFromDict(dir_structure, all_paths), dict):
            return False
        elif isinstance(getFromDict(dir_structure, all_paths), int):
            return True
        else:
            raise ValueError(
                f'_isfile returned neither true nor false {dir_structure} {all_paths} {getFromDict(dir_structure, all_paths)}')

    def rename_branch(self, path_old, path_new, destination_file_exists):
        """
        The way we set things up, since we hash the path to get branch name, we have to delete branch

        Refer to: https://stackoverflow.com/a/21302474
        """
        if path_old.startswith("/"):
            path_old = path_old[1:]

        if path_new.startswith("/"):
            path_new = path_new[1:]


        executor.submit(
            git_rename_branch,
            self.evilgitfs_dir,
            path_old,
            path_new,
            destination_file_exists,
            self.remove_from_remote)

        return True

    def remove_from_remote(self, path, block=False):
        logging.debug(f'REMOVING FROM REMOTE {path}')

        if path.startswith("/"):
            path = path[1:]

        path_hash = hashlib.sha1(bytes(path, 'utf-8')).hexdigest()[:-1]

        partial = path
        if partial.startswith("/"):
            partial = partial[1:]

        if block:
            # used in git_rename_branch
            executor.submit(
                git_remove_from_remote,
                self.evilgitfs_dir,
                path_hash).result()
        else:
            executor.submit(
                git_remove_from_remote,
                self.evilgitfs_dir,
                path_hash)

        return True

    def retrieve_from_remote(self, path, full_path):

        logging.debug(f'RETRIEVING FROM REMOTE {path} {full_path}')

        if path.startswith("/"):
            path = path[1:]

        path_hash = hashlib.sha1(bytes(path, 'utf-8')).hexdigest()[:-1]

        # create preceding directories if neccessary
        partial, all_paths = split_path_all(full_path)
        _, path_file = os.path.split(path)
        dir_traversal = ''

        for i in all_paths[:-1]:  # assume last element is a file
            dir_traversal += '/' + i
            if not os.path.exists(dir_traversal):
                os.mkdir(dir_traversal)

        # create lock file so no one else touches the file while we do our slow work here
        # this is for external interference, this function is wrapped by retrieve_queue
        open(full_path, 'a').close()
        logging.debug(f'created lock file {full_path}')

        # async call, but we want to block using .result()
        executor.submit(
            git_retrieve_from_remote,
            self.evilgitfs_dir,
            path_hash,
            path_file).result()

        # add to LRU!
        self._add_file_to_fs(path, create=False)

        return True

    def commit_to_remote(self, path):
        logging.debug('COMMITING TO REMOTE')

        if path.startswith("/"):
            path = path[1:]

        path_hash = hashlib.sha1(bytes(path, 'utf-8')).hexdigest()[:-1]

        full_path = self._full_path(path)
        _, filename = os.path.split(path)

        executor.submit(
            git_commit_to_remote,
            self.evilgitfs_dir,
            path_hash,
            full_path,
            filename,
            path)

        return True

    def open(self, path, flags):
        full_path = self._full_path(path)
        logging.debug(f'OPEN {path} {full_path} {flags}')

        # check exists in current retrieval queue so we don't retrieve the same object multiple times
        if path in retrieve_queue:
            for i in range(
                    100):  # 10 second wait for retrieve to complete, else go retrieve again (consider raise error instead?)
                time.sleep(0.1)
                if path not in retrieve_queue:
                    break

        partial, all_paths = split_path_all(path)

        if lru_file_cache.get(partial, None) is None:
            if getFromDict(dir_structure, all_paths) is not None:
                # TODO trying to open a directory should give a IsADirectory
                # error, but does it actually go in here?
                retrieve_queue.add(path)
                self.retrieve_from_remote(path, full_path)  # this blocks!
                retrieve_queue.remove(path)

        # if hidden file, won't be found in cache
        # but file will be found by this open

        # if file not present, this will show a filenotfound error

        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        logging.debug(f'CREATE {path} {full_path} mode:{mode}')

        if mode == 33152 or path[-1] == '~':
            # IGNORE the following
            # mode == 33152 : probably hidden file
            # path[-1] == '~' : vim backup file
            return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

        self.actions[path].add('write')
        # seems like need to add the file to LRU / dir_structure at this point,
        # so ls can work right after!
        self._add_file_to_fs(path, create=True)

        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        logging.debug(f'READ {path}')
        self.actions[path].add('read')
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        logging.debug(f'write {path}')
        self.actions[path].add('write')
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        logging.debug(f'truncate {path} {full_path}')
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        # we might need to save here, investigate
        logging.debug(f'FLUSHED {path}')
        return os.fsync(fh)

    def release(self, path, fh):
        """ The application is finished reading or writing the file, now
        check the queue for any pending actions and add them to workers. """

        """
        according to pyfasts3, it's possible that one call can read/write multiple files?!
        if so, we need to add path into self.actions like them
        """
        logging.debug(f'FILE CLOSED {path}')

        actions = self.actions.pop(path, ())

        if 'write' in actions:
            # add the updated file to dir_structure
            self._add_file_to_fs(path)
            self.commit_to_remote(path)  # not in a hurry for this
        elif 'read' in actions:
            pass  # in the future, we might want to check the remote repo for updates on this file?
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        logging.debug(f'fsync {path}')
        return self.flush(path, fh)

    def _add_file_to_fs(self, path, create=False):
        if create:
            size = 0
        else:
            size = os.lstat(self._full_path(path)).st_size
        partial, all_paths = split_path_all(path)
        nested_set(dir_structure, all_paths, size)
        
        # and LRU
        lru_file_cache[partial] = size

        logging.debug(dir_structure)
        logging.debug(lru_file_cache)


def main(mountpoint, evilgitfs_dir):
    global remote_file_size

    data_dir = os.path.join(evilgitfs_dir, 'datadir')
    pure_dir = os.path.join(evilgitfs_dir, 'pure')

    if os.path.exists(evilgitfs_dir):
        # ensure consistencies
        pass
    else:
        os.makedirs(evilgitfs_dir)
        os.makedirs(data_dir)
        os.makedirs(pure_dir)
        open(os.path.join(pure_dir, 'filelist.txt'), 'w').close()

    # Check whether pure exists
    # if not, git clone
    if not os.path.exists(os.path.join(pure_dir, '.git')):
        output = subprocess.run(
            f'git clone https://{username}:{token}@{gitrepo}',
            cwd=pure_dir,
            capture_output=True,
            shell=True)
        
        if b"fatal: repository" in output.stdout and b"not found" in output.stdout:
            raise ValueError('Repo not found, please go to git repo website to create repo')

    # if yes, git pull
    # pure should always be in master, so don't bother check out master, just pull
    output = subprocess.run(
        f'git remote set-url origin https://{username}:{token}@{gitrepo}',
        cwd=pure_dir,
        capture_output=True,
        shell=True)

    output = subprocess.run(
        f"git pull https://{username}:{token}@{gitrepo}",
        cwd=pure_dir,
        capture_output=True,
        shell=True)
    logging.debug(output)

    # Delete all fsworker dirs to cleanup
    for i in glob(os.path.join(evilgitfs_dir, 'fsworker*')):
        shutil.rmtree(i)

    # populate dir_structure and remote_file_size
    with open(os.path.join(pure_dir, 'filelist.txt'), 'r') as csvfile:
        f = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for filepath, branchname, filesize in f:
            partial, all_paths = split_path_all(filepath)
            # do this way because helps in directory commands like ls
            nested_set(dir_structure, all_paths, int(filesize))
            remote_file_size += int(filesize)

        logging.debug(f'remote_file_size {remote_file_size}')

    # populate lru_file_cache
    for root, dirs, files in os.walk(data_dir):
        if files:
            root = root.replace(data_dir, '')
            partial, all_paths = split_path_all(root)
            for file in files:
                try:
                    # only add to cache if exists in repo!
                    filesize = getFromDict(dir_structure, [*all_paths, file])

                    if filesize is None:
                        # TODO offer to retry upload all
                        # else might want to delete files, or LRU won't hold
                        # promise
                        logging.error(
                            f"orphan file (in local but not remote) {[*all_paths,file]}")

                        continue

                    lru_file_cache[os.path.join(partial, file)] = int(filesize)

#                     logging.debug(f"added {[*all_paths,file]}, size {filesize}")
                except KeyError:
                    logging.error(f"file not found {[*all_paths,file]}")
                    continue

    logging.debug(f'dir_structure {dir_structure}')
    logging.debug(f'dir_structure {lru_file_cache}')

    FUSE(
        Passthrough(evilgitfs_dir),
        mountpoint,
        nothreads=False,
        foreground=True)


def sync_loop(evilgitfs_dir, sync_freq):

    while True:
        time.sleep(sync_freq * 60)
        logging.debug('syncing filelist.txt')
        git_sync_filelist(evilgitfs_dir)


if __name__ == '__main__':

    description = """
evilgitfs is a FUSE file system that stores your files on a remote git repository. You can limit the amount of local disk storage used, and evilgitfs uses an LRU cache to make full use of the local disk storage, while allowing you to have a total file storage more than the specified local disk storage.

All commands except read are done in background and non-blocking."""

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('username',
                        help='your git username')
    parser.add_argument('gitrepo',
                        help='target git repository, has to exist')
    parser.add_argument('mountpoint',
                        help='filepath for local mount point')
    parser.add_argument('--cache-size', default=10, type=int,
                        help='cache size on local disk in GB (default=10)')
    parser.add_argument('--sync-freq', default=5, type=int,
                        help='sync frequency of file listing in minutes (default=5)')
    parser.add_argument('--workers', default=5, type=int,
                        help='number of threads for git operations (default=5)')
    parser.add_argument('--git-directory', default='~/.evilgitfs',
                        help='directory for evilgitfs operations and cache storage (default=\'~/.evilgitfs\')')

    args = parser.parse_args()

    try:
        git_token = os.environ['evilgitfs_gittoken']
    except KeyError:
        token = input(
            f'Enter git token for {args.username}. Set environment variable \'evilgitfs_gittoken\' to automate this.\n Token: ')

    username = args.username
    gitrepo = args.gitrepo  
    cache_size = args.cache_size  
    sync_freq = args.sync_freq
    max_workers = args.workers
    evilgitfs_dir = os.path.expanduser(args.git_directory)
    mount_dir = os.path.expanduser(args.mountpoint)

    lru_file_cache = LRU(
        os.path.join(
            evilgitfs_dir,
            'datadir'),
        maxsize=cache_size)
    # key = filepath
    # value = filesize
    # LRU strictly for files because it will evict least-used

    dir_structure = {}
    # {dir_a: {file_a:size, file_b:size, dir_c:{}}
    # empty dir wiped on restart

    retrieve_queue = set()

    remote_file_size = 0

    executor = ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix='fsworker')
    # thread all writes
    # thread all erase
    # block all reads
    # use threads throughout to ensure 1 queue / maximum number of
    # simultaneous connections

    sync_filelist = threading.Thread(
        target=sync_loop, args=(
            evilgitfs_dir, sync_freq))
    sync_filelist.start()

    gitrepo_parsed = urlparse(gitrepo)
    gitrepo = gitrepo_parsed.netloc + gitrepo_parsed.path

    main(mount_dir, evilgitfs_dir)
