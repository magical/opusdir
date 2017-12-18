# opusdir - transcode a directory tree from FLAC to Opus
# requires Python 3.3

import argparse
import collections
import os
import queue
import shutil
import stat
import subprocess
import sys
import threading
import unittest

num_workers_default = 2
opusenc_path = "/home/andrew/bin/opusenc"
cover_names = ['cover.jpg', 'cover.png']

class RunTestsAction(argparse.Action):
    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS, help=None):
        super(RunTestsAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)
    def __call__(self, parser, namespace, values, option_string=None):
        args = sys.argv[:]
        args.remove('--test')
        unittest.main(argv=args)
        parser.exit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action=RunTestsAction, help="run unit tests")
    parser.add_argument("-b", "--bitrate", type=int, default=96, help="opus bitrate in kb/s")
    parser.add_argument("-n", "--dry-run", action='store_true', help="don't do anything")
    parser.add_argument("-v", "--verbose", action='store_true', help="print actions")
    parser.add_argument("-w", "--workers", type=int, default=num_workers_default, help="number of encode workers")
    parser.add_argument("--delete", action='store_true', help="delete files in dest that don't belong")
    parser.add_argument("--exclude", action='append', help="paths to exclude")
    parser.add_argument("source", nargs='+', help="source directories")
    parser.add_argument("dest", help="the destination directory")
    args = parser.parse_args()

    # 1. build source set
    #    the source set is every directory under a source directory
    #    which contains any music files
    #    TODO: and is not excluded

    # {dir name: [source root]}
    dirset = collections.defaultdict(list)
    for sourceroot in args.source:
        for sourcedir, dirs, files in os.walk(sourceroot):
            if should_exclude(sourcedir, args.exclude):
                dirs[:] = []
                continue
            if should_include(sourcedir, files):
                assert sourcedir.startswith(sourceroot)
                dirname = replacepath(sourcedir, sourceroot, '.')
                dirset.setdefault(dirname, []).append(sourceroot)

    # 1b. warn about for duplicate directories
    ignored = []
    for sourcedir in sorted(dirset.keys()):
        roots = dirset[sourcedir]
        if len(roots) > 1:
            print("warning: skipping directory present in multiple sources: %s" % sourcedir)
            for root in roots:
                print("\t(source directory: %s)" % joinpath(root, sourcedir))
            ignored.append(sourcedir)
    for sourcedir in ignored:
        del dirset[sourcedir]

    sourceset = sorted(dirset.keys())

    # 2. emit mkdir actions
    actions = []
    for dir in sourceset:
        destdir = joinpath(args.dest, dir)
        if not os.path.exists(destdir):
            actions.append(mkdir(destdir))

    # 3. sync each directory in the source set with the dest directory
    for dirname in sorted(dirset.keys()):
        root = dirset[dirname]
        sourcedir = joinpath(root[0], dirname)
        destdir = joinpath(args.dest, dirname)

        srcfiles = get_files(sourcedir)
        destfiles = get_files(destdir)

        subactions = sync_dirs(sourcedir, destdir, srcfiles, destfiles, delete=args.delete)
        actions += subactions

    # 4. walk the destination and delete any directory
    # not present in dirset
    if args.delete:
        for destdir, dirs, _ in os.walk(args.dest):
            dirs.sort()
            dirname = replacepath(destdir, args.dest, '.')
            if dirname not in dirset:
                files = get_files(destdir)
                actions += sync_dirs(destdir, destdir, [], files, delete=args.delete)

    # 5. do the actions

    # Start the workers
    workers = []
    q = queue.Queue(args.workers)
    if not args.dry_run:
        for _ in range(args.workers):
            t = threading.Thread(target=worker, args=(q, args))
            t.start()
            workers.append(t)

    # Do each action
    # XXX if mkdir fails, we probably shouldn't try to
    #     transcode anything to that directory
    for action in actions:
        if args.dry_run or args.verbose:
            print(str(action))
        if not args.dry_run:
            if action.action == 'transcode':
                # farm transcode actions out to worker threads
                q.put(action)
            else:
                doaction(action, args)

    # Wait for the queue to empty
    q.join()

    # Stop the workers
    for t in workers:
        q.put(None)
    for t in workers:
        t.join()

def should_exclude(dirname, excludes):
    """Returns true if the directory should be excluded when walking the source tree"""
    if excludes:
        if dirname in excludes:
            return True
    return False

def should_include(dirname, files):
    """Returns true if the directory should be included in the source set."""
    for file in files:
        if file.endswith('.flac'):
            return True
    return False

class File(object):
    def __init__(self, path, name, mtime):
        self.path = path
        self.name = name
        self.mtime = mtime
    def __hash__(self):
        return hash(self.path)
    def __eq__(self, other):
        if not isinstance(other, File):
            return NotImplemented
        return self.path == other.path
    def __repr__(self):
        return 'File(%s, mtime=%s)' % (repr(self.path), repr(self.mtime))

def get_files(path):
    """Return a list of File objects for each file in the given directory."""
    files = []
    path = os.path.normpath(path)
    # XXX os.scandir
    try:
        filenames = os.listdir(path)
    except FileNotFoundError:
        return []
    for filename in filenames:
        filepath = joinpath(path, filename)
        st = os.stat(filepath)
        if stat.S_ISREG(st.st_mode):
            files.append(File(filepath, filename, st.st_mtime))
    files.sort(key=lambda x: x.name)
    return files

def worker(queue, args):
    while True:
        action = queue.get()
        if action is None:
            break
        doaction(action, args)
        queue.task_done()

def doaction(action, args):
    if action.action == 'mkdir':
        domkdir(action, args.dest)
    elif action.action == 'transcode':
        dotranscode(action, args)
    elif action.action == 'copy':
        shutil.copy(action.filepath, action.destpath)
    elif action.action == 'remove':
        doremove(action)
    elif action.action == 'rmdir':
        dormdir(action)
    else:
        print("error: unknown action:", str(action))

def domkdir(action, root):
    """Make all missing directories up to root"""
    dirs = []
    path = os.path.normpath(action.destpath)
    root = os.path.normpath(root)
    while path != root and not os.path.exists(path):
        path, dir = os.path.split(path)
        dirs.append(dir)
    while dirs:
        dir = dirs.pop()
        path = joinpath(path, dir)
        try:
            os.mkdir(path)
        except FileExistsError:
            pass
        except OSError as e:
            print("error: mkdir failed:", e)
            break

def doremove(action):
    """Remove a file"""
    path = os.path.normpath(action.destpath)
    try:
        os.remove(path)
    except FileNotFoundError:
        # it's already gone?
        pass
    except OSError as e:
        print("error: remove failed:", e)

def dormdir(action):
    """Remove a directory"""
    path = os.path.normpath(action.destpath)
    try:
        os.rmdir(path)
    except FileNotFoundError:
        # it's already gone?
        pass
    except OSError as e:
        print("error: remove failed:", e)


def dotranscode(action, args):
    if action.action != 'transcode':
        print('error: dotranscode got non-transcode action:', action)
        return

    # TODO: make sure the directory exists

    # Instead of writing directly to $destpath, write to $destpath.partial,
    # so that if we crash we don't leave partially-encoded files laying around
    tmppath = action.destpath + ".partial"
    cmd = [opusenc_path, '--quiet', '--bitrate', str(args.bitrate), action.filepath, tmppath]
    kwargs = dict(
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    with subprocess.Popen(cmd, **kwargs) as process:
        try:
            _, stderr = process.communicate()
        except:
            process.kill()
            process.wait()
            raise
        returncode = process.poll()

    if returncode != 0:
        print("error: transcode failed:", stderr.decode('utf-8', 'replace').strip())
        print("info: failed command:", " ".join(cmd))
        return

    try:
        os.replace(tmppath, action.destpath)
    except OSError as e:
        print("error: rename failed: %s: %s" % (action.destpath, e))
        return

def sync_dirs(srcdir, destdir, srcfiles, destfiles, delete=False):
    """Return a list of actions to transcode files from sourcedir to destdir

    To delete a directory, pass an empty srcfiles.
    """
    actions = []
    srcmap = {file.name: file for file in srcfiles}
    destmap = {file.name: file for file in destfiles}
    destset = set()

    # Transcode .flac files
    for file in srcfiles:
        if file.path.endswith('.flac'):
            destname = replace_ext(file.name, '.flac', '.opus')
            destpath = joinpath(destdir, destname)
            destset.add(destname)
            if destname not in destmap or destmap[destname].mtime < file.mtime:
                actions.append(transcode(file.path, destpath))

    # Copy cover file
    for name in cover_names:
        if name in srcmap:
            file = srcmap[name]
            destpath = joinpath(destdir, file.name)
            destset.add(file.name)
            if name not in destmap or destmap[name].mtime < file.mtime:
                actions.append(copy(file.path, destpath))
            break

    # Clean up the destination directory
    # by deleting files which are not in the destset.
    # To play it safe, we'll only delete files that we could
    # have conceivably created in the first place.
    #
    # - .opus files
    # - .opus.partial files
    # - cover files
    if delete:
        for file in destfiles:
            if file.name not in destset and can_delete(file.name):
                actions.append(remove(file.path))

        # delete the directory if
        # 1) we're deleting something
        # 2) we aren't creating anything

        # XXX but what we actually want to do is
        #     delete the directory if the source dir doesn't exist
        if actions and not destset:
            actions.append(rmdir(destdir))

    return actions

def can_delete(filename):
    if filename.endswith(".opus.partial"):
        return True
    if filename.endswith(".opus"):
        return True
    if filename in cover_names:
        return True
    return False

class Action(object):
    def __init__(self, action, filepath, destpath):
        self.action = action
        self.filepath = filepath
        self.destpath = destpath
    def __eq__(self, other):
        if not isinstance(other, Action):
            return NotImplemented
        return (self.action == other.action and
                self.filepath == other.filepath and
                self.destpath == other.destpath)
    def __repr__(self):
        return "Action(%r, %r, %r)" % (self.action, self.filepath, self.destpath)
    def __str__(self):
        if self.filepath:
            return "%s %s to %s" % (self.action, self.filepath, self.destpath)
        return "%s %s" % (self.action, self.destpath)

def transcode(filepath, destpath):
    return Action('transcode', filepath, destpath)

def copy(filepath, destpath):
    return Action('copy', filepath, destpath)

def mkdir(path):
    return Action('mkdir', "", path)

def remove(path):
    return Action('remove', "", path)

def rmdir(path):
    return Action('rmdir', "", path)

def replace_ext(path, old, new):
    """Replaces the file extension of a path.

    If path does not end with the old file extension, simply append the new file extenson.
    """
    if path.endswith(old):
        path = path[:len(path)-len(old)]
    return path + new

def replacepath(path, old, new):
    old = os.path.normpath(old)
    new = os.path.normpath(new)
    path = os.path.normpath(path)
    if path == old:
        return new
    elif path.startswith(old+os.sep):
        path = path[len(old+os.sep):]
        return joinpath(new, path)
    elif old == '.':
        return joinpath(new, path)
    else:
        raise ValueError("path does not start with %s: %s" % (old, path))

def joinpath(a, *p, sep=os.sep):
    """Join two or more pathname components.

    Unlike os.path.join, joinpath doesn't treat absolute paths specially,
    so joinpath("a", "/b") is "a/b", not "/b".
    """
    path = a
    for b in p:
        if not path or path.endswith(sep):
            path += b
        else:
            path += sep + b
    return os.path.normpath(path)

class TestCase(unittest.TestCase):
    def test_should_include(self):
        self.assertTrue(should_include('a', ['foo.flac']))
        self.assertFalse(should_include('a', ['']))
        self.assertFalse(should_include('a', ['foo.opus']))
        self.assertFalse(should_include('a', ['foo.mp3']))
        self.assertTrue(should_include('a', ['cover.png', 'foo.flac']))
        self.assertFalse(should_include('a', ['cover.png', 'foo.mp3']))

    def test_sync_dirs(self):
        def file(path, mtime=0):
            return File(path, os.path.basename(path), mtime=mtime)

        def test(msg, srcfiles, dstfiles, actions, *, delete=True):
            with self.subTest(msg):
                self.assertEqual(sync_dirs('a', 'b', srcfiles, dstfiles, delete=delete), actions)

        test('transcodes flac files',
            [file('a/foo.flac')],
            [],
            [transcode("a/foo.flac", "b/foo.opus")],
        )

        test('does not transcode or copy mp3 files',
            [file('a/foo.mp3')],
            [],
            [],
        )

        test('copies cover art',
            [file('a/foo.flac'), file('a/cover.png')],
            [],
            [transcode("a/foo.flac", "b/foo.opus"), copy("a/cover.png", "b/cover.png"),]
        )

        test('prefers cover.jpg to cover.png',
            [file('a/cover.png'), file('a/cover.jpg')],
            [],
            [copy("a/cover.jpg", "b/cover.jpg")],
        )

        test('deletes opus files, partial files, and cover art',
            [],
            [file('b/foo.opus'), file('b/bar.opus.partial'), file('b/cover.jpg')],
            [remove("b/foo.opus"), remove("b/bar.opus.partial"),
             remove("b/cover.jpg"), rmdir('b')],
        )

        test('deletes nothing when delete=False',
            [],
            [file('b/foo.opus'), file('b/bar.opus.partial'), file('b/cover.jpg')],
            [],
            delete=False,
        )

        test('transcode: source is newer than dest -> keep',
            [file('a/foo.flac', 2015)],
            [file('b/foo.opus', 2002)],
            [transcode('a/foo.flac', 'b/foo.opus')],
        )

        test('transcode: source is older than dest -> drop',
            [file('a/foo.flac', 2002)],
            [file('b/foo.opus', 2015)],
            [],
        )

        test('transcode: source is same age as dest -> drop',
            [file('a/foo.flac', 2015)],
            [file('b/foo.opus', 2015)],
            [],
        )

        test('copy: source is newer than dest -> keep',
            [file('a/cover.png', 2015)],
            [file('b/cover.png', 2002)],
            [copy('a/cover.png', 'b/cover.png')],
        )

        test('copy: source is older than dest -> drop',
            [file('a/cover.png', 2002)],
            [file('b/cover.png', 2015)],
            [],
        )

        test('copy: source is older than dest -> drop',
            [file('a/cover.png', 2015)],
            [file('b/cover.png', 2015)],
            [],
        )

        # TODO: test with delete=False

    def test_joinpath(self):
        if os.sep != '/':
            self.skipTest("only works when os.sep=='/'")
        self.assertEqual(joinpath("a", "b"), "a/b")
        self.assertEqual(joinpath("a/", "b"), "a/b")
        self.assertEqual(joinpath("a", "/b"), "a/b")
        self.assertEqual(joinpath("/a", "b"), "/a/b")
        self.assertEqual(joinpath("/a", "/b"), "/a/b")
        self.assertEqual(joinpath(".", "a"), "a")
        self.assertEqual(joinpath("a", "."), "a")
        self.assertEqual(joinpath("a", "/b"), "a/b")

    def test_replacepath(self):
        if os.sep != '/':
            self.skipTest("only works when os.sep=='/'")
        self.assertEqual(replacepath("a/foo", "a", "b"), "b/foo")
        self.assertEqual(replacepath("./foo", ".", "b"), "b/foo")
        self.assertEqual(replacepath("a/foo", "a", "."), "foo")

if __name__ == '__main__':
    main()
