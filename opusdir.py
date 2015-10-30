# opusdir - transcode a directory tree from FLAC to Opus
# requires Python 3.3

# TODO: don't encode files which are newer than the source file

import argparse
import os
import queue
import shutil
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
    parser.add_argument("source", help="the source directory")
    parser.add_argument("dest", help="the destination directory")
    args = parser.parse_args()

    actions = []
    for sourcedir, dirs, files in os.walk(args.source):
        dirs.sort() # traverse dirs in alphabetical order
        files.sort()
        destdir = replacepath(sourcedir, args.source, args.dest)
        subactions = get_transcode_actions_for_dir(sourcedir, destdir, files)
        if subactions and not os.path.exists(destdir):
            actions.append(mkdir(destdir))
        actions += subactions

    workers = []

    # Start the workers
    q = queue.Queue(args.workers)
    if not args.dry_run:
        for _ in range(args.workers):
            t = threading.Thread(target=worker, args=(q, args))
            t.start()
            workers.append(t)

    # Do each action
    for action in actions:
        if args.dry_run or args.verbose:
            print(str(action))
        if not args.dry_run:
            doaction(action, q, args)

    # Wait for the queue to empty
    q.join()

    # Stop the workers
    for t in workers:
        q.put(None)
    for t in workers:
        t.join()

def worker(queue, args):
    while True:
        action = queue.get()
        if action is None:
            break
        dotranscode(action, args)
        queue.task_done()

def doaction(action, queue, args):
    if action.action == 'mkdir':
        domkdir(action, args.source)
    elif action.action == 'transcode':
        queue.put(action)
    elif action.action == 'copy':
        shutil.copy(action.filepath, action.destpath)
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

def dotranscode(action, args):
    if action.action != 'transcode':
        print('error: dotranscode got non-transcode action:', action)
        return

    # TODO: make sure the directory exists

    # Instead of writing directly to $destpath, write to $destpath.partial,
    # so that if we crash we don't leave partially-encoded files laying around
    tmppath = action.destpath + ".partial"
    cmd = [opusenc_path, '--quiet', '--bitrate', str(args.bitrate), action.filepath, tmppath]
    returncode = subprocess.call(cmd, stderr=subprocess.DEVNULL)
    if returncode != 0:
        # TODO: get stderr
        print("error: command failed:", " ".join(cmd))
        return

    try:
        os.replace(tmppath, action.destpath)
    except OSError as e:
        print("error: rename failed: %s: %s" % action.destpath, e)
        return

def get_transcode_actions_for_dir(sourcedir, destdir, files):
    """Return a list of actions to transcode files from sourcedir to destdir"""
    actions = []
    has_music = False
    for name in files:
        if name.endswith('.flac'):
            has_music = True
            basename, ext = os.path.splitext(name)
            filepath = joinpath(sourcedir, name)
            destpath = joinpath(destdir, basename + ".opus")
            actions.append(transcode(filepath, destpath))

    if has_music:
        for name in cover_names:
            if name in files:
                filepath = joinpath(sourcedir, name)
                destpath = joinpath(destdir, name)
                actions.append(copy(filepath, destpath))
                break

    return actions

class Action(object):
    def __init__(self, action, filepath, destpath):
        self.action = action
        self.filepath = filepath
        self.destpath = destpath
    def __eq__(self, other):
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

def replacepath(path, old, new):
    old = os.path.normpath(old)
    new = os.path.normpath(new)
    path = os.path.normpath(path)
    if path == old:
        return new
    elif path.startswith(old+"/"):
        path = path[len(old+"/"):]
        return joinpath(new, path)
    else:
        raise ValueError("path does not start with %s: %s" % (old, path))

def joinpath(a, *p):
    """Join two or more pathname components. Unlike os.path.join, doesn't treat absolute paths specially. Also normalizes the path after joining.

    In other words, joinpath("a", "/b") is "a/b", not "/b".
    """
    sep = os.path.sep
    path = a
    for b in p:
        if not path or path.endswith(sep):
            path += b
        else:
            path += sep + b
    return path
    #return os.path.normpath(path)

class TestCase(unittest.TestCase):
    def test(self):
        dodir = get_transcode_actions_for_dir
        self.assertEqual(dodir('a', 'transcode/a', ['foo.flac']),
            [ transcode("a/foo.flac", "transcode/a/foo.opus") ])
        self.assertEqual(dodir('a', 'transcode/a', ['foo.mp3']), [])
        self.assertEqual(dodir('a', 'transcode/a', ['foo.flac', 'cover.png']),
            [ transcode("a/foo.flac", "transcode/a/foo.opus"), copy("a/cover.png", "transcode/a/cover.png") ])

if __name__ == '__main__':
    main()
