# opusdir - transcode a directory tree from FLAC to Opus

import argparse
import os
import shutil
import subprocess
import sys
import unittest

cover_names = ['cover.jpg', 'cover.png']

class RunTestsAction(argparse.Action):
    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS, help=None):
        super(RunTestsAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs='*',
            help=help)
    def __call__(self, parser, namespace, values, option_string=None):
        args = sys.argv[:]
        args.remove('--test')
        unittest.main(argv=args)
        parser.exit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action=RunTestsAction, help="run unit tests")
    parser.add_argument("-n", "--dry-run", action='store_true', help="don't do anything")
    parser.add_argument("-b", "--bitrate", type=int, default=96, help="opus bitrate in kb/s")
    parser.add_argument("source", help="the source directory")
    parser.add_argument("dest", help="the destination directory")
    args = parser.parse_args()

    actions = []
    for sourcedir, dirs, files in os.walk(args.source):
        dirs.sort() # traverse dirs in alphabetical order
        files.sort()
        destdir = replacepath(dirname, args.source, args.dest)
        subactions = dodir(sourcedir, destdir, files)
        if subactions and not os.path.exists(destdir):
            actions.append(mkdir(destdir))
        actions += subactions

    if args.dry_run:
        for action in actions:
            print(action)
        return

    for action in actions:
        doaction(action, args)

def doaction(action, args):
    if action.action == 'mkdir':
        dirs = []
        path = action.destpath
        while path != args.source and not os.path.exists(path):
            path, dir = os.path.split(path)
            dirs.append(dir)
        while dirs:
            dir = dirs.pop()
            path = joinpath(path, dir)
            try:
                os.mkdir(path)
            except Exception as e:
                print("error: mkdir failed:", e)
                break
    elif action.action == 'transcode':
        # TODO: make sure the directory exists
        cmd = ['opusenc', '--bitrate', str(args.bitrate), action.filepath, action.destpath]
        returncode = subprocess.call(cmd, stderr=subprocess.DEVNULL)
        if returncode != 0:
            print("error: command failed:", " ".join(command))
    elif action.action == 'copy':
        shutil.copy(action.filepath, action.destpath)
    else:
        print("error: unknown action:", str(action))

def dodir(sourcedir, destdir, files):
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
            return "would %s %s to %s" % (self.action, self.filepath, self.destpath)
        return "would %s %s" % (self.action, self.destpath)

def transcode(filepath, destpath):
    return Action('transcode', filepath, destpath)

def copy(filepath, destpath):
    return Action('copy', filepath, destpath)

def mkdir(path):
    return Action('mkdir', "", path)

def replacepath(path, old, new):
    if path.startswith(old):
        return joinpath(new, path[len(old):])
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
        self.assertEqual(dodir('a', 'transcode/a', ['foo.flac']),
            [ transcode("a/foo.flac", "transcode/a/foo.opus") ])
        self.assertEqual(dodir('a', 'transcode/a', ['foo.mp3']), [])
        self.assertEqual(dodir('a', 'transcode/a', ['foo.flac', 'cover.png']),
            [ transcode("a/foo.flac", "transcode/a/foo.opus"), copy("a/cover.png", "transcode/a/cover.png") ])

if __name__ == '__main__':
    main()
