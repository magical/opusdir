# opusdir - transcode a directory tree from FLAC to Opus

import argparse
import os

cover_names = ['cover.jpg', 'cover.png']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--dry-run", help="don't do anything", default=False)
    parser.add_argument("-b", "--bitrate", default=96000, help="opus bitrate")
    parser.add_argument("source", help="the source directory")
    parser.add_argument("dest", help="the destination directory")
    args = parser.parse_args()

    for dirname, dirs, files in os.walk(args.source):
        has_music = False
        dirs.sort()
        files.sort()
        destdir = replacepath(dirname, args.source, args.dest)

        for filename in files:
            if filename.endswith('.flac'):
                has_music = True
                basename, ext = os.path.splitext(filename)
                filepath = joinpath(dirname, filename)
                destpath = joinpath(destdir, basename + ".opus")
                transcode(filepath, destpath)

        if has_music:
            for name in cover_names:
                if name in files:
                    filepath = joinpath(dirname, name)
                    destpath = joinpath(destdir, name)
                    copy(filepath, destpath)
                    break

def transcode(filepath, destpath):
    print("would transcode %s to %s" % (filepath, destpath))

def copy(filepath, destpath):
    print("would copy %s to %s" % (filepath, destpath))

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

if __name__ == '__main__':
    main()
