#!/usr/bin/env python

from time import time
from time import sleep
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from uuid import uuid4
from random import choice
import optparse
import sys
import os

default_sizes = '1024,10240,102400,1024000,10240000'

parser = optparse.OptionParser()
parser.add_option('--access', '-a', action='store')
parser.add_option('--secret', '-s', action='store')
parser.add_option('--sizes', '-S', default=default_sizes,
                  action='store', type='string')
parser.add_option('--buckets', '-b', default=1, action='store', type='int')
parser.add_option('--iterations', '-i', default=1, action='store', type='int')
parser.add_option('--host', default='objects.dreamhost.com', action='store')
parser.add_option('--objects', '-o', default=100, action='store', type='int')
parser.add_option('--noclean', action='store_true', default=False)
parser.add_option('--verbose', '-v', action='store_true', default=False)
parser.add_option('--cleanall', action='store_true', default=False)
parser.add_option('--readtest', action='store_true', default=False)
parser.add_option('--bucket', action='store')
parser.add_option('--file', '-f', action='store')
parser.add_option('--objectcopy', action='store')

options, remainder = parser.parse_args()

if options.bucket and options.buckets != 1:
    parser.error('options --bucket and --buckets are mutually exclusive')

if options.file and options.sizes != default_sizes:
    parser.error('options --file and --sizes are mutually exclusive')

if options.file:
    try:
        file = open(options.file, 'r')
    except IOError:
        print 'File ' + options.file + ' does not exist'
        sys.exit()
    filesize = os.path.getsize(options.file)
else:
    file = None

conn = S3Connection(options.access, options.secret, host=options.host)

data = []
buckets = []
keys = {}

for size in options.sizes.split(','):
    f = open('/dev/urandom', 'r')
    data.append(f.read(int(size)))


def _make_bucket():
    uuid = uuid4()
    print uuid
    bucket = conn.create_bucket(str(uuid))
    buckets.append(bucket)
    return bucket


def _fill_bucket():
    size_uploaded = 0
    if options.bucket:
        try:
            bucket = conn.get_bucket(options.bucket)
        except:
            print 'Error: Cannot find bucket ' + options.bucket
            sys.exit()
    else:
        bucket = _make_bucket()
    if bucket:
        print 'populating ' + str(bucket)
    starttime = time()
    k = Key(bucket)
    k.key = 's3exerciserbucket'
    k.set_contents_from_string('s3exerciserbucket')
    for count in range(0, options.objects):
        k = Key(bucket)
        uuid = uuid4()
        k.key = str(uuid)
        if file:
            k.set_contents_from_file(file)
            size_uploaded += filesize
        else:
            keydata = choice(data)
            k.set_contents_from_string(keydata)
            size_uploaded += len(keydata)
    endtime = time()
    duration = int(endtime) - int(starttime)
    print 'Created ' + str(options.objects) + ' objects (' +\
        str(size_uploaded) + ' bytes) in ' + str(duration) + ' seconds'
    return True


def _object_copy(key):
    try:
        newkey = uuid4()
        conn.copy_key(newkey, options.bucket, key)
    except Exception:
        print 'cannot copy ' + key + ' to ' + newkey


def _download_test():
    for bucket in buckets:
        starttime = time()
        size_downloaded = 0
        read = 0
        for key in bucket.list():
            if options.verbose:
                print 'attempting fetch of key ' + str(key)
            if str(key).endswith(',s3exerciserbucket>'):
                continue
            k = bucket.get_key(key)
            data = k.get_contents_as_string()
            size_downloaded += len(data)
            read += 1
    endtime = time()
    duration = int(endtime) - int(starttime)
    print 'Read ' + str(read) + ' objects (' + str(size_downloaded) +\
        ' bytes) in ' + str(duration) + ' seconds'


def _cleanup():
    bs = None
    print 'Cleaning up'
    cleaned = 0
    skipped = 0
    if options.cleanall:
        if options.bucket:
            try:
                b = conn.get_bucket(options.bucket)
                bs = [b]
            except S3ResponseError:
                print 'Error: Cannot find bucket ' + options.bucket
                sys.exit()
        else:
            bs = conn.get_all_buckets()
    else:
        bs = buckets
        for bucket in bs:
            if bucket.get_key('s3exerciserbucket'):
                cleaned += 1
                print ' Cleaning ' + str(bucket)
                bucket_keys = bucket.list()
                for key in bucket_keys:
                    if options.verbose:
                        print key
                    bucket.delete_key(key)
                    tries = 1
                    while tries <= 3:
                        try:
                            conn.delete_bucket(bucket)
                        except S3ResponseError:
                            print 'Bucket deletion failed!'
                        tries += 1
                        sleep(10)
                    else:
                        print 'Bucket deleted successfully after ' + \
                            str(tries) + ' attempt(s)'
                        break
            else:
                skipped += 1
    print 'cleaned ' + str(cleaned) + ' buckets'
    print 'skipped ' + str(skipped) + ' buckets'
    return True

for i in range(0, options.iterations):
    if not options.cleanall:
        if options.objectcopy:
            _object_copy(options.objectcopy)
        else:
            for count in range(0, int(options.buckets)):
                ret = _fill_bucket()
                if not ret:
                    print '_fill_bucket() failed, bailing'
                    if not options.noclean:
                        _cleanup()
                    sys.exit()
    if options.readtest:
        _download_test()
    if not options.noclean:
        _cleanup()
