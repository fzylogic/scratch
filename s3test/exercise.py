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

parser = optparse.OptionParser()
parser.add_option('--access', '-a', action='store')
parser.add_option('--secret', '-s', action='store') 
parser.add_option('--sizes', '-S', default = '1024,10240,102400,1024000,10240000', action='store', type='string') 
parser.add_option('--buckets', '-b', default=1, action='store', type='int')
parser.add_option('--iterations', '-i', default=1, action='store', type='int')
parser.add_option('--host', default='objects.dreamhost.com', action='store')
parser.add_option('--objects', '-o', default=100, action='store', type='int')
parser.add_option('--noclean', action='store_true', default=False)
parser.add_option('--verbose', '-v', action='store_true', default=False)
parser.add_option('--cleanall', action='store_true', default=False)
parser.add_option('--readtest', action='store_true', default=False)

options, remainder = parser.parse_args()

conn = S3Connection(options.access,options.secret,host=options.host)

data = []
buckets = []
keys = {}

for size in options.sizes.split(','):
  f = open('/dev/urandom','r')
  data.append(f.read(int(size)))

	
def makeBucket():
  uuid = uuid4()
  print uuid
  bucket = conn.create_bucket(str(uuid))
  buckets.append(bucket)
  return bucket

def fillBucket():
  size_uploaded = 0
  bucket = makeBucket()
  print 'populating ' + str(bucket)
  starttime = time()
  k = Key(bucket)
  k.key = 's3exerciserbucket'
  k.set_contents_from_string('s3exerciserbucket')
  for count in range(0,options.objects):
    k = Key(bucket)
    uuid = uuid4()
    k.key = str(uuid)
    keydata = choice(data)
    k.set_contents_from_string(keydata)
    size_uploaded += len(keydata)
  endtime = time()
  duration = int(endtime) - int(starttime)
  print 'Created ' + str(options.objects) + ' objects (' + str(size_uploaded) + ' bytes) in ' + str(duration) + ' seconds'

def downloadTest():
  for bucket in buckets:
    starttime = time()
    size_downloaded = 0
    for key in bucket.list():
      if options.verbose: print 'attempting fetch of key ' + key
      k = bucket.get_key(key)
      data = k.get_contents_as_string()
      size_downloaded += len(data)
    endtime = time()
    duration = int(endtime) - int(starttime)
    print 'Read ' + str(to_read) + ' objects (' + str(size_downloaded) + ' bytes) in ' + str(duration) + ' seconds'

def cleanup():
  bs = None
  print 'Cleaning up'
  cleaned = 0
  skipped = 0
  if options.cleanall:
    bs = conn.get_all_buckets()
  else:
    bs = buckets
  for bucket in bs:
    if bucket.get_key('s3exerciserbucket'):
      cleaned += 1
      print ' Cleaning ' + str(bucket)
      bucket_keys = bucket.list()
      for key in bucket_keys:
        if options.verbose: print key
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
          print 'Bucket deleted successfully after ' + str(tries) + ' attempt(s)'
          break
    else:
      skipped += 1
  print 'cleaned ' + str(cleaned) + ' buckets'
  print 'skipped ' + str(skipped) + ' buckets'

for i in range(0,options.iterations):
  if not options.cleanall:
    for count in range(0,int(options.buckets)):
      fillBucket()
  if options.readtest:
    downloadTest()
  if not options.noclean : cleanup()
