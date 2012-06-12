#!/usr/bin/env python

from time import time
from time import sleep
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from uuid import uuid4
from random import choice
from os import listdir
import optparse
import sys

bucketcount = None
objcount = None
noclean = None
cleanall = None
iterations = None

parser = optparse.OptionParser()
parser.add_option('--access', '-a', action="store")
parser.add_option('--secret', '-s', action="store")
parser.add_option('--buckets', '-b', default=1, action="store", type="int")
parser.add_option('--iterations', '-i', default=1, action="store", type="int")
parser.add_option('--host', default='objects.dreamhost.com', action="store")
parser.add_option('--objects', '-o', default=100, action="store", type="int")
parser.add_option('--noclean', action="store_true", default=False);
parser.add_option('--cleanall', action="store_true", default=False);

options, remainder = parser.parse_args()

conn = S3Connection(options.access,options.secret,host=options.host)

bucketcount = options.buckets
objcount = options.objects
noclean = options.noclean
cleanall = options.cleanall
iterations = options.iterations

data = []
for size in (1024,10240,102400,1024000,10240000):
  f = open('/dev/urandom','r')
  data.append(f.read(size))

buckets = []

def makeBucket():
  uuid = uuid4()
  print uuid
  bucket = conn.create_bucket(str(uuid))
  buckets.append(bucket)
  return bucket

def fillBucket():
  print 'creating bucket'
  bucket = makeBucket()
  print 'populating ' + str(bucket)
  starttime = time()
  for count in range(0,objcount):
    k = Key(bucket)
    uuid = uuid4()
    k.key = str(uuid)
    k.set_contents_from_string(choice(data))
  endtime = time()
  duration = int(endtime) - int(starttime)
  print 'Created ' + str(objcount) + ' objects in ' + str(duration) + ' seconds'
  print str(bucket) + ' contains ' + str(len(bucket.get_all_keys())) + ' objects'

def cleanup():
  bs = None
  if cleanall: 
    bs = conn.get_all_buckets()
  else:
    bs = buckets
  for bucket in bs:
    keys = bucket.list()
    for key in keys:
      print key
      bucket.delete_key(key)
    tries = 1
    while tries <= 3:
      try:
        conn.delete_bucket(bucket)
      except S3ResponseError:
        print "Bucket deletion failed!"
        tries += 1
        sleep(10)
      else:
        print "Bucket deleted successfully after " + str(tries) + " attempt(s)"
        return

for i in range(0,iterations):
  if not cleanall:
    for count in range(0,int(bucketcount)):
      fillBucket()
  if not noclean : cleanup()
