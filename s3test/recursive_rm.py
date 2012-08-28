from boto.s3.connection import S3Connection
import optparse
import sys

parser = optparse.OptionParser()
parser.add_option('--access', '-a', action='store')
parser.add_option('--secret', '-s', action='store')
parser.add_option('--bucket', '-b', action='store')
parser.add_option('--allbuckets', '-A', action='store_true')

parser.add_option('--host', default='objects.dreamhost.com', action='store')

options, remainder = parser.parse_args()

conn = S3Connection(options.access, options.secret, host=options.host)

buckets = ()

if options.bucket:
    try:
        bucket = conn.get_bucket(options.bucket)
    except:
        print 'Error: Cannot find bucket ' + options.bucket
        sys.exit()
    buckets = bucket
elif options.allbuckets:
    buckets = conn.get_all_buckets()

for bucket in buckets:
    for key in bucket.list():
        print 'deleting ' + key.name
        #bucket.delete_key(key)

    print 'deleting bucket ' + bucket.name
    #conn.delete_bucket(bucket)

print 'Done.'
