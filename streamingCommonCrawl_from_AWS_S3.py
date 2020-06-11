import boto
import boto3
import botocore
from boto.s3.key import Key
from gzipstream import GzipStreamFile
import warc
import gzip
import logging as LOG
import os.path as Path

from tempfile import TemporaryFile
if __name__ == '__main__':
  # Let's use a random gzipped web archive (WARC) file from the 2014-15 Common Crawl dataset
  ## Connect to Amazon S3 using anonymous credentials
  boto_config = botocore.client.Config(
                signature_version=botocore.UNSIGNED,
                read_timeout=180,
                retries={'max_attempts' : 20})
  s3client = boto3.client('s3', config=boto_config)

  line = 'cc-index/collections/CC-MAIN-2018-39/indexes/cdx-00010.gz'
  boto_config = botocore.client.Config(
                  signature_version=botocore.UNSIGNED,
                  read_timeout=180,
                  retries={'max_attempts' : 20})
  s3client = boto3.client('s3', config=boto_config)
  # Verify bucket
  try:
      s3client.head_bucket(Bucket='commoncrawl')
  except botocore.exceptions.ClientError as exception:
      LOG.error('Failed to access bucket "commoncrawl": %s', exception)
      
  # Check whether WARC/WAT/WET input exists
  try:
      s3client.head_object(Bucket='commoncrawl',
                          Key=line)
  except botocore.client.ClientError as exception:
      LOG.error('Input not found: %s', line)
      
  # Start a connection to one of the WARC/WAT/WET files
  LOG.info('Loading s3://commoncrawl/%s', line)
  try:
      temp = TemporaryFile(mode='w+b',
                          dir='C:\Users\Aditya\Documents\demonstrational\Radii Corporation\common-crawl-extractor')
      s3client.download_fileobj('commoncrawl', line, temp)
  except botocore.client.ClientError as exception:
      LOG.error('Failed to download %s: %s', line, exception)
      
  temp.seek(0)
  
  # The warc library accepts file like objects, so let's use GzipStreamFile
  ccfile = warc.WARCFile(fileobj=(GzipStreamFile(temp)))
  for num, record in enumerate(ccfile):
    if record['WARC-Type'] == 'response':
      # Imagine we're interested in the URL, the length of content, and any Content-Type strings in there
      print(record['WARC-Target-URI'], record['Content-Length'])
      print ('\n'.join(x for x in record.payload.read().replace('\r', '').split('\n\n')[0].split('\n') if 'content-type:' in x.lower()))
      print ('=-=-' * 10)
    if num > 100:
      break