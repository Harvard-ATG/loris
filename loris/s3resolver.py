# -*- coding: utf-8 -*-
from loris.resolver import _AbstractResolver
from urllib import unquote
from loris_exception import ResolverException
import os
import boto
import logging

logger = logging.getLogger(__name__)


class S3Resolver(_AbstractResolver):
    '''
    Resolver for images coming from AWS S3 bucket.
    The config dictionary MUST contain
     * `cache_root`, which is the absolute path to the directory where source images
        should be cached.
     * `s3_bucket`, which is the name of the s3 bucket that stores the images
    '''
    def __init__(self, config):
        super(S3Resolver, self).__init__(config)
	
        logger.debug('loaded s3resolver with config: %s' % config)

        if 'cache_root' in self.config:
            self.cache_root = self.config['cache_root']
        else:
            message = 'Server Side Error: Configuration incomplete and cannot resolve. Missing setting for cache_root.'
            logger.error(message)
            raise ResolverException(500, message)

        if 's3buckets' in self.config:
            self.s3buckets = self.config['s3buckets'].split(',')
        else:
            message = 'Server Side Error: Configuration incomplete and cannot resolve. Missing setting for s3buckets.'
            logger.error(message)
            raise ResolverException(500, message)

    @staticmethod
    def format_from_ident(ident):
        return ident.split('.')[-1]

    @staticmethod
    def create_directory_if_not_exists(path):
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            logger.debug("Attempting to create directories for %s" % directory)
            # Doc claims that dir mode defaults to 0777, but in practice the mode
            # seems to take on that of the parent directory, which in this case is 0755.
            # Setting it to 0755 anyways, just in case.
            os.makedirs(directory, 0755)  # Could throw an exception, but what do we do with it?
        else:
            logger.debug("Directory exists for %s" % directory)

    def is_resolvable(self, ident):
        ident = unquote(ident)
        local_fp = os.path.join(self.cache_root, ident)
        if os.path.exists(local_fp):
            return True
        else:
            # check that we can get to this object on S3
            s3 = boto.connect_s3()

            bucketname, keyname = self._s3bucket_from_ident(ident)
            try:
                bucket = s3.get_bucket(bucketname)
            except boto.exception.S3ResponseError as e:
                logger.error(e)
                return False

            if bucket.get_key(keyname):
                return True
            else:
                logger.debug('AWS key %s does not exist in bucket %s' % (keyname, bucketname))
                return False

    def resolve(self, ident):
        ident = unquote(ident)
        local_fp = os.path.join(self.cache_root, ident)
        logger.debug('ident: %s local_fp: %s' % (ident, local_fp))
        format = self.format_from_ident(ident)
        logger.debug('src format %s' % (format,))

        bucketname, keyname = self._s3bucket_from_ident(ident)

        if os.path.exists(local_fp):
            logger.debug('src image from local disk: %s' % (local_fp,))
            return (local_fp, format)
        else:
            # get image from S3
            logger.debug('Getting img from AWS S3. bucketname, keyname: %s, %s' % (bucketname, keyname))

            s3 = boto.connect_s3()
            bucket = s3.get_bucket(bucketname)
            key = bucket.get_key(keyname)

            # Need local_fp directory to exist before writing image to it
            self.create_directory_if_not_exists(local_fp)
            try:
                key.get_contents_to_filename(local_fp)
            except boto.exception.S3ResponseError as e:
                logger.warn(e)

            return (local_fp, format)

    def _s3bucket_from_ident(self, ident):
        key_parts = ident.split('/', 1)
        if len(key_parts) != 2:
            raise ResolverException(500, 'Invalid identifier. Expected bucket to prefix the identifier: bucket/ident')

        (bucketname, keyname) = key_parts

        if bucketname not in self.s3buckets:
            raise ResolverException(500, 'Invalid bucket. Must be one of: %s' % self.s3buckets)

        return bucketname, keyname
