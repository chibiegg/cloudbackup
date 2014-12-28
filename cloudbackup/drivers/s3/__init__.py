# encoding=utf-8

from .writer import MultiPartUploader

def get_writer(bucket, keyname, part_size, num_threads):
    return MultiPartUploader(bucket, keyname, part_size, num_threads)

