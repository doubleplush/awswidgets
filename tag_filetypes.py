#!/usr/bin/env python3

import argparse
import logging
import os
import re

import boto3
import botocore

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket')
    parser.add_argument('prefix', default='')

    args = parser.parse_args()

    return args


class S3Helper():

    def __init__(self, bucket):
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.bucket = self.s3.Bucket(name=bucket)

    def update_object_tag(self, obj, key, value):
        response = self.s3_client.get_object_tagging(
            Bucket=self.bucket.name,
            Key=obj.key,
        )
        tags = {tag['Key']:tag['Value'] for tag in response['TagSet']}
        changed = False
        if tags.get(key) != value:
            changed = True
            tags[key] = value
            response = self.s3_client.put_object_tagging(
                Bucket=self.bucket.name,
                Key=obj.key,
                Tagging=dict(
                    TagSet=[{'Key': k, 'Value': v} for k, v in tags.items()]
                )
            )

        return changed, tags


def main():
    args = get_args()

    s3_helper = S3Helper(bucket=args.bucket)

    found_objects = False
    for s3_obj in s3_helper.bucket.objects.filter(Prefix=args.prefix):
        found_objects = True
        filetype = os.path.splitext(s3_obj.key)[1].strip('.').lower()
        print(s3_obj.key)
        try:
            s3_helper.update_object_tag(s3_obj, 'filetype', filetype)
        except botocore.exceptions.ClientError as e:
            import pdb; pdb.set_trace()
            print('Error on key %s: %s' % (s3_obj.key, e.msg))
            continue

    if not found_objects:
        raise IOError('No objects with prefix %s found!' % args.prefix)


if __name__ == '__main__':
    main()
