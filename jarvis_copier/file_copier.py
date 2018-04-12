import shutil
import arrow
import os
from argparse import ArgumentParser
from jarvis_copier.file_util import *

formatted_date_str = arrow.utcnow().format("YYYYMMDD-HHmmss")


def copy_file(args):
    source_str = '{}/{}.csv'.format(args.source_dir, args.source_filename)
    target_str = '{}/{}-{}.csv'.format(args.source_dir, args.source_filename, formatted_date_str)
    shutil.copy(source_str, target_str)
    return target_str


def send_file_to_s3(target, config):
    res = create_session(config)
    res.Object('niagara-jarvis', 'new/' + os.path.basename(target)).put(Body=open(target, 'rb'))
    print("File {} copied to S3.".format(target))


def get_args():
    parser = ArgumentParser()
    parser.add_argument('--source_dir', required=True, help="Source Directory")
    parser.add_argument('--source_filename', required=True, help="Source FileName")
    parser.add_argument('--target_dir', required=True, help="Target Directory")
    return parser.parse_args()


def delete_file(target):
    os.remove(target)
    print("File {} deleted from the system.".format(target))


if __name__ == '__main__':
    args = get_args()
    config = init_config()
    target = copy_file(args)
    send_file_to_s3(target, config)
    delete_file(target)
