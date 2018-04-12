from jarvis_copier.file_util import *


bucket_name = "niagara-jarvis"


def process_s3_folder(config):
    res = create_session(config)
    for summary in res.Bucket(bucket_name).objects.filter(Prefix="new"):
        if summary.key.endswith('.csv'):
            print(summary.key)
            process_file(res, summary.key)
            move_s3_file(res, summary.key)


def process_file(res, key):
    alist = res.Object(bucket_name, key).get()['Body'].read().decode().split("\n")
    for str in alist:
        print(str)


def move_s3_file(res, key):
    copy_source = {
        "Bucket": bucket_name,
        "Key": key
    }
    key_parts = key.split("/")
    new_key = "{}/{}".format("processed", key_parts[1])
    bucket = res.Bucket(bucket_name)
    bucket.copy(copy_source, new_key)
    res.Object(bucket_name, key).delete()


if __name__ == '__main__':
    config = init_config()
    process_s3_folder(config)
