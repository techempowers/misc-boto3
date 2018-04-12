import configparser
import boto3


def init_config():
    config = configparser.ConfigParser()
    config.read('conf/conf.ini')
    return config


def create_session(config):
    session = boto3.Session(
        aws_access_key_id = config['AWS']['access_key'],
        aws_secret_access_key = config['AWS']['secret_key'],
        region_name= config['AWS']['region']
    )
    res = session.resource('s3')
    return res
