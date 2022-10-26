#!/usr/bin/env python3
""" video handler
Environment:
  AWS_ENDPOINT
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  TIMMY_SQS_ENDPOINT
  TIMMY_SQS_QUEUE
  TIMMY_TMP_FOLDER
  TIMMY_SQS_CHUNK
"""
import time
from os import path, walk, stat, remove, rmdir, environ as env
import hashlib
import json
import logging
import boto3
import progressbar
from torrentp import TorrentDownloader
from botocore.exceptions import ClientError


def object_check(s3_client, bucket, key):
    """ Check object existance in S3 by head request """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if int(error.response["Error"]["Code"]) != 404:
            logging.error("%s", error)
        return False
    return True


def download_torrent(magnet, folder):
    """ Download torrent file """
    try:
        torrent_file = TorrentDownloader(magnet, folder)
        torrent_file.start_download()
    except Exception as error:
        logging.error("%s", error)
        return False
    return True


def upload_file(folder_path, bucket, key=None):
    """ Upload a file to an S3 bucket """
    try:
        s3_client = boto3.session.Session().client(
            service_name="s3",
            endpoint_url=env.get("AWS_ENDPOINT"),
            aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        )
        for root, dirs, files in walk(folder_path):
            for filename in files:
                local_path = path.join(root, filename)
                if object_check(s3_client, bucket, local_path):
                    logging.info("%s already in bucket", filename)
                    remove(local_path)
                else:
                    logging.info("Uploading: %s", filename)
                    statinfo = stat(local_path)
                    widgets = [
                        "Uploading: " + filename + " : ",
                        progressbar.Percentage(),
                        " ",
                        progressbar.Bar(marker=progressbar.RotatingMarker()),
                        " ",
                        progressbar.ETA(),
                        " ",
                        progressbar.FileTransferSpeed(),
                    ]
                    up_progress = progressbar.progressbar.ProgressBar(
                        widgets=widgets, maxval=statinfo.st_size
                    )
                    up_progress.start()
                    def upload_progress(chunk):
                        up_progress.update(up_progress.currval + chunk)
                    if key is None:
                        key = local_path
                    s3_client.upload_file(
                        local_path, Bucket=bucket, Key=local_path, Callback=upload_progress
                    )
                    remove(local_path)
        rmdir(root)
        s3_client._endpoint.http_session.close()
    except ClientError as error:
        logging.error("%s", error)
        return False
    return filename


def set_local_folder(payload):
    """ Set temporary folder path """
    folder = env.get("TIMMY_TMP_FOLDER", "")
    if payload["output"]["key"] == "":
        folder += payload["input"]["name"] + "."
        folder += hashlib.md5(
            payload["input"]["magnet_url"].encode("utf-8")
        ).hexdigest()
    else:
        folder += payload["output"]["key"]
    return folder


def message_handler():
    """ Obtain message from SQS and handle them """
    client = boto3.client(
        service_name="sqs",
        endpoint_url=env.get("TIMMY_SQS_ENDPOINT"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        region_name="ru-central1",
    )
    queue_url = env.get("TIMMY_SQS_QUEUE")
    messages = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=int(env.get("TIMMY_SQS_CHUNK", 10)),
        WaitTimeSeconds=20,
    ).get("Messages")
    if messages is not None:
        for message in messages:
            payload = json.loads(message.get("Body"))
            folder = set_local_folder(payload)
            download_torrent(payload["input"]["magnet_url"], folder)
            upload_file(folder, payload["output"]["bucket"])
            logging.info("Message processed: %s", message.get("MessageId"))
            client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message.get("ReceiptHandle")
            )
            return True
    return False


if __name__ == "__main__":
    logging.basicConfig(
        level=env.get("LOG_LEVEL", 20), format="%(levelname)s - %(message)s"
    )
    while 1:
        message_handler()
        time.sleep(5)
