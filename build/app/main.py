#!/usr/bin/env python3
""" torrent handler """
"""
Environment:
  AWS_ENDPOINT
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  TIMMY_UPLOAD_BUCKET
  TIMMY_SQS_ENDPOINT
  TIMMY_SQS_QUEUE
  TIMMY_TMP_FOLDER
  TIMMY_SQS_CHUNK
"""
from os import path, walk, stat, remove, rmdir, environ as env
import hashlib
import json
import logging
import asyncio
import boto3

import progressbar
from torf import Magnet, URLError, MagnetError
from torrentp import TorrentDownloader
from botocore.exceptions import ClientError
from botocore.config import Config as botoConfig


def get_download_path(torrent_name):
    """ Set temporary folder path """
    return (
        env.get("TIMMY_TMP_FOLDER", "")
        + torrent_name
        + "."
        + hashlib.md5(torrent_name.encode("utf-8")).hexdigest()
    )


def object_check(client, bucket, key):
    """ Check object existance in S3 by head request """
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if int(error.response["Error"]["Code"]) != 404:
            logging.error("%s", error)
        return False


async def download_torrent(magnet, folder):
    """ Download torrent file """
    try:
        torrent_file = TorrentDownloader(magnet, folder)
        torrent_file.start_download()
    except Exception as error:
        logging.error("%s", error)


async def s3_upload(local_folder, upload_bucket, prefix=None):
    """ Upload a file to an S3 bucket """
    try:
        for root, dirs, files in walk(local_folder):
            for filename in files:
                file_path = path.join(root, filename)
                file_key = file_path.replace(local_folder + "/", "")
                if prefix:
                    file_key = prefix + "/" + file_key
                if object_check(s3_client, upload_bucket, file_key):
                    logging.info("%s already in bucket", filename)
                    remove(file_path)
                else:
                    logging.info("Uploading: %s ", filename)
                    statinfo = stat(file_path)
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

                    s3_client.upload_file(
                        file_path,
                        Bucket=upload_bucket,
                        Key=file_key,
                        Callback=upload_progress,
                    )
                    remove(file_path)
        rmdir(root)
    except ClientError as error:
        logging.error("%s", error)


async def message_handler():
    """ Obtain message from SQS and handle them """
    try:
        queue_url = env.get("TIMMY_SQS_QUEUE")
        messages = sqs_client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=int(env.get("TIMMY_SQS_CHUNK", 5)),
            VisibilityTimeout=int(env.get("TIMMY_SQS_CHUNK", 1)) * 3600,
        ).get("Messages")
        if messages is not None:
            for message in messages:
                magnet_url = message.get("Body")
                magnet_link_obj = Magnet.from_string(magnet_url)
                download_path = get_download_path(magnet_link_obj.torrent().name)
                sqs_client.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message.get("ReceiptHandle")
                )
                await download_torrent(magnet_url, download_path)
                await s3_upload(
                    download_path, env.get("TIMMY_UPLOAD_BUCKET")
                )
                logging.info("Message processed: %s", message.get("MessageId"))
            logging.info("%s", "Chunk processed")
    except (URLError, MagnetError, ClientError) as error:
        logging.error("%s", error)


async def sqs_init():
    """ init SQS client """
    global sqs_client
    sqs_client = boto3.client(
        service_name="sqs",
        endpoint_url=env.get("TIMMY_SQS_ENDPOINT"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        region_name="ru-central1",
        config=botoConfig(connect_timeout=30, read_timeout=90),
    )


async def s3_init():
    """ init S3 client """
    global s3_client
    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=env.get("AWS_ENDPOINT"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        config=botoConfig(connect_timeout=30, read_timeout=90),
    )


async def default_handler():
    """ default handler """
    logging.info("%s", "Ready to handle messages in queue")
    while True:
        await message_handler()
        await asyncio.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.getLevelName(env.get("LOG_LEVEL", "INFO")),
        format="TIMMY - %(levelname)s - %(asctime)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    asyncio.run(s3_init())
    asyncio.run(sqs_init())
    asyncio.run(default_handler())

