from os import makedirs
from os.path import join as pjoin
from threading import Thread
import sys
import threading
import logging

import boto3

from config import config
from worker import lambda_worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    session = boto3.Session(
        region_name="us-east-1" # Must
    )

    start_id, end_id = int(sys.argv[1]), int(sys.argv[2])

    lambda_arn     = config.get("FunctionARN")
    target_dir     = config.get("TargetDir")
    s3_paths_file  = config.get("S3paths")
    max_concurrent = config.get("MaxConcurrent")

    with open(s3_paths_file, "r") as f:
        s3_paths = [line.strip() for line in f.readlines()]
    makedirs(target_dir, exist_ok=True)
    limiter = threading.BoundedSemaphore(max_concurrent)

    for item_id in range(start_id, end_id):
        lambda_client = session.client('lambda')
        s3_route      = "s3://commoncrawl/%s" % s3_paths[item_id]

        target_folder = pjoin(target_dir, str(item_id // 1000))
        target_path   = pjoin(target_folder, '%05d.txt' % item_id)
        makedirs(target_folder, exist_ok=True)

        limiter.acquire()
        callback = lambda : limiter.release()

        w = Thread(
            target=lambda_worker,
            args=(
                lambda_client,
                lambda_arn,
                s3_route,
                target_path,
                callback
            )
        )
        w.start()
        if item_id % max_concurrent == 0:
            logger.info("Worker #%05d / %d Emitted" % (item_id, len(s3_paths)))

if __name__ == "__main__":
    main()