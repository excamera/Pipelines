from __future__ import print_function
from classify_image import maybe_download_and_extract
from classify_image import run_inference_on_image
import boto3
import os
import sys
import uuid
import logging
import time
from random import randint
import subprocess as sp
FFMPEG_BIN = "./ffmpeg"
TEMP_BUCKET_NAME = ""
sys.path.append(".") 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
   
s3_client = boto3.client('s3')

def grab_frame(file_name, image_id, logger):
    command = [ FFMPEG_BIN,
            '-i', file_name,
            '-ss', '00:00:'+image_id,
            '-f', 'image2',
            '-vframes', '1',
            get_image_name(image_id)]
#    f = open('/tmp/log.txt','w+')
    log = "SUCCESS"
    try :
       log = sp.check_output(command)
    except sp.CalledProcessError as e:
        logger.info("the exception is " + str(e.output) + " -------------")
        return str(e.output)
    return log

def cleanup_files(video_filepath, image_filepath):
    sp.check_call(["rm", "-rf", video_filepath])
    sp.check_call(["rm", "-rf", image_filepath])

def get_image_name(image_id):
    return '/tmp/'+image_id.replace('.','_')+'.jpg'
    
def get_time_from_id(lambda_id):
    sec = lambda_id /25
    milisecond = ((lambda_id/25.0) -sec) * 1000
    return '%d.%d'%(sec, milisecond)

def handler(event, context):
    bucket = TEMP_BUCKET_NAME
    key = event['key']
    lambda_id = 1
    image_id = get_time_from_id(lambda_id)
    video_name = '{}{}'.format(uuid.uuid4(), key)
    download_path = '/tmp/%s' % video_name
    s3_client.download_file(bucket, key, download_path)
    log = grab_frame(download_path, image_id, logger)
    (infer,times) = run_inference_on_image(get_image_name(image_id), logger)
    cleanup_files(download_path, get_image_name(image_id)) 
    output_file = video_name.split(".")[0] + ".txt"
    open("/tmp/" + output_file, "w+").write(infer)
    s3_client.upload_file("/tmp/" + output_file, bucket, output_file)
    return {'output_key' : output_file}
