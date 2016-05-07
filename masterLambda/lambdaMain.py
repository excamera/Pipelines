from __future__ import print_function
import boto3
import os
import sys
import uuid
import logging
import time
from random import randint
import subprocess as sp
import threading
import Queue
import json

CUT_TOOL = "./cut.sh"
CHUNK_SIZE = 10
INPUT_VIDEO_BUCKET = ""
TEMP_VIDEO_BUCKET = ""
RESULT_BUCKET = ""
WORKER_LAMBDA = ""
REDUCER_LAMBDA = ""
SNS_TOPIC_ARN = ""

sys.path.append(".") 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
   
s3_client = boto3.client('s3')

def split_video(file_name):
    command = [ CUT_TOOL, file_name, str(CHUNK_SIZE)]
    num_chunks = 0
    try :
       num_chunks = int(sp.check_output(command).rstrip()) - 1
    except sp.CalledProcessError as e:
        logger.info("the exception is " + str(e.output))
    return num_chunks;

def geti(i):
    if i < 10:
        return "00%d" % i
    elif i < 100:
        return "0%d" % i
    else:
        return str(i)

def get_video_part_name(video_name, i):
    return video_name.split(".")[0] + "-" + geti(i) + "." + video_name.split(".")[1] 

def upload_fire(queue, bucket, part_name):
    client = boto3.session.Session()
    s3_client = client.client('s3')
    file_path = "/tmp/" + part_name 
    s3_client.upload_file(file_path, TEMP_VIDEO_BUCKET, part_name)    
    lambda_client = client.client('lambda')
    data = lambda_client.invoke(FunctionName=WORKER_LAMBDA, InvocationType='RequestResponse', Payload=json.dumps({"key":part_name}))["Payload"].read()
    data = json.loads(data)
    queue.put(data)

def invoke_reducer(list_keys, video_name):
    lambda_client = boto3.client('lambda')
    return lambda_client.invoke(FunctionName=REDUCER_LAMBDA, InvocationType='RequestResponse', Payload=json.dumps({"key_list": list_keys, "video_name" : video_name}))["Payload"].read()

def notify_sns(output_key):
    message = {"output_key": output_key}
    client = boto3.client('sns')
    response = client.publish(
        TargetArn=SNS_TOPIC_ARN,
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )

def invoke_threads(unique_video_name, download_path, num_chunks):
    data_queue = Queue.Queue()    
    threads = []
    for i in range(1,num_chunks+1):
        t = threading.Thread(target=upload_fire, args = (data_queue, TEMP_VIDEO_BUCKET, get_video_part_name(unique_video_name, i)))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    return data_queue

def write_to_s3(data,video_file):
    file_name = video_file.split(".")[0] + ".txt"
    f = file('/tmp/'+ file_name, "w+")
    f.write(data)
    f.close()
    s3_client.upload_file("/tmp/"+file_name, RESULT_BUCKET,  file_name)
    
def cleanup_files(video_filepath):
    sp.check_call(["rm", "-rf", video_filepath +"*"])

def handle(key):
    unique_video_name = "{}{}".format(uuid.uuid4(), key)
    download_path = '/tmp/%s' % unique_video_name
    s3_client.download_file(INPUT_VIDEO_BUCKET, key, download_path)
    num_chunks = split_video(download_path)
    output_key_queue = invoke_threads(unique_video_name, download_path, num_chunks)
    output_key_list = []
    len_queue = output_key_queue.qsize()
    for i in range(0, len_queue):
        output_key_list.append(output_key_queue.get()["output_key"])
    output_key = invoke_reducer(output_key_list, unique_video_name)
    notify_sns(output_key) 
    cleanup_files(download_path)    
    return {"output" : output_key}

def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        return handle(key) 


