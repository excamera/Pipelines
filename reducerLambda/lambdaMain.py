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

INPUT_VIDEO_BUCKET = ""
TEMP_VIDEO_BUCKET = ""
RESULT_BUCKET = ""
WORKER_LAMBDA = ""
REDUCER_LAMBDA = ""
FFMPEG_BIN = "./ffmpeg"

sys.path.append(".") 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
   
s3_client = boto3.client('s3')

def concat_videos(meta_file_name, output_file_name):
    command = [ FFMPEG_BIN,
            "-safe", "0",
            "-f", "concat",
            "-i", meta_file_name,
            "-c", "copy",
            output_file_name
            ]
    print(command)
    try :
        print(sp.check_output(command))
    except sp.CalledProcessError as e:
        logger.info("--------------> the exception is " + str(e.output)+ "<-----------")

def download_key(key):
    client = boto3.session.Session()
    s3_client = client.client('s3')
    file_path = "/tmp/" + key 
    s3_client.download_file(TEMP_VIDEO_BUCKET, key, file_path)    

def invoke_threads(key_list):
    threads = []
    for key in key_list:
        t = threading.Thread(target=download_key, args = (key,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

def join(key_list, video_name):
    data = ""
    if(len(key_list) == 0):
        return data
    if(key_list[0].split(".")[1] == "txt"):
        for key in key_list:
            data += open("/tmp/"+key).read()
        file_name = video_name.split(".")[0] + ".txt"
        f = file('/tmp/'+ file_name, "w+")
        f.write(data)
        f.close()
        return file_name
    else:
        meta_file_name = video_name.split(".")[0] + "meta.txt"
        meta_file = open("/tmp/"+meta_file_name, "a+")
        for key in key_list:
            str_temp = "file '/tmp/%s'\n"% key
            meta_file.write(str_temp)
        meta_file.close()
        final_video_name = video_name.split(".")[0] + "joined." + video_name.split(".")[1]
        print(open("/tmp/"+meta_file_name,"r+").read())
        concat_videos("/tmp/"+meta_file_name, "/tmp/" + final_video_name)
        return final_video_name

def write_to_s3(file_name):
    s3_client.upload_file("/tmp/"+file_name, RESULT_BUCKET,  file_name)
    return file_name
    
def cleanup_files(key_list, video_name ):
    files_to_del = ""
    key_list.append(video_name.split(".")[0]+"*")
    for key in key_list:
        files_to_del += "/tmp/" + key + " "
    sp.check_call(["rm", "-rf", "/tmp/*"])

def handler(event, context):
    key_list = event["key_list"]
    video_name = event["video_name"]
    invoke_threads(key_list)
    data_file_name = join(key_list, video_name)
    final_key = write_to_s3(data_file_name)
    cleanup_files(key_list, final_key)
    return {"output_key" : final_key}    
    
