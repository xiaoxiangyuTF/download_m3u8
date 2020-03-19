#!/usr/bin/env python
# -*- coding:utf-8 -*
# ############################################################
#
# Copyright (c) 2020 xxx.com, Inc. All Rights Reserved
#
# ############################################################

'''
@Date: 2020-03-15 16:14:35
@Author: tf
@LastEditTime: 2020-03-19 00:08:18
@LastEditors: tianfeng04
@Description:
'''
import argparse
import datetime
import hashlib
import re
import requests
import shutil
import threading
import os
from queue import Queue

class Downloadm3u8(object):
    """
    download m3u8
    Arguments:
        object {[type]} -- [description]
    """

    def __init__(self, thread_num):
        """
        init
        Arguments:
            object {[type]} -- [description]
            thread_num {[type]} -- [description]
        """
        self.__thread_num = thread_num
        self.__tmp_dir = './cache'

    def __down_m3u8_file(self, m3u8_url):
        """
        download m3u8 file
        Arguments:
            m3u8_url {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        resp = requests.get(m3u8_url)
        m3u8_text = resp.text
        ts_queue = Queue(10000)
        lines = m3u8_text.split('\n')

        base_download_url = os.path.dirname(m3u8_url)
        local_m3u8_path = os.path.join(self.__tmp_dir, "local.m3u8")
        index = 0
        contents = []
        key_url = None
        for line in lines:
            if re.search('URI=', line):
                p1 = re.compile(r'URI=["](.*?)["]', re.S)
                key_url = re.findall(p1, line)[0]

            if re.match('^#', line) or len(line) < 10:
                contents.append(line + os.linesep)
                continue

            index += 1
            file_url = os.path.join(base_download_url, line)
            file_name = str(index).zfill(6) + ".ts"
            ts_queue.put([file_url, file_name])
            contents.append(file_name + os.linesep)

        # if key_url exist, download
        key_name = ''
        if key_url:
            ret = requests.get(key_url)
            key_name = hashlib.md5(key_url.encode()).hexdigest() + ".key"
            key_path = os.path.join(self.__tmp_dir, key_name)
            with open(key_path, 'wb') as f:
                f.write(ret.content)
                f.close()

        # if key_url exist, update key_url by local_url
        for index, line in enumerate(contents):
            if re.search('URI=', line):
                p1 = re.compile(r'URI=["](.*?)["]', re.S)
                key_url = re.findall(p1, line)[0]
                line = line.replace(key_url, key_name)
                contents[index] = line

        # update m3u8
        with open(local_m3u8_path, 'w') as f:
            for line in contents:
                f.write(line)
            f.close()
        return ts_queue, local_m3u8_path

    def __thread_download_ts(self, ts_queue):
        tt_name = threading.current_thread().getName()
        while not ts_queue.empty():
            ts_obj = ts_queue.get()
            ts_url = ts_obj[0]
            filename = ts_obj[1]
            r = requests.get(ts_url, stream=True)
            ts_local_path = os.path.join(self.__tmp_dir, filename)
            if os.path.exists(ts_local_path):
                continue
            with open(ts_local_path, 'wb') as fp:
                for chunk in r.iter_content(5242):
                    if chunk:
                        fp.write(chunk)
            print("[{}]: src: {} --> dst: {} succeed.".format(tt_name, ts_url, ts_local_path))

    def __merge_ts_by_ffmepg(self, local_m3u8_path, video_name):
        """
        merge *.ts by ffmepg
        Arguments:
            concatfile {[type]} -- [description]
            name {[type]} -- [description]
        """
        try:
            command = 'ffmpeg -allowed_extensions ALL -i {} -c copy -y {}'.format(local_m3u8_path, video_name)
            print(command)
            os.system(command)
            print('merge succeed.')
        except:
            print('merge failed.')

    def run(self, m3u8_url, video_name):
        """
        download video
        Arguments:
            m3u8_url {[type]} -- [description]
            video_name {[type]} -- [description]
        """
        if not os.path.exists(self.__tmp_dir):
            os.makedirs(self.__tmp_dir)

        s, local_m3u8_path = self.__down_m3u8_file(m3u8_url=m3u8_url)
        start_time = datetime.datetime.now().replace(microsecond=0)
        threads = []
        for i in range(self.__thread_num):
            t = threading.Thread(target=self.__thread_download_ts, name='th-'+str(i), kwargs={'ts_queue': s})
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        end_time = datetime.datetime.now().replace(microsecond=0)
        print('dowonload consuming: {}'.format(end_time - start_time))

        start_time = datetime.datetime.now().replace(microsecond=0)
        self.__merge_ts_by_ffmepg(local_m3u8_path=local_m3u8_path, video_name=video_name)
        end_time = datetime.datetime.now().replace(microsecond=0)
        print("merge consuming: {}".format(end_time - start_time))

        shutil.rmtree(self.__tmp_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version',
                        version='%(prog)s version : v0.0.1', help='show the version')

    parser.add_argument("--m3u8_url", "-m", type=str, help='download m3u8 url')
    parser.add_argument("--video_name", "-v", type=str, help="vido name like 0001.mp4")
    parser.add_argument("--num_thread", "-n", type=int, default=8, help="thread nums")

    args = parser.parse_args()
    m3u8_url = args.m3u8_url
    video_name = args.video_name
    thread_num = args.num_thread
    if thread_num < 0:
        thread_num = 16
    Downloadm3u8(thread_num=thread_num).run(m3u8_url=m3u8_url, video_name=video_name)
