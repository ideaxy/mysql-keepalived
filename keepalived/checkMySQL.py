#!/usr/bin/env python3
# coding: utf-8
# http://zhishuedu.com 
# Copyright (c) 2017 - wubx(wubx@zhishuedu.com)  

import sys
import os
import getopt
import pymysql
import logging
from filelock import FileLock
import config

dbhost = config.dbhost
dbport = config.dbport
dbuser = config.dbuser
dbpassword = config.dbpassword

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/tmp/kp_check.log',
                    filemode='a')

def checkMySQL():
    global dbhost
    global dbport
    global dbuser
    global dbpassword

    shortargs = 'h:P:'
    try:
        opts, args = getopt.getopt(sys.argv[1:], shortargs)
        for opt, value in opts:
            if opt == '-h':
                dbhost = value
            elif opt == '-P':
                dbport = value
    except getopt.GetoptError as e:
        logging.error(f"命令行参数解析错误: {e}")
        return 1

    db = instanceMySQL(dbhost, dbport, dbuser, dbpassword)
    st = db.ishaveMySQL()
    return st

class instanceMySQL:
    conn = None

    def __init__(self, host=None, port=None, user=None, passwd=None):
        self.dbhost = host
        self.dbport = int(port)
        self.dbuser = user
        self.dbpassword = passwd

    def ishaveMySQL(self):
        cmd = f"ps -ef | egrep -i \"mysqld\" | grep {self.dbport} | egrep -iv \"mysqld_safe\" | grep -v grep | wc -l"
        mysqldNum = os.popen(cmd).read().strip()
        logging.info(f"进程{self.dbport}数量:{mysqldNum}")
        cmd = f"sudo netstat -tunlp | grep \":{self.dbport}\" | wc -l"
        mysqlPortNum = os.popen(cmd).read().strip()
        logging.info(f"端口{self.dbport}数量:{mysqlPortNum}")
        try:
            if int(mysqldNum) <= 0:
                logging.error("进程数量小于0.")
                return 1
            if int(mysqldNum) > 0 and int(mysqlPortNum) <= 0:
                logging.error("端口未被监听.")
                return 1
            return 0
        except ValueError:
            logging.error("无法将进程或端口计数转换为整数")
            return 1

    def connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.dbhost,
                port=self.dbport,
                user=dbuser,
                password=self.dbpassword
            )
            return 0
        except pymysql.Error as e:
            logging.error(f"数据库连接失败: {e}")
            return 1

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

if __name__ == "__main__":
    lock = FileLock("/tmp/kpc.txt")
    if lock:
        logging.info("Get Lock.start!!!")
    try:
        with lock.acquire(timeout=5):
            pass
    except TimeoutError:
        logging.warning("get file lock timeout")

    st = checkMySQL()
    sys.exit(st)
