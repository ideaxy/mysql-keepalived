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

preSlaveSQL_s = "set global super_read_only=1"
preSlaveSQL = "set global read_only=1;"

preMasterSQL_s = "set global super_read_only=0;"
preMasterSQL = "set global read_only=0;"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filename='/tmp/kp.log',
    filemode='a'
)


class DBase:
    conn = None

    def __init__(self, host=None, port=None, user=None, passwd=None):
        self.dbhost = host
        self.dbport = port
        self.dbuser = user
        self.dbpassword = passwd
        try:
            self.conn = pymysql.connect(
                host=self.dbhost,
                port=int(self.dbport),
                user=self.dbuser,
                password=self.dbpassword
            )
        except pymysql.Error as e:
            logging.error(f"数据库连接失败: {e}")
            sys.exit(1)

    def makeMaster(self):
        try:
            cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            if preMasterSQL.strip():
                cursor.execute(preMasterSQL)
                logging.info("将主库read_only设置为false.")
                cursor.execute(preMasterSQL_s)
                logging.info("将主库super_read_only设置为false.")
                self.conn.commit()
            cursor.execute("show slave status")
            results = cursor.fetchall()
            for row in results:
                if row.get('Slave_IO_Running') == 'Yes':
                    channel_name = row.get('Channel_Name')
                    cursor.execute(f"stop slave io_thread for channel  '{channel_name}'")
                    logging.warning(f"停止从库 IO 线程，通道名: {channel_name}")
                    self.conn.commit()
        except pymysql.Error as e:
            logging.error(f"设置主库时出错: {e}")
        finally:
            if cursor:
                cursor.close()

    def makeSlave(self):
        try:
            cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            if preSlaveSQL.strip():
                cursor.execute(preSlaveSQL)
                logging.info("将从库read_only设置为true.")
                cursor.execute(preSlaveSQL_s)
                logging.info("将从库super_read_only设置为true.")
                self.conn.commit()
            cursor.execute("show slave status")
            results = cursor.fetchall()
            for row in results:
                if row.get('Slave_IO_Running') == 'No':
                    channel_name = row.get('Channel_Name')
                    cursor.execute(f"start slave for channel '{channel_name}'")
                    logging.warning(f"启动从库，通道名: {channel_name}")
                    self.conn.commit()
        except pymysql.Error as e:
            logging.error(f"设置从库时出错: {e}")
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        if self.conn:
            try:
                self.conn.close()
            except pymysql.Error as e:
                logging.error(f"关闭数据库连接时出错: {e}")


if __name__ == "__main__":
    lock = FileLock("/tmp/kps.txt")
    if lock:
        logging.info("ZST Get Lock.start!!!")
    try:
        with lock.acquire(timeout=5):
            logging.info(sys.argv)
            dbhost = config.dbhost
            dbport = config.dbport
            dbuser = config.dbuser
            dbpassword = config.dbpassword
            db = DBase(dbhost, dbport, dbuser, dbpassword)
            if len(sys.argv) > 3 and sys.argv[3].upper() == 'MASTER':
                logging.warning("当前变为主库!!!")
                db.makeMaster()
            elif len(sys.argv) > 3 and sys.argv[3].upper() == "BACKUP":
                logging.warning("当前变为从库!!!")
                db.makeSlave()
            db.disconnect()
    except TimeoutError:
        logging.warning("获取文件锁超时")
    except IndexError:
        logging.error("命令行参数不足，请检查输入。")
    except Exception as e:
        logging.error(f"发生未知错误: {e}")
