# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/26 0026 下午 2:26
# @Function:

import datetime
import logging
import os
import time

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler

import ftpProxy
import openCsv
from ftpDownStor import myFtpFun

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)


class infoMationGo():
    def __init__(self):
        self.savePath = "/run/user/1000/gvfs/afp-volume:host=RS.local,user=admin,volume=RS_GIS/RS_DATA/Himawari/L2/WLF/010"
        self.remotepath = "/pub/himawari/L2/WLF/010"
        # ftp连接参数
        self.ftp_params = {
            'proxy_host': 'proxy ip',
            'proxy_port': port,
            'ftp_host': 'ftp.ptree.jaxa.jp',
            'ftp_user': 'email@email.com',
            'ftp_password': 'password',
        }
        self.ftp = {}
        self.bufsize = 4096  # bufsize设置缓冲块大小
        self.constructInit()

    def constructInit(self):
        # 获取配置文件的参数值
        # 获取ftp连接参数
        proxy_host = self.ftp_params.get('proxy_host')
        proxy_port = self.ftp_params.get('proxy_port')
        ftp_host = self.ftp_params.get('ftp_host')
        ftp_user = self.ftp_params.get('ftp_user')
        ftp_password = self.ftp_params.get('ftp_password')

        ftp_proxy = ftpProxy.ftp_proxy(proxy_host, proxy_port, ftp_host, ftp_user, ftp_password)
        ftp_proxy.setProxy()
        self.ftp = ftp_proxy.ftpLogin()
        logging.info("ftp登录,ftp:%s" % self.ftp)

        self.myftpfun = myFtpFun(self.ftp, self.bufsize)

    def existsPath(self, path):
        if os.path.exists(path):
            pass
        else:
            os.mkdir(path)

    #
    def compareDiffArray(self, arr1, arr2):
        return set(arr1).difference(set(arr2))

    def readRemoteDir(self, path):
        # 进入到ftp目录下获取目录所有的文件夹名称及数目
        self.ftp.cwd(path)
        listNlst = self.ftp.nlst()
        listNlst.sort()
        return listNlst

    def init_first_time(self):
        list_remote = self.readRemoteDir(self.remotepath)
        # 先同步所有数据
        # 否则在本地服务器上创建文件夹
        for i in list_remote:
            path_month = self.savePath + '/' + i
            self.existsPath(path_month)
            remotepath_1 = self.remotepath + '/' + i
            list_remote_month = self.readRemoteDir(remotepath_1)
            # day的差异
            for j in list_remote_month:
                path_day = path_month + '/' + j
                self.existsPath(path_day)
                remotepath_2 = remotepath_1 + '/' + j
                list_local_day = self.readRemoteDir(remotepath_2)
                for m in list_local_day:
                    path_hour = path_day + '/' + m
                    self.existsPath(path_hour)
                    remotepath_3 = remotepath_2 + '/' + m
                    dirs_local_hour = os.listdir(path_hour)
                    list_local_hour = self.readRemoteDir(remotepath_3)
                    # hour的差异
                    fileDiff = self.compareDiffArray(list_local_hour, dirs_local_hour)
                    if len(fileDiff) == 0:
                        # 进入每个文件夹检查是否与服务器文件相同，不相同进行下载
                        for n in list_local_hour:
                            path_10_min = path_hour + '/' + n
                            remotepath_4 = remotepath_3 + '/' + n
                            dirs_local_10_min = os.listdir(path_10_min)
                            list_local_10_min = self.readRemoteDir(remotepath_4)
                            # 10_min的差异
                            fileDiff = self.compareDiffArray(list_local_10_min, dirs_local_10_min)
                            for z in fileDiff:
                                self.myftpfun.DownLoadFile(path_10_min + '/' + z, remotepath_4 + '/' + z)
                            # 下载文件
                    else:
                        # 下载文件夹内的所有数据
                        # 第一个变量本地文件夹, 第二个变量ftp文件夹
                        self.myftpfun.DownLoadFileTree(path_hour, remotepath_3)  # ok

    def go(self):
        # 检查是否是第一次初始化
        if self.connectPost():
            self.connectPost('update console set isinit = 1', True)
            self.init_first_time()
        else:
            # 不是第一次操作的时候更新最新的文件夹中的文件即可
            self.updateSingleFile()

    # 更新最新的文件夹中的文件
    def updateSingleFile(self):
        # 查询月
        list_remote = self.readRemoteDir(self.remotepath)
        lastPath = list_remote[len(list_remote) - 1]
        path_local = self.savePath + '/' + lastPath
        path_remote = self.remotepath + '/' + lastPath
        self.existsPath(path_local)
        # 进入月，查询日
        list_remote = self.readRemoteDir(path_remote)
        lastPath = list_remote[len(list_remote) - 1]
        path_local = path_local + '/' + lastPath
        path_remote = path_remote + '/' + lastPath
        self.existsPath(path_local)
        # 进入日，查询时
        list_remote = self.readRemoteDir(path_remote)
        # 先对时进行查重，如果不一样，先进行倒数第二个文件夹的查重下载，然后再去最后一个文件夹下查重下载
        for i in list_remote[len(list_remote) - 2:]:
            path_local_i = path_local + '/' + i
            path_remote_i = path_remote + '/' + i
            logging.info(path_remote_i)
            self.existsPath(path_local_i)
            # 进入时，对比文件
            dirs_local = os.listdir(path_local_i)
            list_remote = self.readRemoteDir(path_remote_i)
            # 10_min的差异
            fileDiff = self.compareDiffArray(list_remote, dirs_local)
            for z in fileDiff:
                # 下载文件
                fileCSVPath = path_local_i + '/' + z
                self.myftpfun.DownLoadFile(fileCSVPath, path_remote_i + '/' + z)
                if os.path.exists(fileCSVPath):
                    pass
                else:
                    time.sleep(2)
                # 创建空间数据
                point_tableName = z.split(".")[0]
                polygon_tableName = point_tableName + '_polygon'
                readcsvfile = openCsv.readCsvFile()
                csv_data = readcsvfile.readFile(fileCSVPath)
                # Point
                readcsvfile.connectDatabase(point_tableName, True)
                readcsvfile.insertInto(csv_data, point_tableName, True)
                readcsvfile.insert_db_connect_table(point_tableName)
                # # Polygon
                # readcsvfile.connectDatabase(polygon_tableName, False)
                # readcsvfile.insertInto(csv_data, polygon_tableName, False)

    def connectPost(self, sql='select isinit from console', isDo=False):
        # 数据库连接参数
        conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
        cur = conn.cursor()
        if isDo:
            cur.execute(sql)
            conn.commit()
        else:
            cur.execute(sql)
            isFirst = cur.fetchone()[0]
            if isFirst == 0:
                return True
            else:
                return False
        cur.close()
        conn.close()

    def destoryInfo(self):
        self.ftp.close()


def getInfomationFun():
    logging.info("开始UTC时间:[%s]" % (datetime.datetime.utcnow()))
    getInfomation = infoMationGo()
    getInfomation.go()
    getInfomation.destoryInfo()


if __name__ == "__main__":
    logging.info("localtion开始时间:[%s]" % (datetime.datetime.now()))
    # getInfomation = infoMationGo()
    # getInfomation.go()
    # getInfomation.destoryInfo()
    # 解决定时任务的问题
    scheduler = BlockingScheduler()
    scheduler.add_job(getInfomationFun, 'interval', minutes=10)  # date: 特定的时间点触发；interval: 固定时间间隔触发；cron: 在特定时间周期性地触发
    scheduler.start()
