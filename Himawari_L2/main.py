# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/26 0026 下午 2:26
# @Function: 同步服务器与本地的文件
# 要求表console的属性isinit为0


import logging
import os

import psycopg2

import ftpProxy
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
            'proxy_host': '127.0.0.1',
            'proxy_port': 7890,
            'ftp_host': 'ftp.ptree.jaxa.jp',
            'ftp_user': 'nasa.gaosong_gmail.com',
            'ftp_password': 'SP+wari8',
        }
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

        self.bufsize = 4096  # bufsize设置缓冲块大小
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
                        for n in fileDiff:
                            # 下载文件
                            path_10_min = path_hour + '/' + n
                            remotepath_4 = remotepath_3 + '/' + n
                            self.myftpfun.DownLoadFile(path_10_min, remotepath_4)

                            # path_10_min = path_hour + '/' + n
                            # remotepath_4 = remotepath_3 + '/' + n
                            # self.existsPath(path_10_min)
                            # dirs_local_10_min = os.listdir(path_10_min)
                            # list_local_10_min = self.readRemoteDir(remotepath_4)
                            # # 10_min的差异
                            # fileDiff = self.compareDiffArray(list_local_10_min, dirs_local_10_min)
                            # for z in fileDiff:
                            #     self.myftpfun.DownLoadFile(path_10_min + '/' + z, remotepath_4 + '/' + z)
                    else:
                        # 下载文件夹内的所有数据
                        # 第一个变量本地文件夹, 第二个变量ftp文件夹
                        self.myftpfun.DownLoadFileTree(path_hour, remotepath_3)  # ok

    def go(self):
        self.init_first_time()
        # # 检查是否是第一次初始化
        # if self.connectPost():
        #     # self.connectPost('update console set isinit = 1', True)
        #     self.init_first_time()
        # else:
        #     # 不是第一次操作的时候更新最新的文件夹中的文件即可
        #     self.updateSingleFile()

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
        lastPath = list_remote[len(list_remote) - 1]
        path_local = path_local + '/' + lastPath
        path_remote = path_remote + '/' + lastPath
        self.existsPath(path_local)
        # 进入时，对比文件
        dirs_local = os.listdir(path_local)
        list_remote = self.readRemoteDir(path_remote)
        # 10_min的差异
        fileDiff = self.compareDiffArray(list_remote, dirs_local)
        for z in fileDiff:
            # 下载文件
            self.myftpfun.DownLoadFile(path_local + '/' + z, path_remote + '/' + z)

    def connectPost(self, sql='select isinit from console', isDo=False):
        # 数据库连接参数
        conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
        cur = conn.cursor()
        if isDo:
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        else:
            cur.execute(sql)
            isFirst = cur.fetchone()[0]
            cur.close()
            conn.close()
            if isFirst == 0:
                return True
            else:
                return False


def getInfomationFun():
    getInfomation.go()


if __name__ == "__main__":

    getInfomation = infoMationGo()
    getInfomation.go()
    # logging.info("开始时间:[%s]" % (datetime.datetime.now()))
    # # 解决定时任务的问题
    # scheduler = BlockingScheduler()
    # scheduler.add_job(getInfomationFun, 'interval', minutes=10)  # date: 特定的时间点触发；interval: 固定时间间隔触发；cron: 在特定时间周期性地触发
    # scheduler.start()
