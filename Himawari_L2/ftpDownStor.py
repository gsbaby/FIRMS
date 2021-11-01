# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/26 0026 上午 11:10
# @Function:
import logging
import os

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)


class myFtpFun:
    bIsDir = False
    path = ""

    def __init__(self, ftp, bufsize):
        self.ftp = ftp
        self.bufsize = bufsize  # bufsize设置缓冲块大小

    def DownLoadFile(self, LocalFile, RemoteFile):  # 下载单个文件
        file_handler = open(LocalFile, 'wb')
        self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)  # 可加入self.bufsize控制缓冲块大小
        file_handler.close()
        return True

    def UpLoadFile(self, LocalFile, RemoteFile):  # 上传单个文件
        if os.path.isfile(LocalFile) == False:
            return False
        file_handler = open(LocalFile, "rb")
        self.ftp.storbinary('STOR %s' % RemoteFile, file_handler, self.bufsize)
        file_handler.close()
        return True

    def UpLoadFileTree(self, LocalDir, RemoteDir):  # LocalDir本地文件夹, RemoteDir远程文件夹
        if os.path.isdir(LocalDir) == False:
            return False
        logging.info("本地文件:", LocalDir)
        LocalNames = os.listdir(LocalDir)
        logging.info("文件列表:", LocalNames)
        logging.info(RemoteDir)
        self.ftp.cwd(RemoteDir)
        for Local in LocalNames:
            src = os.path.join(LocalDir, Local)
            if os.path.isdir(src):
                self.UpLoadFileTree(src, Local)
            else:
                logging.info("正在上传文件：{}".format(Local))
                self.UpLoadFile(src, Local)

        self.ftp.cwd("..")
        return

    def DownLoadFileTree(self, LocalDir, RemoteDir):
        logging.info("Ftp地址:" + RemoteDir)
        if os.path.isdir(LocalDir) == False:
            os.makedirs(LocalDir)
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        # logging.info("文件夹中包含的文件个数{}个".format(len(RemoteNames)))
        # logging.info("文件夹中包含的文件名称：" + str(RemoteNames))
        # logging.info(self.ftp.nlst(RemoteDir))
        for file in RemoteNames:
            Local = os.path.join(LocalDir, file)
            if self.isDir(file):
                self.DownLoadFileTree(Local, file)
            else:
                logging.info("下载文件：{}".format(file))
                self.DownLoadFile(Local, file)
        self.ftp.cwd("..")
        return

    def show(self, list):
        result = list.lower().split(" ")
        if self.path in result and "<dir>" in result:
            self.bIsDir = True

    def isDir(self, path):
        self.bIsDir = False
        self.path = path
        # this ues callback function ,that will change bIsDir value
        self.ftp.retrlines('LIST', self.show)
        return self.bIsDir
