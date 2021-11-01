# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/26 0026 下午 1:44
# @Function:

from ftplib import FTP
import socks
import socket
import os
import shutil
import logging

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)

'''
    通过代理连接FTP服务器，并实现上传和下载功能，完成上传或下载后，将目录下的文件移动至备份目录
'''


class ftp_proxy():
    def __init__(self, proxy_host, proxy_port, ftp_host, ftp_user, ftp_password):
        # 初始化ftp参数
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.ftp_host = ftp_host
        self.ftp_user = ftp_user
        self.ftp_password = ftp_password

    # 设置ftp代理
    def setProxy(self):
        socks.set_default_proxy(socks.HTTP,
                                self.proxy_host,
                                self.proxy_port,
                                )
        socket.socket = socks.socksocket

    # ftp登录
    def ftpLogin(self):
        self.ftp = FTP(self.ftp_host)
        self.setDebugLevel(1)  # set_debuglevel(2) #打开调试级别2，显示详细信息
        self.ftp.login(
            user=self.ftp_user,
            passwd=self.ftp_password
        )
        logging.info("Welcome you! %s" % self.ftp.welcome)
        return self.ftp

    def setDebugLevel(self, number):
        self.ftp.set_debuglevel(number)  # 关闭调试

    def close(self):
        self.ftp.quit()