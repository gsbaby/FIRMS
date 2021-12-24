# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/22 0022 上午 9:24
# @Function:

import datetime
import logging
import os
import time
from threading import Timer

from apscheduler.schedulers.blocking import BlockingScheduler

import Common
from database import sqlFunction
from getCookies import earthdataLogin

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)


class MyTimer(object):
    def __init__(self, start_time, interval, callback_proc, args=None, kwargs=None):
        self.__timer = None
        self.__start_time = start_time
        self.__interval = interval
        self.__callback_pro = callback_proc
        self.__args = args if args is not None else []
        self.__kwargs = kwargs if kwargs is not None else {}

    def exec_callback(self, args=None, kwargs=None):
        self.__callback_pro(*self.__args, **self.__kwargs)
        self.__timer = Timer(self.__interval, self.exec_callback)
        self.__timer.start()

    def start(self):
        interval = self.__interval - (datetime.datetime.now().timestamp() - self.__start_time.timestamp())
        self.__timer = Timer(interval, self.exec_callback)
        self.__timer.start()

    def cancel(self):
        self.__timer.cancel()
        self.__timer = None


class GetInfos:
    def __init__(self):
        self.option = {
            "url": "https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code&client_id=mncVv733h6TakcPFwjBC9w&redirect_uri=https%3A%2F%2Fnrt4.modaps.eosdis.nasa.gov%2Fcallback&state=L2FyY2hpdmUvRklSTVM%3D%0A"
        }
        self.earthdata = earthdataLogin(self.option)
        self.wgetInfo = "1=1"
        self.download_token = ""
        self.today = ''
        self.savePath = '/run/user/1000/gvfs/afp-volume:host=RS.local,user=admin,volume=RS_GIS/RS_DATA/MODIS'
        self.localtion = 'Russia_Asia'

    def returnCookies(self):
        try:
            self.earthdata.login(self.option)
            # 立马获取的是登录home获取到的cookies
            cookies_home = self.earthdata.get_cookies
            params_home = {}
            for i in cookies_home:
                params_home[i['name']] = i['value']
            logging.info("home cookies:[%s]" % params_home)
            logging.info("Waiting and Jumping!")
            time.sleep(5)  # 或者等待5s
            # 等待页面跳转后立马获取的是登录home获取到的cookies
            cookies_nrt4 = self.earthdata.get_cookies
            params = {}
            for i in cookies_nrt4:
                params[i['name']] = i['value']
            logging.info("nrt4 cookies:[%s]" % params)
            return params
        except:
            return {}

    def getWgetInfo(self):
        try:
            logging.info('into getWgetInfo')
            # 获取两级的cookies
            cookies_params = self.returnCookies()
            # 获取下载token
            self.download_token = Common.getToken(cookies_params)
            logging.info("download_token:[%s]" % self.download_token)
            self.destory()
            # d = datetime.date.today()
            d = datetime.datetime.utcnow()
            day = Common.getTodays()
            logging.info("utc time:[%s]" % (str(d) + str(day)))
            # 获取wget参数
            self.today = str(d.year) + str(day)
            self.setWgetIo()
        except:
            return ""

    def setWgetIo(self):
        self.wgetInfo = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=4 'https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/modis-c6.1/" + \
                        self.localtion + "/MODIS_C6_1_" + self.localtion + "_MCD14DL_NRT_" + self.today + ".txt' --header 'Authorization: Bearer " + self.download_token + "' -P " + self.savePath

    def goGetInfos(self):
        if os.system(self.wgetInfo) == 0 or os.system(self.wgetInfo) == 2048:
            day = Common.getTodays()
            d = datetime.datetime.utcnow()
            self.today = str(d.year) + str(day)
            logging.info(self.today)
            self.setWgetIo()
            logging.info("更新文件:MODIS_C6_1_Russia_Asia_MCD14DL_NRT_[%s].txt" % self.today)
            # # 关闭浏览器
            # self.earthdata.closeChrome()
            time.sleep(5)
            filePath = self.savePath + "/FIRMS/modis-c6.1/" + self.localtion + "/MODIS_C6_1_" + self.localtion + "_MCD14DL_NRT_" + self.today + ".txt"
            tableName = 'FIRMS_Modis_' + self.localtion + '_' + self.today
            tableName_polygon = 'FIRMS_Modis_' + self.localtion + '_' + self.today + '_polygon'
            if os.path.exists(filePath):
                logging.info(self.wgetInfo)
                logging.info("创建点面数据表!")
                sqlFunction.connectDatabase(tableName, True)
                sqlFunction.connectDatabase(tableName_polygon, False)
                data = Common.readlineTxt(filePath)
                logging.info("插入点面数据及空间信息!")
                sqlFunction.insertInto(data, tableName, True)
                sqlFunction.insertInto(data, tableName_polygon, False)
                sqlFunction.insert_db_connect_table(tableName)
                sqlFunction.insert_db_connect_table(tableName_polygon)
            else:
                logging.info("无当前文件:MODIS_C6_1_Russia_Asia_MCD14DL_NRT_[%s].txt" % self.today)
        else:
            logging.info("wget错误代码：[%s]" % os.system(self.wgetInfo))
            logging.info("UTC Time time start time:[%s]" % (datetime.datetime.utcnow()))
            self.getWgetInfo()
            self.goGetInfos()

    def destory(self):
        self.earthdata.closeChrome()


def getInfosFun():
    logging.info("本次执行开始时间:[%s]" % (datetime.datetime.now()))
    # getInfos = GetInfos()
    getInfos.goGetInfos()


if __name__ == "__main__":
    # getInfosFun()

    # # 使用Timer会出现lock.acquire()的问题
    # start = datetime.datetime.now()  # start = datetime.datetime.now().replace(minute=3, second=0, microsecond=0) # 为每个小时的 3 分钟时候被执行一次
    # tmr = MyTimer(start, 10 * 60, getInfosFun)  # 每30min执行一次
    # tmr.start()
    # # tmr.cancel()  #停止

    logging.info("开始时间:[%s]" % (datetime.datetime.now()))
    getInfos = GetInfos()
    # getInfos.goGetInfos()
    # 解决定时任务的问题
    scheduler = BlockingScheduler()
    # scheduler.add_job(getInfosFun, 'interval', seconds=60)  # date: 特定的时间点触发；interval: 固定时间间隔触发；cron: 在特定时间周期性地触发
    scheduler.add_job(getInfosFun, 'interval', minutes=30)  # date: 特定的时间点触发；interval: 固定时间间隔触发；cron: 在特定时间周期性地触发
    scheduler.start()
