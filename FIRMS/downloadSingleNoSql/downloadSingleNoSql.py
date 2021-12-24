# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/22 0022 上午 9:24
# @Function:
#
# from urllib.request import urlretrieve
#

import datetime
import logging
import os

import Common
from getCookies import earthdataLogin

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)


class GetInfos:
    def __init__(self, option):
        self.option = {
            "url": "https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code&client_id=A6th7HB-3EBoO7iOCiCLlA&redirect_uri=https%3A%2F%2Fladsweb.modaps.eosdis.nasa.gov%2Fcallback&state=L2FyY2hpdmUvYWxsRGF0YS8%3D%0A"
        }
        self.earthdata = earthdataLogin(self.option)
        self.wgetLadswebInfo = "1=1"
        self.download_token = ""
        self.savePath = option['savePath']
        self.localtion = option['localtion']
        self.cycleTime = option['cycleTime']
        self.type = option['type']
        self.year = option['year']
        self.startDay = option['startDay']
        self.stopDay = option['stopDay']
        self.file = ''

    def returnLadswebCookies(self):
        try:
            self.earthdata.login(self.option)
            # 立马获取的是登录home获取到的cookies
            cookies_home = self.earthdata.get_cookies
            params_home = {}
            for i in cookies_home:
                params_home[i['name']] = i['value']
            return params_home
        except:
            return {}

    def getWgetLadswebInfo(self):
        try:
            logging.info('into getWgetLadswebInfo')
            # 获取两级的cookies
            cookies_params = self.returnLadswebCookies()
            # 获取下载token
            self.download_token = Common.getLadswebToken(cookies_params)
            logging.info("download_token:[%s]" % self.download_token)
            print("download_token:[%s]" % self.download_token)
            self.setWgetLadswebIo()
        except:
            return ""

    def setWgetLadswebIo(self):

        if self.type == '5000' and self.cycleTime >= 30:
            year_2020 = ['001', '032', '061', '092', '122', '153', '183', '214', '245', '275', '306', '336']

            year_2021 = ['001', '032', '060', '091', '121', '152', '182', '213', '244', '274', '305']
            for day in year_2021:
                contents = self.getInfoList(day)
                logging.info("contents:[%s]" % contents)
                for content in contents:
                    self.wgetInfo = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/" + \
                                    self.type + "/" + self.localtion + "/" + self.year + "/" + str(
                        day) + "/" + content + \
                                    "' --header 'Authorization: Bearer " + self.download_token + "' -P " + self.savePath

                    logging.info("weget:[%s]" % self.wgetInfo)
                    self.downloadInfoFiles()
        else:
            for day in range(self.startDay, self.stopDay + 1, self.cycleTime):
                contents = self.getInfoList(day)
                logging.info("contents:[%s]" % contents)
                for content in contents:
                    self.wgetInfo = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/" + \
                                    self.type + "/" + self.localtion + "/" + self.year + "/" + str(
                        day) + "/" + content + \
                                    "' --header 'Authorization: Bearer " + self.download_token + "' -P " + self.savePath

                    logging.info("weget:[%s]" % self.wgetInfo)
                    self.downloadInfoFiles()

    def getInfoList(self, day):
        url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/" + self.type + "/" + self.localtion + "/" + self.year + "/" + str(
            day) + "/"
        contentsInfos = self.earthdata.getAttrContentsInfo(url, 'a', {'class': 'hide'})
        tile = ['H25V03', 'H24V05', 'H25V04', 'H25V05', 'H27V04', 'H24V04', 'H26V03', 'H26V04', 'H23V04', 'H28V05',
                'H27V05', 'H26V06', 'H25V06', 'H27V06', 'H26V05', 'H28V06', 'H29V06', 'H23V05', 'H28V08', 'H29V08',
                'H28V07', 'H29V07']
        if self.type == '5000':
            tile = ['h25v04', 'h25v05', 'h26v04', 'h26v05', 'h26v06', 'h27v04', 'h27v05', 'h27v06', 'h28v04', 'h28v05',
                    'h28v06', 'h28v07', 'h28v08', 'h29v03', 'h29v04', 'h29v05', 'h29v06', 'h29v07', 'h29v08', 'h30v03',
                    'h30v04', 'h30v05', 'h30v06', 'h31v04']
        contents = []
        for i in tile:
            for j in contentsInfos:
                if i.lower() in j:
                    contents.append(j)
        return contents

    def downloadInfoFiles(self):
        try:
            os.system(self.wgetInfo)
        except:
            logging.info("wget错误代码：[%s]" % os.system(self.wgetLadswebInfo))
            logging.info("UTC Time time start time:[%s]" % (datetime.datetime.utcnow()))
            self.getWgetLadswebInfo()

    def destory(self):
        self.earthdata.closeChrome()


if __name__ == "__main__":
    logging.info("开始时间:[%s]" % (datetime.datetime.now()))
    # 5000/VNP46A2
    option = {
        'type': '5000',
        'localtion': 'VNP46A3',
        'cycleTime': 30,  # 数据更新周期，当时间按周期为月份的时候，这里的时间周期需要大于等于30
        'year': '2021',
        # 'startDay': 100,
        # 'stopDay': 335,
        'startDay': 300,    # 当数据更新周期为一个月或者一年的时候，这里面的开始结束时间就没有意义了
        'stopDay': 339,     # 因为每年存在闰月导致时间不固定，需要手动进行操作
        'savePath': '/home/gaosong/gaosong/Modis/'
    }
    getInfos = GetInfos(option)
    getInfos.getWgetLadswebInfo()
    getInfos.destory()
