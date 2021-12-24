# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/21 0021 下午 5:24
# @Function:

import logging
import time

# coding:utf-8
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# 存放日志地点
LOG_FILE_NAME = "./log.log"
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.ERROR)


class earthdataLogin():
    def __init__(self, option):
        options = Options()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
        options.add_argument('window-size=1920x3000')  # 设置浏览器分辨率
        options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        options.add_argument('--hide-scrollbars')  # 隐藏滚动条，应对一些特殊页面
        options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片，提升运行速度
        options.add_argument('--headless')  # 浏览器不提供可视化界面。Linux下如果系统不支持可视化不加这条会启动失败

        self.url = option["url"]
        # self.url = "https://urs.earthdata.nasa.gov/home"
        self.driverChrome = webdriver.Chrome(options=options)  # 你的google浏览器驱动存放路径

    def login(self, option={"url":""}):
        self.url = option["url"]
        self.driverChrome.get(self.url)
        logging.info("Open Url!")
        username = self.driverChrome.find_element_by_id("username")
        username.send_keys("username")
        password = self.driverChrome.find_element_by_id("password")
        password.send_keys('password')
        self.driverChrome.find_element_by_name('commit').click()
        logging.info("Login In!")
        time.sleep(10)  # 等待浏览器加载10秒

    def goUrl(self, url):
        self.driverChrome.get(url)


    def getAttrContentsInfo(self, url, lable, attrs):
        self.driverChrome.get(url)
        # selenium获取当前页面源码
        html = self.driverChrome.page_source
        # BeautifulSoup转换页面源码
        bs = BeautifulSoup(html, 'lxml')
        # 解析获得Ifream组件
        downloadIfream = bs.find_all(lable, attrs=attrs)
        contents = []
        for i in downloadIfream:
            contents.append(i.contents[0])
        return contents

    @property
    def get_cookies(self):
        cookies = self.driverChrome.get_cookies()
        return cookies

    def closeChrome(self):
        self.driverChrome.close()
