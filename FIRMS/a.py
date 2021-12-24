# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/25 上午8:59
# @Function:
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options

# def job():
#     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# # 定义BlockingScheduler
# sched = BlockingScheduler()
# sched.add_job(job, 'interval', seconds=5)
# sched.start()

url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD09A1/2020/273/"
driver = webdriver.Chrome()  # 你的google浏览器驱动存放路径
driver.get(url)

# selenium获取当前页面源码
html = driver.page_source
# BeautifulSoup转换页面源码
bs = BeautifulSoup(html, 'lxml')

# 解析获得Ifream组件
downloadIfream = bs.find_all('a', attrs={'class': 'hide'})

contents = []
for i in downloadIfream:
    contents.append(i.contents[0])

print(contents)