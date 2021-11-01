# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/22 0022 上午 11:07
# @Function:

import datetime
import time
import requests

def getToken(params, url="https://nrt4.modaps.eosdis.nasa.gov/oauth/key"):
    cookie = "urs_guid_ops=" + params["urs_guid_ops"] + ";nrt4-oauth2-auth=" + params["nrt4-oauth2-auth"]
    headers = {
        "cookie": cookie,
        "origin": "https://nrt4.modaps.eosdis.nasa.gov",
        "referer": "https://nrt4.modaps.eosdis.nasa.gov/profile/app-keys",
        "sec-ch-ua": "'Not A;Brand';v='99', 'Chromium';v='90', 'Google Chrome';v='90'",
        "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    }
    proxies = {'http': None, 'https': None}
    res = requests.post(url=url, headers=headers, proxies=proxies)
    return res.text

def cal_date_str_spilt():
    ''''
    处理形如"2020-3-26"
    使用字符串的spilt方法解析
    '''
    # d = datetime.date.today()
    d = datetime.datetime.utcnow()
    return [d.year, d.month, d.day]
def judge_leap_year(year, month):
    # 只有闰年且202, 2001:4d0:241a:月份大于2月才加多一天
    if year % 400 == 0 or year % 100 and year % 4 == 0 and month > 2:
        return 1
    else:
        return 0

def getTodays():
    sum_1 = 0
    date_list_1 = cal_date_str_spilt()
    month_day = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    sum_1 += sum(month_day[:date_list_1[1] - 1]) + date_list_1[2] + judge_leap_year(date_list_1[0], date_list_1[1])
    return sum_1

def readlineTxt(path):
    with open(path, "r") as f:
        tempdata=[]
        data = []
        for line in f.readlines():
            line = line.strip('\n')
            tempdata.append(line)
        for i in tempdata[1:]:
            data.append(i.split(","))
        return data
