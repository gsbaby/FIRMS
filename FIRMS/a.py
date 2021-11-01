# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/25 上午8:59
# @Function:
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

def job():
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# 定义BlockingScheduler
sched = BlockingScheduler()
sched.add_job(job, 'interval', seconds=5)
sched.start()