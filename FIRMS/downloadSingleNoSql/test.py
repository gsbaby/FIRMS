# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/12/10 上午10:44
# @Function:
import os

a = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD09A1/2020/329/MOD09A1.A2020329.h29v07.061.2020357143524.hdf' --header 'Authorization: Bearer anVzdGxpa2ViYWJ5OloyRnZjMjl1WjJkaGIzTnZibWRBYjNWMGJHOXZheTVqYjIwPToxNjM5MDk4Njg3OmY4ZGY0NzJjYjQ3MzBhM2M3ZGFmNmVmOWJjYjA3NjRkNzAyMTJhNjY' -P /home/gaosong/"

os.system(a)