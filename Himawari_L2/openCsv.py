# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/27 上午11:37
# @Function:
import csv
import math

import psycopg2


class readCsvFile:
    def __init__(self):
        self.data = []

    def readFile(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as csvFile:
            reader = csv.reader(csvFile)
            data = []
            for i in reader:
                data.append(i[1:])
        return data[2:]

    def connectDatabase(self, table, ispoint=True):
        # 数据库连接参数
        conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
        cur = conn.cursor()
        selectExistsTable = '''select exists(select * from information_schema.tables where table_name='{}')'''.format(
            table.lower())
        cur.execute(selectExistsTable)
        ishave = cur.fetchone()[0]
        if ishave:
            cur.execute("drop table {}".format(table.lower()))
            conn.commit()
        if ispoint:
            createUserListTable = '''CREATE TABLE IF NOT EXISTS {} (pid serial PRIMARY KEY,year varchar(20) DEFAULT NULL,month varchar(20) DEFAULT NULL,day varchar(20) DEFAULT NULL,time varchar(20) DEFAULT NULL,lat varchar(20) DEFAULT NULL,lon varchar(20) DEFAULT NULL,area varchar(20) DEFAULT NULL,volcano varchar(20) DEFAULT NULL,level varchar(20) DEFAULT NULL,reliability varchar(20) DEFAULT NULL,frp varchar(20) DEFAULT NULL,qf varchar(20) DEFAULT NULL,hot varchar(20) DEFAULT NULL,geom geometry(POINT,4326))'''.format(
                table.lower())
        else:
            createUserListTable = '''CREATE TABLE IF NOT EXISTS {} (pid serial PRIMARY KEY,year varchar(20) DEFAULT NULL,month varchar(20) DEFAULT NULL,day varchar(20) DEFAULT NULL,time varchar(20) DEFAULT NULL,lat varchar(20) DEFAULT NULL,lon varchar(20) DEFAULT NULL,area varchar(20) DEFAULT NULL,volcano varchar(20) DEFAULT NULL,level varchar(20) DEFAULT NULL,reliability varchar(20) DEFAULT NULL,frp varchar(20) DEFAULT NULL,qf varchar(20) DEFAULT NULL,hot varchar(20) DEFAULT NULL,geom geometry(POLYGON,4326))'''.format(
                table.lower())
        cur.execute(createUserListTable)
        conn.commit()
        cur.close()
        conn.close()

    # 通过某字段长度进行四至边界范围的确定
    def getPolygonInfo_byAttribute(self, data):
        polygon = []
        for i in range(len(data)):
            Area = float(data[i][6])
            scan = math.sqrt(Area)
            track = scan
            # polygon的经纬度与point的xy位置颠倒了下
            longitude = float(track) / 222.0
            latitude = float(scan) / (222.0 * math.cos(float(data[i][4]) * math.pi / 180))
            lat = float(data[i][5])
            lon = float(data[i][4])
            polygon.append(
                [lat - latitude, lon - longitude, lat - latitude, lon + longitude, lat + latitude, lon + longitude,
                 lat + latitude, lon - longitude, lat - latitude, lon - longitude])
        return polygon

    def getPoint(self, data, table, count):
        geomInfo = []
        for i in range(len(data)):
            geom = "update {} set geom = ST_GeomFromText('POINT({} {})', 4326) where pid = {}".format(table.lower(),
                                                                                                      data[i][5],
                                                                                                      data[i][4],
                                                                                                      i + count + 1)
            geomInfo.append(geom)
        return geomInfo

    def getPolygon(self, data, table, count):
        geomInfo = []
        polygonGem = self.getPolygonInfo_byAttribute(data)
        for i in range(len(polygonGem)):
            geom = "update {} set geom = ST_GeomFromText('POLYGON(({} {},{} {},{} {},{} {},{} {}))', 4326) where pid = {}".format(
                table.lower(),
                polygonGem[i][0], polygonGem[i][1], polygonGem[i][2], polygonGem[i][3], polygonGem[i][4],
                polygonGem[i][5], polygonGem[i][6], polygonGem[i][7], polygonGem[i][8], polygonGem[i][9],
                i + count + 1
            )
            geomInfo.append(geom)
        return geomInfo

    def insertInto(self, data, table, ispoint=True):
        # 数据库连接参数
        conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
        cur = conn.cursor()
        selectCount = "select count(*) from {}".format(table.lower())
        cur.execute(selectCount)
        count = cur.fetchone()[0]
        insertSql = "INSERT INTO {} (year,month,day,time,lat,lon,area,volcano,level,reliability,frp,qf,hot)" \
                    " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table.lower())
        cur.executemany(insertSql, data)
        conn.commit()
        if ispoint:
            geomInfo = self.getPoint(data, table, count)
        else:
            geomInfo = self.getPolygon(data, table, count)
        for update in geomInfo:
            cur.execute(update)
            conn.commit()
        cur.close()
        conn.close()

    def insert_db_connect_table(self, table):
        # 数据库连接参数
        conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
        cur = conn.cursor()
        insertSql = "INSERT INTO db_connect_table (table_name,isGeom) VALUES('{}',1)".format(table.lower())
        cur.execute(insertSql)
        conn.commit()
        cur.close()
        conn.close()

# if __name__ == '__main__':
#     tableName = 'H08_20211027_0620_L2WLF010_FLDK'
#     tablePath = 'H08_20211027_0620_L2WLF010_FLDK'
#     fileCSVPath = "/run/user/1000/gvfs/afp-volume:host=RS.local,user=admin,volume=RS_GIS/RS_DATA/Himawari/L2/WLF/010/202110/27/06/" + tablePath + '.06001_06001.csv'
#     readcsvfile = readCsvFile()
#     data = readcsvfile.readFile(fileCSVPath)
#     readcsvfile.connectDatabase(tableName, True)
#     readcsvfile.insertInto(data, tableName, True)
