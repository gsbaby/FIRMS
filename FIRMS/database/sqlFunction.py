# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/23 下午2:03
# @Function:

import math

import psycopg2

import Common


def connectDatabase(table, ispoint=True):
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
        createUserListTable = '''CREATE TABLE IF NOT EXISTS {} (pid serial PRIMARY KEY,Latitude varchar(20) DEFAULT NULL,Longitude varchar(20) DEFAULT NULL,Brightness varchar(20) DEFAULT NULL,Scan varchar(20) DEFAULT NULL,Track varchar(20) DEFAULT NULL,Acq_Date varchar(20) DEFAULT NULL,Acq_Time varchar(20) DEFAULT NULL,Satellite varchar(20) DEFAULT NULL,Confidence varchar(20) DEFAULT NULL,Version varchar(20) DEFAULT NULL,Bright_T31 varchar(20) DEFAULT NULL,FRP varchar(20) DEFAULT NULL,Type varchar(20) DEFAULT NULL,DayNight varchar(20) DEFAULT NULL,geom geometry(POINT,4326))'''.format(
            table.lower())
    else:
        createUserListTable = '''CREATE TABLE IF NOT EXISTS {} (pid serial PRIMARY KEY,Latitude varchar(20) DEFAULT NULL,Longitude varchar(20) DEFAULT NULL,Brightness varchar(20) DEFAULT NULL,Scan varchar(20) DEFAULT NULL,Track varchar(20) DEFAULT NULL,Acq_Date varchar(20) DEFAULT NULL,Acq_Time varchar(20) DEFAULT NULL,Satellite varchar(20) DEFAULT NULL,Confidence varchar(20) DEFAULT NULL,Version varchar(20) DEFAULT NULL,Bright_T31 varchar(20) DEFAULT NULL,FRP varchar(20) DEFAULT NULL,Type varchar(20) DEFAULT NULL,DayNight varchar(20) DEFAULT NULL,geom geometry(POLYGON,4326))'''.format(
            table.lower())
    cur.execute(createUserListTable)
    conn.commit()
    cur.close()
    conn.close()

# 通过某字段长度进行四至边界范围的确定
def getPolygonInfo_byAttribute(data):
    polygon = []
    for i in range(len(data)):
        scan = data[i][3]
        track = data[i][4]
        # polygon的经纬度与point的xy位置颠倒了下
        longitude = float(track) / 222.0
        latitude = float(scan) / (222.0 * math.cos(float(data[i][0]) * math.pi / 180))
        lat = float(data[i][1])
        lon = float(data[i][0])
        polygon.append(
            [lat - latitude, lon - longitude, lat - latitude, lon + longitude, lat + latitude, lon + longitude,
             lat + latitude, lon - longitude, lat - latitude, lon - longitude])
    return polygon

def getPolygonInfo(data):
    polygon = []
    for i in range(len(data)):
        scan = data[i][3]
        track = data[i][4]
        # polygon的经纬度与point的xy位置颠倒了下
        longitude = 1 / 222.0
        latitude = 1 / (222.0 * math.cos(float(data[i][0]) * math.pi / 180))
        lat = float(data[i][1])
        lon = float(data[i][0])
        polygon.append(
            [lat - latitude, lon - longitude, lat - latitude, lon + longitude, lat + latitude, lon + longitude,
             lat + latitude, lon - longitude, lat - latitude, lon - longitude])
    return polygon


def getPoint(data, table, count):
    geomInfo = []
    for i in range(len(data)):
        geom = "update {} set geom = ST_GeomFromText('POINT({} {})', 4326) where pid = {}".format(table.lower(),
                                                                                                  data[i][1],
                                                                                                  data[i][0],
                                                                                                  i + count + 1)
        geomInfo.append(geom)
    return geomInfo


def getPolygon(data, table, count):
    geomInfo = []
    polygonGem = getPolygonInfo(data)
    for i in range(len(polygonGem)):
        geom = "update {} set geom = ST_GeomFromText('POLYGON(({} {},{} {},{} {},{} {},{} {}))', 4326) where pid = {}".format(
            table.lower(),
            polygonGem[i][0], polygonGem[i][1], polygonGem[i][2], polygonGem[i][3], polygonGem[i][4],
            polygonGem[i][5], polygonGem[i][6], polygonGem[i][7], polygonGem[i][8], polygonGem[i][9],
            i + count + 1
        )
        geomInfo.append(geom)
    return geomInfo


def insertInto(data, table, ispoint=True):
    # 数据库连接参数
    conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
    cur = conn.cursor()
    selectCount = "select count(*) from {}".format(table.lower())
    cur.execute(selectCount)
    count = cur.fetchone()[0]
    insertSql = "INSERT INTO {} (Latitude,Longitude,Brightness,Scan,Track,Acq_Date,Acq_Time,Satellite,Confidence,Version,Bright_T31,FRP,DayNight)" \
                " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table.lower())
    cur.executemany(insertSql, data)
    conn.commit()
    if ispoint:
        geomInfo = getPoint(data, table, count)
    else:
        geomInfo = getPolygon(data, table, count)
    for update in geomInfo:
        cur.execute(update)
        conn.commit()
    cur.close()
    conn.close()

def insert_db_connect_table(table):
    # 数据库连接参数
    conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
    cur = conn.cursor()

    isSqlTableName = "select * from db_connect_table where table_name = '{}'".format(table.lower())
    cur.execute(isSqlTableName)
    isConnectTableName = cur.fetchone()[0]
    if isConnectTableName:
        pass
    else:
        insertSql = "INSERT INTO db_connect_table (table_name,isGeom) VALUES('{}',1)".format(table.lower())
        cur.execute(insertSql)
        conn.commit()
    cur.close()
    conn.close()


# if __name__ == "__main__":
#     tableName = 'firms_modis_russia_asia_2021298_polygon'
#     connectDatabase(tableName, False)
#     data = Common.readlineTxt(
#         "/run/user/1000/gvfs/afp-volume:host=RS.local,user=admin,volume=RS_GIS/RS_DATA/MODIS/FIRMS/modis-c6.1/Russia_Asia/MODIS_C6_1_Russia_Asia_MCD14DL_NRT_2021298.txt")
#     insertInto(data, tableName, False)
