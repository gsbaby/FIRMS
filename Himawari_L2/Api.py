# -*- coding: utf-8 -*-
# @Author  : GaoSong
# @Time    : 2021/10/29 上午11:09
# @Function: 

import psycopg2
from flask import Flask, jsonify, request

app = Flask(__name__)


def responseInfo(response):
    response = jsonify(results=[response])
    response.headers['Access-Control-Allow-Headers'] = '*'
    return response


def connectPostGIS(sql):
    # 数据库连接参数
    conn = psycopg2.connect(database="nyc", user="postgres", password="postgres", host="192.168.5.89", port="5432")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


@app.route('/firms/getGeomTables')
def getGeomTables():
    response = {"status": "normal"}
    try:
        results = connectPostGIS('select * from db_connect_table order by table_name')
        # results = connectPostGIS('select * from db_connect_table order by table_name desc')
        result = []
        for i in results:
            result.append({
                'table_name': i[0],
                'isgeom': i[1],
            })
        response = {"status": "success"}
        response["code"] = 0
        response = {"data": result}
    except:
        response["code"] = 500
        response = {"status": "fail"}

    response = responseInfo(response)
    return response


# SELECT pid,ST_AsGeoJSON(geom) AS geom FROM h08_20211028_1950_l2wlf010_fldk
@app.route('/firms/getTableGeoJson', methods=['POST'])
def getTableGeoJson():
    instance = request.json
    response = {"status": "normal"}
    table_name = instance["table_name"]
    try:
        results = getGeoJson(table_name)
        response = {"status": "success"}
        response["code"] = 0
        response = {"data": {"GeoJson": results}}
    except:
        print('{}有误'.format(table_name))
        response["code"] = 500
        response = {"status": "fail"}
        response = {"message": "查询数据表不存在！"}

    response = responseInfo(response)
    return response


# SELECT pid,ST_AsGeoJSON(ST_Transform(ST_Buffer(geom, 100,'quad_segs=8'),4326)) AS geom FROM h08_20211028_1950_l2wlf010_fldk;
@app.route('/firms/getBufferTableGeoJson', methods=['POST'])
def getBufferTableGeoJson():
    instance = request.json
    response = {"status": "normal"}
    table_name = instance["table_name"]
    distance = instance["distance"]
    try:
        results = connectPostGIS(
            "SELECT pid,ST_AsGeoJSON(ST_Transform(ST_Buffer(geom, {},'quad_segs=8'),4326))::json AS geom from {}".format(
                distance, table_name))
        response = {"status": "success"}
        response["code"] = 0
        response = {"data": {"GeoJson": results}}
    except:
        print('{}有误'.format(table_name))
        response["code"] = 500
        response = {"status": "fail"}
        response = {"message": "查询数据表不存在！"}
    response = responseInfo(response)
    return response


def getChinaFeature():
    sql = "select ST_AsGeoJSON(geom)::json as geometry from china t"
    sql_infomation_all = connectPostGIS(sql)
    coordinates = sql_infomation_all[0][0]['coordinates']
    strinfo = []
    for i in coordinates:
        for j in i[0]:
            strinfo.append(" ".join('%s' % id for id in j))
    strii = "(" + ",".join(strinfo) + ")"
    return strii


def getGeoJson(table_name):
    # 获取中国区域范围
    strii = getChinaFeature()
    # 1.获取表中所有字段信息  ****暂时不需要
    # sql_attribute = "SELECT a.attname as name FROM pg_class as c,pg_attribute as a where c.relname = '{}' and a.attrelid = c.oid and a.attnum>0".format(
    #     table_name)
    # resultAttributes = connectPostGIS(sql_attribute)
    # 2.获取数据中的空间信息和属性信息
    if "modis" in table_name:
        # MODIS
        sql_infomation = "select 'Feature' as type,ST_AsGeoJSON(geom)::json as geometry,json_strip_nulls(row_to_json(row(pid,latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight)))::json as properties from {} t where ST_Intersects(t.geom,ST_GeomFromText('polygon(" + strii + ")',4326))"
        sql_infomation = sql_infomation.format(table_name)
    else:
        # 葵花
        sql_infomation = "select 'Feature' as type,ST_AsGeoJSON(geom)::json as geometry,json_strip_nulls(row_to_json(row(pid,year,month,day,time,lat,lon,area,volcano,level,reliability,frp,qf,hot)))::json as properties from {} t where ST_Intersects(t.geom,ST_GeomFromText('polygon(" + strii + ")',4326))"
        sql_infomation = sql_infomation.format(table_name)
    sql_infomation_all = connectPostGIS(sql_infomation)
    results = {
        "type": "FeatureCollection"
    }
    features = []
    for i in sql_infomation_all:
        properties = {
            "pid": i[2]['f1'],
            "year": i[2]['f2'],
            "month": i[2]['f3'],
            "day": i[2]['f4'],
            "time": i[2]['f5'],
            "lat": i[2]['f6'],
            "lon": i[2]['f7'],
            "area": i[2]['f8'],
            "volcano": i[2]['f9'],
            "level": i[2]['f10'],
            "reliability": i[2]['f11'],
            "frp": i[2]['f12'],
            "qf": i[2]['f13'],
            "hot": i[2]['f14']
        }
        features.append({
            "type": i[0],
            "properties": properties,
            "geometry": i[1]
        })
    results["features"] = features
    return results


if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, port=16666)
