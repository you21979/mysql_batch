#!/usr/bin/python
import datetime
import MySQLdb
import json
import os

CONFIG_FILE="partition.json"

# -----------------------------------
def config_read(filename):
    config = json.load(open(filename))
    return config

# -----------------------------------

def date_show_all_partitions(conn, tablename):
    lists = []
    infotable = "information_schema.PARTITIONS"
    sql = "SELECT PARTITION_NAME FROM "+ infotable +" WHERE TABLE_NAME='"+ tablename +"' ORDER BY PARTITION_NAME desc;"
    cur = conn.cursor()
    cur.execute(sql)
    res = cur.fetchall()
    for row in res:
        lists.append(row[0])
    cur.close()
    return lists

def partition_exec(conn, table):
    lists = date_show_all_partitions(conn, table)
    for v in lists:
        if v == "pmax":
            continue
        print table + ":" + v

def main():
    path = os.path.join(os.path.join(os.path.dirname(__file__), ".."), "config");
    conf = config_read(os.path.join(path, CONFIG_FILE))
    myconf = conf["MYSQL"]
    conn = MySQLdb.connect(host=myconf["HOST"], db=myconf["DB"], user=myconf["USER"], passwd=myconf["PASS"])

    for table in conf["TABLES"]:
        partition_exec(conn, table)

    conn.close()

main()
