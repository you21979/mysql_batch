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

def sql_exec(conn,sql):
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
# -----------------------------------

def partition_drop_by_day(day):
    pname = day + datetime.timedelta(0)
    strname = pname.strftime('p%Y%m%d')
    return strname

def partition_drop_by_range(baseday, count):
    list = []
    for i in range(count):
	    list.append(partition_drop_by_day(baseday + datetime.timedelta(i)))
    return list

def partition_make_drop_sql(tablename, list):
    head = "ALTER TABLE " + tablename + " DROP PARTITION"
    tail = ";\n"
    sql = ""
    for item in list:
        sql = sql + head + " " + item + tail
    return sql

def date_show_head_partition(conn, tablename):
    infotable = "information_schema.PARTITIONS"
    sql = "SELECT PARTITION_NAME FROM "+ infotable +" WHERE TABLE_NAME='"+ tablename +"' AND PARTITION_NAME!='pmax' ORDER BY PARTITION_NAME asc limit 1;"
    cur = conn.cursor()
    cur.execute(sql)
    res = cur.fetchall()
    if len(res) == 0:
        return date_show_today()
    name = res[0][0][1:]
    cur.close()
    return name

def date_show_today():
    return datetime.date.today().strftime("%Y%m%d")

def partition_exec(conn, table, expire):
    head = datetime.datetime.strptime(date_show_head_partition(conn, table), "%Y%m%d")
    today = datetime.datetime.strptime(date_show_today(), "%Y%m%d")
    diff = today - head
    count = diff.days - expire
    if(count > 0):
        sql_exec(conn, partition_make_drop_sql(table, partition_drop_by_range(head, count)))

def main():
    path = os.path.join(os.path.join(os.path.dirname(__file__), ".."), "config");
    conf = config_read(os.path.join(path, CONFIG_FILE))
    myconf = conf["MYSQL"]
    createcount = conf["RESERVE_COUNT"]
    conn = MySQLdb.connect(host=myconf["HOST"], db=myconf["DB"], user=myconf["USER"], passwd=myconf["PASS"])

    for table in conf["TABLES"]:
        partition_exec(conn, table["NAME"], table["EXPIRE"])

    conn.close()

main()

#ALTER TABLE users_log DROP PARTITION p20130603;
