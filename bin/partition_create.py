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

def partition_create_by_day(day):
    pname = day + datetime.timedelta(0)
    prange = day + datetime.timedelta(1)
    strname = pname.strftime('p%Y%m%d')
    strrange = prange.strftime('%Y-%m-%d')
    return "".join(("PARTITION ", strname, " VALUES LESS THAN (TO_DAYS('", strrange, "')) ENGINE=InnoDB"))

def partition_create_by_range(baseday, count):
    list = []
    for i in range(count):
	    list.append(partition_create_by_day(baseday + datetime.timedelta(i)))
    return list

def partition_make_sql(tablename, list):
    sql = ""
    head = "ALTER TABLE " + tablename + " REORGANIZE PARTITION pmax INTO (\n"
    tail = "PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE=InnoDB);\n"
    sql = head
    for item in list:
        sql = sql + " " + item + ",\n"
    sql = sql + " " + tail
    return sql

def date_show_last_partition(conn, tablename):
    infotable = "information_schema.PARTITIONS"
    sql = "SELECT PARTITION_NAME FROM "+ infotable +" WHERE TABLE_NAME='"+ tablename +"' ORDER BY PARTITION_NAME desc limit 2;"
    cur = conn.cursor()
    cur.execute(sql)
    res = cur.fetchall()
    name = res[1][0][1:]
    cur.close()
    return name

def date_show_today():
    return datetime.date.today().strftime("%Y%m%d")

def partition_exec(conn, table, createcount):
    last = datetime.datetime.strptime(date_show_last_partition(conn, table), "%Y%m%d")
    today = datetime.datetime.strptime(date_show_today(), "%Y%m%d")
    diff = last - today
    count = createcount - diff.days
    if(count > 0):
        sql_exec(conn, partition_make_sql(table, partition_create_by_range(last + datetime.timedelta(1), count)))

def main():
    path = os.path.join(os.path.join(os.path.dirname(__file__), ".."), "config");
    conf = config_read(os.path.join(path, CONFIG_FILE))
    myconf = conf["MYSQL"]
    createcount = conf["COUNT"]
    conn = MySQLdb.connect(host=myconf["HOST"], db=myconf["DB"], user=myconf["USER"], passwd=myconf["PASS"])

    for table in conf["TABLES"]:
        partition_exec(conn, table, createcount)

    conn.close()

main()
