#!/usr/bin/python
"""
   Copyright 2013 yuki akiyama (you2197901 at gmail.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
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
    sql = "SELECT PARTITION_NAME FROM "+ infotable +" WHERE TABLE_NAME='"+ tablename +"' AND PARTITION_NAME!='pmax' ORDER BY PARTITION_NAME desc;"
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
        print table + ":" + v

def main():
    path = os.path.join(os.path.join(os.path.dirname(__file__), ".."), "config");
    conf = config_read(os.path.join(path, CONFIG_FILE))
    myconf = conf["MYSQL"]
    conn = MySQLdb.connect(host=myconf["HOST"], db=myconf["DB"], user=myconf["USER"], passwd=myconf["PASS"])

    for table in conf["TABLES"]:
        partition_exec(conn, table["NAME"])

    conn.close()

main()
