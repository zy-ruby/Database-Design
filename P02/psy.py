#!/usr/bin/python
# !/usr/bin/python
import psycopg2
import os
import sys
import datetime
import json
from types import *
import argparse

conn = psycopg2.connect("dbname=flightsskewed user=ubuntu password=ubuntu")
curs = conn.cursor()

with open("example.json") as f:
    for line in f:
        dic = json.loads(line)
        if 'newcustomer' in dic:
            cus = dic['newcustomer']
            ffLong =  cus['frequentflieron']
            sqlQuery = "SELECT airlineid FROM airlines WHERE airlines.name='%s'" %ffLong
            curs.execute(sqlQuery)
            ffShort = curs.fetchone()
            ffs = str(ffShort[0])
            sqlInsertC = "INSERT INTO customers VALUES ('%s','%s',to_date('%s','yyyy-mm-dd'),'%s')"%(cus['customerid'],cus['name'],cus['birthdate'],ffs)
            curs.execute(sqlInsertC)


        if 'flightinfo' in dic:
            flInfo = dic['flightinfo']
            custs = flInfo['customers']
            for cust in custs:
            	if 'name' in cust:
            		sqlIsrtNewC = "INSERT INTO customers VALUES ('%s','%s', to_date('%s','yyyy-mm-dd'),'%s')" %(cust['customerid'],cust['name'],cust['birthdate'],cust['frequentflieron'])
            		curs.execute(sqlIsrtNewC)

            	sqlIsrtF = "INSERT INTO flewon VALUES ('%s','%s',to_date('%s','YYYY-MM-DD'))" %(flInfo['flightid'],cust['customerid'],flInfo['flightdate'])
            	curs.execute(sqlIsrtF)

conn.commit()
curs.close()
conn.close()
