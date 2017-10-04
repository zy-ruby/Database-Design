#!/usr/bin/python
import psycopg2
import os
import sys

if len(sys.argv) != 2:
    print("USAGE: {:s} <your file>".format(sys.argv[0]))
    exit()
    
def executePrint(s):
	cur.execute(s)
	print cur.fetchall()

conn = psycopg2.connect("dbname=flightsskewed user=ubuntu")
cur = conn.cursor()

# run code
os.system("python {:s}".format(sys.argv[1]))


## Add a customer who doesn't exist
print "==== Testing first json update"
executePrint("select * from customers where customerid = 'cust1000'")

print "==== Testing second json update"
executePrint("select * from flewon where flightid = 'DL108' and flightdate='2015-09-25'")
executePrint("select * from customers where customerid = 'cust1001'")
