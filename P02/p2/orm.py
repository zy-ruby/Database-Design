from peewee import *
import psycopg2
import os
import sys
import datetime
import json
from types import *
import argparse

database = PostgresqlDatabase('flightsskewed', **{'password': 'Sherry0228!', 'user': 'ubuntu'})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class Airports(BaseModel):
    airportid = CharField(primary_key=True)
    city = CharField(null=True)
    name = CharField(null=True)
    total2011 = IntegerField(null=True)
    total2012 = IntegerField(null=True)

    class Meta:
        db_table = 'airports'

class Airlines(BaseModel):
    airlineid = CharField(primary_key=True)
    hub = ForeignKeyField(db_column='hub', null=True, rel_model=Airports, to_field='airportid')
    name = CharField(null=True)

    class Meta:
        db_table = 'airlines'

class Customers(BaseModel):
    birthdate = DateField(null=True)
    customerid = CharField(primary_key=True)
    frequentflieron = ForeignKeyField(db_column='frequentflieron', null=True, rel_model=Airlines, to_field='airlineid')
    name = CharField(null=True)

    class Meta:
        db_table = 'customers'

class Flights(BaseModel):
    airlineid = ForeignKeyField(db_column='airlineid', null=True, rel_model=Airlines, to_field='airlineid')
    dest = ForeignKeyField(db_column='dest', null=True, rel_model=Airports, to_field='airportid')
    flightid = CharField(primary_key=True)
    local_arrival_time = TimeField(null=True)
    local_departing_time = TimeField(null=True)
    source = ForeignKeyField(db_column='source', null=True, rel_model=Airports, related_name='airports_source_set', to_field='airportid')

    class Meta:
        db_table = 'flights'

class Flewon(BaseModel):
    customerid = ForeignKeyField(db_column='customerid', null=True, rel_model=Customers, to_field='customerid')
    flightdate = DateField(null=True)
    flightid = ForeignKeyField(db_column='flightid', null=True, rel_model=Flights, to_field='flightid')

    class Meta:
        db_table = 'flewon'

class Numberofflightstaken(BaseModel):
    customerid = CharField(null=True)
    customername = CharField(null=True)
    numflights = BigIntegerField(null=True)

    class Meta:
        db_table = 'numberofflightstaken'

with open("example.json") as f:
    for line in f:
        dic = json.loads(line)
        if 'newcustomer' in dic:
            cus = dic['newcustomer']
            ffLong =  cus['frequentflieron']
            print ffLong
            ff = Airlines.select().where(Airlines.name=='%s'%ffLong).get()
            ffShort = ff.airlineid
            newCus = Customers(name=cus['name'], customerid=cus['customerid'], birthdate=cus['birthdate'], frequentflieron=ffShort)
            newCus.save(force_insert=True)

        if 'flightinfo' in dic:
            flInfo = dic['flightinfo']
            custs = flInfo['customers']
            for cust in custs:
                if 'name' in cust:
                    newCus = Customers(name=cust['name'], customerid=cust['customerid'], birthdate=cust['birthdate'], frequentflieron=cust['frequentflieron'])
                    newCus.save(force_insert=True)
                newF = Flewon(customerid=cust['customerid'], flightdate=flInfo['flightdate'],flightid=flInfo['flightid'])
                newF.save(force_insert=True)
