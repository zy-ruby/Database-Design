import json
import re
from pyspark import SparkContext
from collections import Counter
from itertools import islice, izip

class DisplayRDD:
	def __init__(self, rdd):
		self.rdd = rdd

	def _repr_html_(self):                                  
		x = self.rdd.mapPartitionsWithIndex(lambda i, x: [(i, [y for y in x])])
		l = x.collect()
		s = "<table><tr>{}</tr><tr><td>".format("".join(["<th>Partition {}".format(str(j)) for (j, r) in l]))
		s += '</td><td valign="bottom">'.join(["<ul><li>{}</ul>".format("<li>".join([str(rr) for rr in r])) for (j, r) in l])
		s += "</td></table>"
		return s

	def repr(self):                                  
		x = self.rdd.mapPartitionsWithIndex(lambda i, x: [(i, [y for y in x])])
		l = x.collect()
		s = "<table><tr>{}</tr><tr><td>".format("".join(["<th>Partition {}".format(str(j)) for (j, r) in l]))
		s += '</td><td valign="bottom">'.join(["<ul><li>{}</ul>".format("<li>".join([str(rr) for rr in r])) for (j, r) in l])
		s += "</td></table>"
		return s

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
    rdd1 = playRDD.map(lambda x: x.split()).map(lambda x:(x[0],(' '.join(x),len(x))))
    rdd2 = rdd1.filter(lambda x: x[1][1]>10)
    return rdd2

def task2_flatmap(x):
    people = x["laureates"]
    surname = []
    for p in people:
        surname.append(p["surname"])
    return surname

def task3helper(line):
    surnames = []
    for p in line["laureates"]:
        surnames.append(p["surname"])
    return (line["category"],surnames)

def task3(nobelRDD):
    pairRDD1 = nobelRDD.map(json.loads).map(task3helper).reduceByKey(lambda v1,v2:v1+v2)
    return pairRDD1


def task4helper(x,l):
    for date in l:
        if x.find(date)!=-1:
            return True
    return False

def task4(logsRDD, l):
    rdd1 = logsRDD.map(lambda x: x.split(":")[0]).filter(lambda x:task4helper(x,l)).distinct()
    rdd = rdd1.map(lambda x:(x.split(" - -")[0],1)).reduceByKey(lambda v1,v2:v1+v2).filter(lambda x:x[1]==2).map(lambda x:x[0])
    return rdd
    
def task5(bipartiteGraphRDD):
    rdd = bipartiteGraphRDD.map(lambda (x,y):(x,1)).reduceByKey(lambda v1,v2:v1+v2).map(lambda (x,y):(y,1)).reduceByKey(lambda v1,v2:v1+v2)
    return rdd

def task6helper(x,day):
    if x.find(day)!=-1:
        return 1
    return 0

def task6helper2 (x):
    list = x.split(' - -')
    host = list[0]
    if len(list)==2:
        others = list[1].split('GET ')
        if len(others) == 2:
            get = others[1].split(' HTTP')[0]
            return (host, get)
        return (host,None)
    return (host,None)
        
    
def task6(logsRDD, day1, day2):
    rdd1 = logsRDD.filter(lambda x: task6helper(x,day1)).map(task6helper2)
    rdd2 = logsRDD.filter(lambda x: task6helper(x,day2)).map(task6helper2)
    return rdd1.cogroup(rdd2).map(lambda (m,n):(m,tuple(map(list,n)))).filter(lambda (x,y):((len(y[0])!=0)and(len(y[1])!=0)))


#words = re.findall("\w+",  "the quick person did not realize his speed and the quick person bumped")
#print tuple(izip(words, islice(words, 1, None)))
#
def task7(nobelRDD):
    rdd = nobelRDD.map(json.loads).map(lambda x:x["laureates"]).flatMap(lambda x:[y["motivation"] for y in x])
    rdd1 = rdd.flatMap(lambda x:list(set(list(izip(x.split(),islice(x.split(),1,None)))))).map(lambda x:(x,1)).reduceByKey(lambda v1,v2:v1+v2)
    return rdd1


    

def task8(bipartiteGraphRDD, currentMatching):
    reverseCurr = currentMatching.map(lambda (x,y):(y,x))
    emptyRDD = bipartiteGraphRDD.subtractByKey(currentMatching).map(lambda (x,y):(y,x)).subtractByKey(reverseCurr).map(lambda (x,y):(y,x))
    rdd = emptyRDD.reduceByKey(lambda v1,v2:min(v1,v2)).map(lambda (x,y):(y,x)).reduceByKey(lambda v1,v2:min(v1,v2)).map(lambda (x,y):(y,x))
    
    return rdd
