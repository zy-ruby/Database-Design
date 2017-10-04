from disk_relations import *
from transactions import *
import time
import random

#####################################################################################################
####
#### A Few Pre-Defined Transactions
####
#####################################################################################################

# pjk
def TransactionRCwriter(relation, id, sleeptime=.5):
	for i in range(0, 4):
		tstate = TransactionState()
		tstate.setMode(TransactionState.READ_COMMITTED)

		if not tstate.getXLockTuple(relation, id):
			tstate.abortTransaction()
			return
		for j in range(0, 3):
			AddTen(tstate, relation, id)
			print "Wrote:  {}".format(tstate.getAttribute(relation, id, "A"))
			time.sleep(sleeptime)

		tstate.commitTransaction()
		print "Comitted write now {}".format(tstate.getAttribute(relation, id, "A"))

	tstate = TransactionState()
	tstate.setMode(TransactionState.READ_COMMITTED)

	if not tstate.getXLockTuple(relation, id):
		tstate.abortTransaction()
		return
	AddTen(tstate, relation, id)
	print "Wrote:  {}".format(tstate.getAttribute(relation, id, "A"))

	while True:
		time.sleep(5.0)

	
# pjk
def TransactionRCreader(relation, id, sleeptime = 0.3):
	attr = "A"

	tstate = TransactionState()
	tstate.setMode(TransactionState.READ_COMMITTED)
	while True:
		v = tstate.getAttribute(relation, id, attr)
		print "Reader: {}".format(v)
		time.sleep(sleeptime)

# pjk
def TransactionRRwriter(relation, id, sleeptime=.18):
	for i in range(0, 8):
		tstate = TransactionState()
		tstate.setMode(TransactionState.REPEATABLE_READ)

		if not tstate.getXLockTuple(relation, id):
			tstate.abortTransaction()
			return
		AddTen(tstate, relation, id)
		tstate.commitTransaction()
		print "Comitted write now {}".format(tstate.getAttribute(relation, id, "A"))
		time.sleep(sleeptime)

	tstate = TransactionState()
	tstate.setMode(TransactionState.REPEATABLE_READ)

	if not tstate.getXLockTuple(relation, id):
		tstate.abortTransaction()
		return
	AddTen(tstate, relation, id)
	print "Uncomitted write now {}".format(tstate.getAttribute(relation, id, "A"))

	while True:
		time.sleep(5.0)

	
# pjk
def TransactionRRreader(relation, id, sleeptime = 1):
	attr = "A"

	tstate = TransactionState()
	tstate.setMode(TransactionState.REPEATABLE_READ)
	while True:
		v = tstate.getAttribute(relation, id, attr)
		print "Reader1: {}".format(v)
		time.sleep(sleeptime)

def TransactionRRreader2(relation, id, sleeptime=1):
	attr = "A"

	time.sleep(sleeptime)
	tstate = TransactionState()
	tstate.setMode(TransactionState.REPEATABLE_READ)
	while True:
		v = tstate.getAttribute(relation, id, attr)
		print "Reader2: {}".format(v)
		time.sleep(sleeptime)


#=====================================================================

def SubtractTen(tstate, relation, primary_id):
	Subtract(tstate, relation, primary_id, 10)

def Subtract(tstate, relation, primary_id, val):
	oldval = tstate.getAttribute(relation, primary_id, "A")
	newval = str(int(oldval) - val)
	LogManager.createUpdateLogRecord(tstate.transaction_id, relation.fileName, primary_id, "A", oldval, newval)
	tstate.setAttribute(relation, primary_id, "A", newval)

def AddTen(tstate, relation, primary_id):
	oldval = tstate.getAttribute(relation, primary_id, "A")
	newval = str(int(oldval) + 10)
	LogManager.createUpdateLogRecord(tstate.transaction_id, relation.fileName, primary_id, "A", oldval, newval)
	tstate.setAttribute(relation, primary_id, "A", newval)

def MultiplyByTwo(tstate, relation, primary_id):
	oldval = tstate.getAttribute(relation, primary_id, "A")
	newval = str(int(oldval) * 2)
	LogManager.createUpdateLogRecord(tstate.transaction_id, relation.fileName, primary_id, "A", oldval, newval)
	tstate.setAttribute(relation, primary_id, "A", newval)

# Transaction 1 adds 10 to the value of A given a primary id
def TransactionReadRelation(relation, sleeptime = 10):
	tstate = TransactionState()
	if tstate.getSLockRelation(relation):
		time.sleep(sleeptime)
		tstate.commitTransaction()

# Transaction 1 adds 10 to the value of A given a primary id
def Transaction1(relation, primary_id, sleeptime = 10):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id):
		time.sleep(sleeptime)
		AddTen(tstate, relation, primary_id)
		time.sleep(sleeptime)
		tstate.commitTransaction()

# Transaction 2 doubles the value of A for the given tuple ID
def Transaction2(relation, primary_id, sleeptime = 10):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id):
		time.sleep(sleeptime)
		MultiplyByTwo(tstate, relation, primary_id)
		time.sleep(sleeptime)
		tstate.commitTransaction()

# Transaction 3 moves 10 from one tuple to another
def Transaction3(relation, primary_id1, primary_id2, sleeptime = 10, abort = False):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id1): 
		time.sleep(sleeptime)
		if tstate.getXLockTuple(relation, primary_id2):
			SubtractTen(tstate, relation, primary_id1)
			AddTen(tstate, relation, primary_id2)

			time.sleep(sleeptime)
			if not abort:
				tstate.commitTransaction()
			else:
				tstate.abortTransaction()

# Transaction 4 moves 20% from one tuple to another
def Transaction4(relation, primary_id1, primary_id2, sleeptime = 10, abort = False):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id1): 
		time.sleep(sleeptime)

		if tstate.getXLockTuple(relation, primary_id2):
			oldval1 = tstate.getAttribute(relation, primary_id1, "A")
			oldval2 = tstate.getAttribute(relation, primary_id2, "A")
			movevalue = int(oldval1)/5 + 1
			newval1 = str(int(oldval1) - movevalue)
			newval2 = str(int(oldval2) + movevalue)
			LogManager.createUpdateLogRecord(tstate.transaction_id, relation.fileName, primary_id1, "A", oldval1, newval1)
			LogManager.createUpdateLogRecord(tstate.transaction_id, relation.fileName, primary_id2, "A", oldval2, newval2)
			tstate.setAttribute(relation, primary_id1, "A", newval1)
			tstate.setAttribute(relation, primary_id2, "A", newval2)

			time.sleep(sleeptime)

			if not abort:
				tstate.commitTransaction()
			else:
				tstate.abortTransaction()

# Transaction 5 is similar to 3 with some differences in when things are written out
def Transaction5(relation, primary_id1, primary_id2, sleeptime = 10):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id1): 
		SubtractTen(tstate, relation, primary_id1)
		time.sleep(sleeptime)
		if tstate.getXLockTuple(relation, primary_id2):
			SubtractTen(tstate, relation, primary_id2)
			tstate.commitTransaction()

# Transaction 6 moves 10 from one tuple to another
def Transaction6(relation, primary_id1, primary_id2, primary_id3, sleeptime = 10):
	tstate = TransactionState()
	if tstate.getXLockTuple(relation, primary_id1): 
		SubtractTen(tstate, relation, primary_id1)
		if tstate.getXLockTuple(relation, primary_id2): 
			SubtractTen(tstate, relation, primary_id2)
			time.sleep(sleeptime)
			if tstate.getXLockTuple(relation, primary_id3):
				SubtractTen(tstate, relation, primary_id3)
				tstate.commitTransaction()

def ReadWriteTransaction(relation, id_lock_list, sleeptime = 30):
	tstate = TransactionState()
	for i in range(0, len(id_lock_list)-1):
		(oid, locktype) = id_lock_list[i]
		if locktype == 'S':
			tstate.getSLockTuple(relation, oid) 
		else:
			tstate.getXLockTuple(relation, oid) 
	time.sleep(sleeptime)
	(oid, locktype) = id_lock_list[-1]
	if locktype == 'S':
		tstate.getSLockTuple(relation, oid) 
	else:
		tstate.getXLockTuple(relation, oid) 

def ReadWriteAbortTransaction(relation, ids, abortprob):
	random.seed()
	tstate = TransactionState()
	for oid in ids:
		tstate.getXLockTuple(relation, oid) 
		Subtract(tstate, relation, oid, random.randint(0, 10))
		time.sleep(random.randint(0, 10))

		r = random.random()
		if r < abortprob:
			tstate.abortTransaction()
			return
		elif r > (1 - abortprob):
			tstate.commitTransaction()
			return
	tstate.commitTransaction()
