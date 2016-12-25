#!/usr/bin/python
'''
attach '2016-12-17-04-00-domoticz.db' as toMerge;
BEGIN;
insert into meter select * from toMerge.meter;
insert into percentage select * from toMerge.percentage;
COMMIT;
detach toMerge;
attach '2016-12-18-04-00-domoticz.db' as toMerge;
BEGIN;
insert into meter select * from toMerge.meter;
insert into percentage select * from toMerge.percentage;
COMMIT;
detach toMerge;
attach '2016-12-19-04-00-domoticz.db' as toMerge;
BEGIN;
insert into meter select * from toMerge.meter;
insert into percentage select * from toMerge.percentage;
COMMIT;
detach toMerge;





attach 'all.db' as toMerge;
BEGIN;
insert into sensors_numeric select Date as savetime, DeviceRowID as idx, '2016-01-01 03:45:01' as lastupdate, Value as value from toMerge.meter;
insert into sensors_numeric select Date as savetime, DeviceRowID as idx, '2016-01-01 03:45:01' as lastupdate, Percentage as value from toMerge.percentage;
COMMIT;
detach toMerge;

select * from sensors_numeric order by savetime asc limit 50;
'''

from datetime import datetime
import sqlite3
import sys

### convert Grafana datetime string to millisecond unix timestamp
### Grafana datetime string input: str '2015-12-22T07:15:43.230Z'
### millisecond unix timestamp output: int 1450768543230
def strToTimestamp(sDate):
	#~ print ('Input: ' + sDate)
	#~ 2015-12-22T07:15:43.230Z
	#~ 2016-12-19 03:45:01
	#~ '%Y-%m-%d %H:%M:%S'
	datetime_object = datetime.strptime(sDate, '%Y-%m-%d %H:%M:%S')

	#~ get seconds since epoch
	#~ millisecTimestamp = int(((datetime_object - datetime(1970,1,1)).total_seconds()*1000))
	millisecTimestamp = int(((datetime_object - datetime(1970,1,1)).total_seconds()))
	#~ print ('output: ' + str(millisecTimestamp))
	return millisecTimestamp

print (strToTimestamp('2016-12-19 03:45:01'))

#~ sys.exit()

con = sqlite3.connect('/home/ikke/domo/all.db')

### Wrapper for querying sqlite db
###
def db_query_devices(limit = 10):
	with con:
		cur = con.cursor()
		curUpdate = con.cursor()
		
		data = []
		cur.execute("SELECT ROWID, Date from meter")
		#~ cur.execute("SELECT ROWID, Date from percentage")
		for row in cur:
			#~ print (list(row))
			rowid = row[0]
			if not unicode(str(row[1]), 'utf-8').isnumeric():

				tstamp = strToTimestamp( row[1])
				print ("UPDATE meter SET Date = "+str(tstamp)+" where ROWID = "+str(rowid))
				#~ print ("UPDATE percentage SET Date = "+str(tstamp)+" where ROWID = "+str(rowid))
				curUpdate.execute("UPDATE meter SET Date = ? where ROWID = ? ", (tstamp, rowid))
				#~ curUpdate.execute("UPDATE percentage SET Date = ? where ROWID = ? ", (tstamp, rowid))
				con.commit()
		
		#~ data = cur.fetchall()
		
		#~ print(data)
		return data

print (type([]))
print (type({}))

data = db_query_devices()
print(data)
