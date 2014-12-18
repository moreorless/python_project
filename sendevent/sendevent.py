#!/usr/bin/python

import MySQLdb,sys, time
from socket import *

host = sys.argv[1]
textport = sys.argv[2]

# 
totalcount = 0

# open connection
db = MySQLdb.connect('192.168.19.79', 'username', 'password', 'tsoc')

udpSock = socket(AF_INET, SOCK_DGRAM)

try:
    port = int(textport)
except ValueError:
    port = socket.getservbyname(textport, 'udp')
ADDR = (host, port)


cursor = db.cursor()


# fetch 1000 rows every query
sql = 'select lid, ceventmsg from event20130730_0 limit 1000'

starttime = time.time()

try:
    while True:
	    count = cursor.execute(sql)
	    totalcount += count
	    print 'sql %s, fetch %s rows, total count = %s' % (sql, count, totalcount)

	    if count == 0:
	        break
	    # if totalcount > 100000:
	    #     break

	    results = cursor.fetchall()
	    for r in results:
	        lid = r[0]
	        msg = r[1]
	        udpSock.sendto(msg, ADDR)	        

	    sql = 'select lid, ceventmsg from event20130730_0 where lid > ' + str(lid) + ' limit 1000'
except Exception, e:
    print "Error: ", e

endtime = time.time()


print "start time : ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime))
print "end time : ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endtime))
timeused = (endtime - starttime)
print "time used : " + str(timeused) + "s, eps = " + str(totalcount / timeused)

db.close()
udpSock.close()

