# One-off utility program for ingesting gamma snow observations into the 
# database from a standalone CSV file.

# This program assumes the data is in the following format from OWP:
#:------------------------------------------------------------------------------
#: Total No. of flight lines sent = 18
#:------------------------------------------------------------------------------
#:Line   Survey    %SC   SWE   SWE %SM Est Fall  %SM  Pilot
#:No.    Date           (in) (35%) (M) Typ Date  (F)  Remarks
#:==============================================================================
#ND202 , DY170116 , 100 , 3.8 , 3.7, 34, AM, 161025 , 34                           

# The first column is the identifier that will be used to lookup from the DB.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import MySQLdb
import pandas as pd
import argparse
import sys
import os
import datetime
from ConfigParser import SafeConfigParser

def main(argv):
	parser = argparse.ArgumentParser(description="Utility " + \
                 "program to ingest snow observations manually.")
	parser.add_argument('configFile',metavar='config',type=str,
                            help='Config file for access to the DB.')
	parser.add_argument('obsFile',metavar='obsFile',type=str,
                            help='Input observations file to ingest.')

	args = parser.parse_args()

	if not os.path.isfile(args.configFile):
		print "ERROR: Configuration file: " + args.configFile + \
                      " not found."
		sys.exit(1)
	
	if not os.path.isfile(args.obsFile):
		print "ERROR: Input observations file: " + args.obsFile + \
                      " not found."
		sys.exit(1)

	parser = SafeConfigParser()
	parser.read(args.configFile)

	# Establish path to custom libraries
	libDir = parser.get('paths','libDir')
	sys.path.append(libDir)

	colNames = ['ID', 'DATE', 'SC', 'SWE', 'SWE_35', 'SM', 'TYP', 'FALL_DATE', 'SM_F', 'REMARKS']
	obsDf = pd.read_csv(args.obsFile,sep=',',header=None,names=colNames)

	host = parser.get('sqlInfo','host')
	user = parser.get('sqlInfo','userName')
	dbName = parser.get('sqlInfo','dbName')
	sweTbl = parser.get('sqlInfo','sweTable')
	pwd = parser.get('sqlInfo','pass')

	db = MySQLdb.connect(host,user,pwd,dbName)
	conn = db.cursor()
	
	numObs = len(obsDf.ID)

	# Loop through and enter observations into the DB.
	for obNum in range(0,numObs):
		sweMM = obsDf.SWE[obNum] * 25.4
		station = obsDf.ID[obNum]
		dTmp = obsDf.DATE[obNum]
		dTmp = dTmp.strip()
		if dTmp[0] != 'D':
			print "ERROR: Unexpected date format in input file."
		date = datetime.datetime.strptime(dTmp[2:8],'%y%m%d')
		dStr = date.strftime('%Y-%m-%d') + ' 12:00:00'

		# Query database for station if it exists.
		sql = "select id from NWM_snow_meta where station='%s'" % (station) + \
                      " and network='GAMMA'"
		conn.execute(sql)
		result = conn.fetchone()
		if result is None:
			print "NEED INFO ON: " + station
			latNew = raw_input('Enter Latitude:')
			lonNew = raw_input('Enter Longitude:')
			if len(latNew) == 0 and len(lonNew) == 0:
				print "SKIPPING: " + station
				continue
			else:
				# Enter new station into DB
				sql = "insert into NWM_snow_meta (network,station,latitude,longitude) values " + \
                                      "('%s','%s','%s','%s');" % ('GAMMA',station,float(latNew),float(lonNew))
				conn.execute(sql)
				db.commit()
				# Get new ID created
				sql = "select id from NWM_snow_meta where station='%s'" % (station) + \
                                       " and network='GAMMA'"
				conn.execute(sql)
				result = conn.fetchone()
				uniqueID = result[0]
		else:
			uniqueID = result[0]

		# Enter data into DB
		sql = "insert into NWM_SWE (id,obs_mm,date_obs)	values " + \
                      "('%s','%s','%s');" % (uniqueID,sweMM,dStr)

		conn.execute(sql)
		db.commit()
		
	conn.close()	
if __name__ == "__main__":
	main(sys.argv[1:])
