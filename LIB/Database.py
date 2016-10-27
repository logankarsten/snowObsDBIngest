"""
Database functions
The database object provides an interface to a mysql database.
Requires python module MySQLdb
"""

import MySQLdb
from errMod import *
from ConfigParser import SafeConfigParser

class Database(object):
	def __init__(self,parser):
		self.connected = False
		self.email = parser.get('paths','emailAddy')
		self.lockFile = parser.get('paths','tmpDir') + "/" + parser.get('paths','lockFile')
		self.errTitle = parser.get('paths','errTitle')
		self.warningTitle = parser.get('paths','warningTitle')
		self.host = parser.get('sqlInfo','host')
		self.userName = parser.get('sqlInfo','userName')
		self.password = parser.get('sqlInfo','pass')
		self.dbName = parser.get('sqlInfo','dbName')
		self.sweTable = parser.get('sqlInfo','sweTable')
		self.sdTable = parser.get('sqlInfo','sdTable')
		self.db = None

	def connect(self):
		"""
		Connect to the MySQL Database Server
		"""

		if self.connected:
			errMsg = "ERROR: Connection to DB already established"
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		try:
			db = MySQLdb.connect(self.host,self.userName,self.password,self.dbName)
		except:
			errMsg = "ERROR: Unable to connect to database: " + self.dbName
			errOut(errMsg,self.errTitle,self.email,self.lockFile)
		
		self.db = db
		self.conn = db.cursor()
		self.connected = True

	def disconnect(self):
		"""
		Disconnect from MySQL database server and cleanup
		"""

		if not self.connected:
			errMsg = "ERROR: Connection to DB already disconnected"
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		if self.conn is not None: self.conn.close()
		self.conn = None
		self.connected = False

	def queryMeta(self,id,type,obsLat,obsLon):
		# Function to check the metadata table for station entry.
		# Function will return boolean True/False back to calling
		# program indicating if station was found.
		# If station is found, but lat/lon differ, send error message
		# as this indicates station has moved.

		if not self.connected:
			errMsg = "ERROR: Not connected to database."
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		sql = "select id from NWM_snow_meta where station='%s'" % (id) + \
                      " and network='%s'" % (type) + " and " + \
                      "latitude='%s'" % (obsLat) + " and " + \
                      "longitude='%s';" % (obsLon) 

		self.conn.execute(sql)
		result = self.conn.fetchone()
		if result is None:
			stnStatus = False
		else:
			stnStatus = True		
	
		return(stnStatus)

	def addStn(self,id,type,obsLat,obsLon):
		# Function to add station to the metadata table for station entry.
		# Function uses the combination of the station_type and station_id
		# to create a unique key value to that station. 		

		if not self.connected:
			errMsg = "ERROR: Database not connected."
			errOut(errMsg,self.errTitle,self.email,self.lockFile)	

		sql = "insert into NWM_snow_meta (network,station,latitude,longitude) values " + \
                      "('%s','%s','%s','%s');" % (type,id,obsLat,obsLon)

		try:
			self.conn.execute(sql)
			self.db.commit()
			print "ADDED STATION: " + id + " NETWORK: " + type
		except:
			errMsg = "ERROR: Unable to create metadata entry for ID: " + id + " NETWORK: " + type
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

	def queryUnique(self,id,type,obsLat,obsLon):
		# Function to return unique ID for given station_id and station_type.
		if not self.connected:
                        errMsg = "ERROR: Database not connected."
                        errOut(errMsg,self.errTitle,self.email,self.lockFile)

		sql = "select id from NWM_snow_meta where station='%s'" % (id) + \
                      " and network='%s'" % (type) + " and " + \
                      "latitude='%s'" % (obsLat) + " and " + \
                      "longitude='%s';" % (obsLon)
	
		self.conn.execute(sql)
		result = self.conn.fetchone()
		if result is None:
			errMsg = "ERROR: Unable to locate metadata for ID: " + id + " NETWORK: " + type
			errOut(errMsg,self.errTitle,self.email,self.lockFile)
		else:
			uniqueID = result[0]	
		
		return(uniqueID)

	def enterSWE(self,uniqueID,date,obs):
		# Function to enter in SWE observation (mm) into SWE table.
		if not self.connected:
			errMsg = "ERROR: Database not connected."
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		# First test to ensure observation has not already been instered into the table. This should
		# be accounted for in the log file, but this is a fail safe.
		sql = "select id from NWM_SWE where id='%s'" % (uniqueID) + \
                      " and date_obs='%s';" % (date)

		self.conn.execute(sql)
		result = self.conn.fetchone()
		if result is not None:
			errMsg = "ERROR: SWE Observation already entered for ID: " + str(uniqueID) + \
                                 " DATE: " + date.strftime('%Y-%m-%d %H:%M:%S')
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		# Enter data into database
		sql = "insert into NWM_SWE (id,obs_mm,date_obs) values " + \
                      "('%s','%s','%s');" % (uniqueID,obs,date)

		try:
			self.conn.execute(sql)
			self.db.commit()
		except:
			errMsg = "ERROR: Unable to enter SWE observation for UNIQUE ID: " + \
                                 str(uniqueID) + " DATE: " + date
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

	def enterDepth(self,uniqueID,date,obs):
		# Function to enter in snow depth observation (mm) into depth table.
		if not self.connected:
			errMsg = "ERROR: Database not connected."
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		# First test to ensure observation has not already been inserted into the table. This should
		# be accounted for in the log file, but this is a fail safe.
		sql = "select id from NWM_SD where id='%s'" % (uniqueID) + \
			" and date_obs='%s';" % (date)

		self.conn.execute(sql)
		result = self.conn.fetchone()
		if result is not None:
			errMsg = "ERROR: SD Observation already entered for ID: " + str(uniqueID) + \
				 " DATE: " + date.strftime('%Y-%m-%d %H:%M:%S')
			errOut(errMsg,self.errTitle,self.email,self.lockFile)

		# Enter data into database
		sql = "insert into NWM_SD (id,obs_mm,date_obs) values " + \
		      "('%s','%s','%s');" % (uniqueID,obs,date)

		try:
			self.conn.execute(sql)
			self.db.commit()
		except:
			errMsg = "ERROR: Unable to enter SD observation for UNIQUE ID: " + \
			         str(uniqueID) + " DATE: " + date
			errOut(errMsg,self.errTitle,self.email,self.lockFile)
