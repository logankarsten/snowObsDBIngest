# General Python module file for IO handling.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
from errMod import *
import pandas as pd
import numpy as np

def logStatus(dCheck,prod,logDir):
	# Check to see if log file has been generated for
	# particular datetime/product combination.
	logStatus = False

	if prod == "SWE":
		logFile = logDir + '/LOG_SWE_' + dCheck.strftime('%Y%m%d%H') + '.LOG'
	else:
		logFile = logDir + "/LOG_SD_" + dCheck.strftime('%Y%m%d%H') + ".LOG"

	if os.path.isfile(logFile):
		logStatus = True

	return logStatus

def checkFile(fileCheck,errTitle,emailAddy,lockFile):
	# Generic routine to check for file existence
	if not os.path.isfile(fileCheck):
		errMsg = "ERROR: Expected to find: " + fileCheck
		errOut(errMsg,errTitle,emailAddy,lockFile)

def ingestObsTxt(obsFile,prod,errTitle,emailAddy,lockFile):
	# Generic routine to read in observations from CSV text
	# file using Pandas.
	try:
		dataObj = pd.read_csv(obsFile,sep='|')
	except: 
		errMsg = "ERROR: Unable to open: " + obsFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	# Check to make sure the data structure contains some data,
	# and correct names.
	if list(dataObj.columns.values) != ['station_id', 'date', 'value (meters)', 'qc', 'actual', 'latitude', 'longitude', 'station_type']:
		errMsg = "ERROR: Unexpected data format for: " + obsFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	# Check to make sure data is present.
	if len(dataObj.station_id) == 0:
		errMsg = "ERROR: Zero length data found in: " + obsFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	return dataObj

def ingestLogTxt(logFile,prod,errTitle,emailAddy,lockFile):
	# Generic routine to read in log file CSV containing
	# information on what has been processed for given datetime.
	try:
		dataObj = pd.read_csv(logFile,sep='|')
	except:
		errMsg = "ERROR: Unable to open: " + logFile

	# Check to make sure data structure contains data and correct names.
	if list(dataObj.columns.values) != ['station_id','station_type','date','complete']:
		errMsg = "ERROR: Unexpected data format for: " + logFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	# Check to make sure data is present.
	if len(dataObj.station_id) == 0:
		errMsg = "ERROR: Zero length data found in: " + logFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	return dataObj

def newLogData(logFile,obsPD,errTitle,emailAddy,lockFile):
	# Leverage existing observations dataset to create a fresh
	# log dataset. All entries are incomplete by default. 
	# As observations get succesfully entered into the database,
	# update the log dataset and over-write log file.
	try:
		dataTmp1 = obsPD[['station_id','station_type','date']]
		dbLen = len(obsPD['station_id'])
		flagsOut = np.empty([dbLen],np.bool)
		flagsOut[:] = False
		flagsDB = pd.DataFrame({'complete':flagsOut})
		logDB = pd.concat([dataTmp1,flagsDB],axis=1)
	except:
		errMsg = "ERROR: Unable to create log data frame for log file: " + logFile
		errOut(errMsg,errTitle,emailAddy,lockFile)

	# Write log database to CSV log file.
	try:
		logDB.to_csv(logFile,sep='|',index=False)
	except:
		errMsg = "ERROR: Unable to create log file: " + logFile
		errOut

	return logDB

def writeLogData(logFile,logPD,errTitle,emailAddy,lockFile):
	# Function to write log dataframe object to CSV file. Mostly
	# used as the log file is being updated during the ingest 
	# process.
	try:
		logPD.to_csv(logFile,sep='|',index=False)
	except:
		errMsg = "ERROR: Unable to write to log file: " + logFile
		errOut(errMsg,errTitle,emailAddy,lockFile)
