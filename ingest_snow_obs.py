# Main calling proram to ingestion of snow depth and SWE observations
# processed by the National Operational Hydrologic Remote Sensing 
# Center in Chanhassen, MN. Files from NOHRSC are stored as 
# CSV format. Data is processed into an SQL database located
# on hydro-c1-web.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# The workflow runs as follows:
# 1.) Loop back and check for Log files for each
#     hour.
# 2.) If hour wasn't processed, process hour's
#     worth of observations.
# 3.) If log file present, open and check to make sure
#     all observations from CSV file were processed. 
# 4.) Process data to SQL database using unique
#     ID keys for each station observation.

import datetime
import os
import sys
import pandas as pd
from ConfigParser import SafeConfigParser

configPath = './dbConfig.parm'
parser = SafeConfigParser()
parser.read(configPath)

# Establish constants
libDir = parser.get('paths','libDir')
tmpDir = parser.get('paths','tmpDir')
logDir = parser.get('paths','logDir')
obsDir = parser.get('paths','obsDir')
emailAddy = parser.get('paths','emailAddy')
errTitle = parser.get('paths','errTitle')
warningTitle = parser.get('paths','warningTitle')
numHoursBack = parser.get('timeMgmt','numHoursBack')
hoursPad = parser.get('timeMgmt','hoursPad')
numHoursBack = int(numHoursBack)
hoursPad = int(hoursPad)
lockFile = parser.get('paths','lockFile')

# Establish PID and lock file for this running program.
pid = os.getpid()
lockFile = tmpDir + "/" + lockFile

# Place dependent module files into the path.
sys.path.append(libDir)

# Import program specific modules
from errMod import *
from ioMod import *
from ingestMod import *

# Create lock file
createLock(lockFile,pid,warningTitle,emailAddy)

dCurrent = datetime.datetime.now()
for hoursBack in range(numHoursBack,hoursPad,-1):
	dProc = dCurrent - datetime.timedelta(seconds=3600*hoursBack)
	print dProc.strftime('%Y-%m-%d %H:%M:%S')

	# SWE Processing
	newLog = logStatus(dProc,'SWE',logDir)

	if not newLog:
		ingestNew(dProc,'SWE',errTitle,emailAddy,logDir,obsDir,lockFile,parser)
	else:
		ingestSplit(dProc,'SWE',errTitle,emailAddy,logDir,obsDir,lockFile,parser)

	# Snow Depth Processing
	#newLog = logStatus(dProc,'SD',logDir)
	#print 'SD LOG STATUS: ' + str(newLog)

	#if not newLog:
	#	ingestNew(dProc,'SD',errTitle,emailAddy,logDir,obsDir,lockFile,parser)
	#else:
	#	ingestSplit(dProc,'SD',errTitle,emailAddy,logDir,obsDir,lockFile,parser)

# Remove lock file
os.remove(lockFile)
