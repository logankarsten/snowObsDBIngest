# Module file to handle workflow for the snow observations ingest.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

from ioMod import *
from snowSqlMod import *

def ingestNew(dProc,prod,errTitle,emailAddy,logDir,obsDir,lockFile,parser):
	# Establish input files necessary.
	if prod == "SWE":
		obsFile = obsDir + "/swe_" + dProc.strftime('%Y%m%d%H') + ".txt"
		logFile = logDir + "/LOG_SWE_" + dProc.strftime('%Y%m%d%H') + ".LOG"
	else:
		obsFile = obsDir + "/snowdepth_" + dProc.strftime('%Y%m%d%H') + ".txt"
		logFile = logDir + "/LOG_SD_" + dProc.strftime('%Y%m%d%H') + ".LOG"

	checkFile(obsFile,errTitle,emailAddy,lockFile)
	obsPD = ingestObsTxt(obsFile,prod,errTitle,emailAddy,lockFile)
	logPD = newLogData(logFile,obsPD,errTitle,emailAddy,lockFile)

	# Loop through observations and enter in accordingly.
	enterObs(prod,obsPD,logPD,logFile,errTitle,emailAddy,lockFile,parser)

def ingestSplit(dProc,prod,errTitle,emailAddy,logDir,obsDir,lockFile,parser):
	# Establish input files necessary.
	if prod == "SWE":
		obsFile = obsDir + "/swe_" + dProc.strftime('%Y%m%d%H') + ".txt"
		logFile = logDir + "/LOG_SWE_" + dProc.strftime('%Y%m%d%H') + ".LOG"
	else:
		obsFile = obsDir + "/snowdepth_" + dProc.strftime('%Y%m%d%H') + ".txt"
		logFile = logDir + "/LOG_SD_" + dProc.strftime('%Y%m%d%H') + ".LOG"

	checkFile(obsFile,errTitle,emailAddy,lockFile)
	obsPD = ingestObsTxt(obsFile,prod,errTitle,emailAddy,lockFile)
	logPD = ingestLogTxt(logFile,prod,errTitle,emailAddy,lockFile)

	# Loop through observations and enter in accordingly.
	enterObs(prod,obsPD,logPD,logFile,errTitle,emailAddy,lockFile,parser)
