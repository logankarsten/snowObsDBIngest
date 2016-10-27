# Module file for making calls to Database object,
# which connects to the database, makes calls
# to enter data and check for new stations.

# Logan Karsten
# National Center for Atmospheric Research
# Reseach Applications Laboratory

from Database import Database
from errMod import *
from ioMod import *
import datetime

def enterObs(prod,obsPD,logPD,logFile,errTitle,emailAddy,lockFile,parser):
	# Initialize DB object
	db = Database(parser)

	# Loop through the log and obs dataframe objects that
	# were previously initialized to. First check status
	# of each observation. If it's been entered into the
	# database, continue on. If not, first check to see
	# to see if observation station is in the metadata table.
	# If not, enter into the metadata table. Next, extract
	# unique ID for station, and enter observation. From there,
	# update the status of the observation to having been entered
	# into the database. For each iteration, re-write the log dataframe
	# to the CSV log file. This is in case an error occurs halfway through
	# the iterations.

	# Connect to the database
	db.connect()

	if len(obsPD.station_id) != len(logPD.station_id):
		errMsg = "ERROR: Length of log data frame and observation data frame mismatch."
		errOut(errMsg,errTitle,emailAddy,lockFile)

	# Begin looping through observations. Skipping last row of file as it's an invalid entry.
	for obsNum in range(0,len(obsPD.station_id)-1):
		qcFlag = 1
		if not logPD.complete[obsNum]:
			id = obsPD.station_id[obsNum]
			type = obsPD.station_type[obsNum]
			date = datetime.datetime.strptime(obsPD.date[obsNum],'%Y-%m-%d %H:%M:%S')
			obsValue = obsPD['value (meters)'][obsNum]
			obsValueMM = obsValue * 1000.0
			obsLat = obsPD.latitude[obsNum]
			obsLon = obsPD.longitude[obsNum]
			obsQC = obsPD.qc[obsNum]
			obsActual = obsPD.actual[obsNum]
		
			# Check meta data table for station.
			stnStatus = db.queryMeta(id,type,obsLat,obsLon)

			if not stnStatus:
				# Enter new station into metadata table
				db.addStn(id,type,obsLat,obsLon)

			# Extract unique ID for station
			uniqueID = db.queryUnique(id,type,obsLat,obsLon)

			# Perform some rough QC.
			if obsValue < 0.0:
				qcFlag = 0
			if obsValue > 25.0:
				qcFlag = 0

			if qcFlag == 1:
				if prod == "SWE":
					# Enter observation into SWE table
					db.enterSWE(uniqueID,date,obsValueMM)
				else:
					# Enter observation into depth table
					db.enterDepth(uniqueID,date,obsValueMM)

			logPD.complete[obsNum] = True
		
		writeLogData(logFile,logPD,errTitle,emailAddy,lockFile)

	db.disconnect()
