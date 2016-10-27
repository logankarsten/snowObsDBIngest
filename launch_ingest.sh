#!/bin/sh

# Launching script to ingest hourly snow water equivalent
# and snow depth observations into an SQL database
# using a python workflow. # Program will be launched
# four times a day to ingest latest observations.
# The observations will be downloaded independently via
# the NOHRSC FTP.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# Source bash environment
. $HOME/.bashrc

cd /d2/karsten/SNOW_DB
/usr/local/python/bin/python ingest_snow_obs.py

exit 0
