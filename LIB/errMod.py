# Python module for error handling from main program.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import sys
import smtplib
from ftplib import FTP
from email.mime.text import MIMEText

def errOut(msgContent,emailTitle,emailRec,lockFile):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
   # Remove lock file
   os.remove(lockFile)
   sys.exit(1)

def warningOut(msgContent,emailTitle,emailRec):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
   sys.exit(1)

def createLock(lockFile,pid,warningTitle,emailAddy):
   if os.path.isfile(lockFile):
      fileLock = open(lockFile,'r')
      pid = fileLock.readline()
      warningMsg =  "WARNING: Another Snow DB Ingest Program Running. PID: " + pid
      warningOut(warningMsg,warningTitle,emailAddy)
   else:
      fileLock = open(lockFile,'w')
      fileLock.write(str(pid))
      fileLock.close()
