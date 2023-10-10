# -*- coding: utf-8 -*-
"""
Created 8/21 15:32:24 2023
@author: chanson

Purpose: Output a SQL query to an excel file
    This program will perform the following:
        read metadata file for matching job
        read job detail from file
        execute sql statement
        save results into excel file
        move excel file to final destination
        optionally send encrypted email with custom subject, body and attachment with data
        send ms teams messages on success and failures
Inputs:
    jobID - job id being executed used to find job detail in metadata
    metadata - metadata file with job information
Outputs:
    Excel file - generated output file containing data
    error log - logging for troubleshooting
    MS Teams status messages - message sent to teams on success failure
Modules Needed:
    pip/pip3 install pandas pymsteams sqlalchemy logging
    
Example:
    python sql2excel.py --job 1
    
Change Log
Date        user      description
=========================================================
20230821    chanson   Initial Creation of script
20230828    chanson   Added support to send encrypted email (email address, subject, message added to metadata)
20231002    chanson   Removed time portion of datetime filestamp
20231003    chanson   Removed JobWebHookSuccess from logic flow
20231010    chanson   Added new default paramter defaultpurgedays and logic to purge log and final folder

"""

import sys, os, json, math, traceback
import pandas as pd
import sqlalchemy as sa
import urllib
import time
import argparse
import logging
import pymsteams
import shutil
import smtplib
from os.path import basename
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

#Get Arguments


#Initialize some variables
date = datetime.now() #time.localtime() # get struct_time
APP_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
LOG_DIR = APP_DIR+'/LOGS/'
SQL_DIR = APP_DIR+'/SQL/'
OUT_DIR = APP_DIR+'/OUT/'
FINAL_DIR = APP_DIR+'/FINAL/'
METADATA = APP_DIR+"/Metadata.xlsx"

DefaultWebHookError=""
DefaultEmailOnError=""
DefaultWebHookSucces=""
DefaultFilePurgeDays=""


#set filenames
LOG_NAME = LOG_DIR+'sql2excel-'+date.strftime("%Y%m%d")+'.log'


def PurgeFiles(path, purgedays=14):
    logging.info("Purging Files in: " + path + " > " +str(purgedays) + " days old")
    now = time.time()
    for f in os.listdir(path):
        f = os.path.join(path, f)
        if os.stat(f).st_mtime < now -int(purgedays) * 86400:
            if os.path.isfile(f):
                logging.info("removing " + os.path.join(path, f))
                os.remove(os.path.join(path, f))

def checkDirs():
    global LOG_DIR, SQL_DIR, OUT_DIR
    #some housekeeping check that the paths exist
    logging.info("Checking Directories")
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    if not os.path.exists(SQL_DIR):
        os.makedirs(SQL_DIR)
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
    if not os.path.exists(FINAL_DIR):
        os.makedirs(FINAL_DIR)

def getconfig():
    logging.info("Reading Configuration")
    config = json.loads(open(os.path.join(APP_DIR, 'Metadata.json')).read())
    return config

def getargs():
    logging.info("Parsing Arguments")
    parser = argparse.ArgumentParser(description='Run SQL code and save to excel')
    parser.add_argument('--job', '-j', default=1, help='job id to execute from metadata')
    args = parser.parse_args()
    return args

# Pulls the start and end dates from e2lconfig.json to be used in the queries.
def setGlobal(config):
    global DefaultWebHookError, DefaultWebHookSuccess, DefaultEmailOnError, DefaultFilePurgeDays
    logging.info("set Global Variables")
    defaults = config['defaults']
    DefaultWebHookError = defaults[0]['DefaultWebHookError']
    DefaultWebHookSuccess = defaults[0]['DefaultWebHookSuccess']
    DefaultEmailOnError = defaults[0]['DefaultEmailOnError']
    DefaultFilePurgeDays = defaults[0]['DefaultFilePurgeDays']
    


def setup_logging():
    #Start Logging
    #Turn on logging in append
    global LOG_NAME
    logging.basicConfig(filename=LOG_NAME, filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logging.info('Logging Started')
    logging.info("Log File %s",LOG_NAME)        
    

# Purpose: Send MSTeams message to channel
def SendTeamsMessage(webhook, title, message, msgtype="NOTICE"):
    if webhook:
        myTeamsMessage = pymsteams.connectorcard(webhook)
        myTeamsMessage.title(title)
        if msgtype=="ERROR":
            myTeamsMessage.color("#FF0000") #Red
        elif msgtype=="NOTICE":
            myTeamsMessage.color("#00FF00") #Green
        else:
            myTeamsMessage.color("#00FF00") #Default to Green
        myTeamsMessage.text(message)
        myTeamsMessage.send()

# Purpose: Send Email to user with attachment
def SendEmailMessage(send_from, send_to, subject, text, files=None, server="mail.glfhc.org"):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = "[ENCRYPT] "+subject #always encrypt for safety
    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    try:
        port = 587
        smtp = smtplib.SMTP(server, port)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo
        emailuser = os.getenv("SQL2EXCEL_EMAIL_USER")
        emailpass = os.getenv("SQL2EXCEL_EMAIL_PASSWORD")
        smtp.login(emailuser, emailpass)
        smtp.ehlo
        smtp.sendmail(send_from, send_to.split(","), msg.as_string())
        smtp.close()
    except smtplib.SMTPResponseException as e:
        error_code = e.smtp_code
        error_message = e.smtp_error
        logging.error(str(error_code)+" "+str(error_message))
        

def createSQLConnection():
    #create database connection
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=BI-PRD-DB;"
                                     "DATABASE=Athena_Stg2;"
                                     "Trusted_Connection=yes")
    
    conn = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    return conn


def runexcel2sql(config, conn, args):
    global DefaultWebHookError, DefaultWebHookSuccess

    jobs = config['jobs']
    
    #Let's do this!!
    #Loop through each row for matching job
    for job in jobs:
        JobID=job['job']
        JobErrors=0
        #does job matches passed in argument
        if str(JobID)==str(args.job):
            JobActive=job['Active']
            JobInputSQL=SQL_DIR+str(job['InputSQL'])
            JobOutputDir=job['OutputDir']
            JobOutputName=job['OutputName']
            JobEmail=job['Email']
            JobEmailSubject=job['EmailSubject']
            JobEmailBody=job['EmailBody']
            
            logging.info("Job ID %s",JobID)    
            #Is Current job Active
            if int(JobActive)==1:
                if not os.path.isfile(JobInputSQL):
                    errmsg="Job %s %s - Input File not found "+JobInputSQL+" "+str(time.ctime())
                    logging.info("Error %s",errmsg)
    
                #create destination folder if it doesn't exist
                if not os.path.exists(JobOutputDir):
                    os.makedirs(JobOutputDir)
                    
                #final output filename
                #file format: JobName-JobID-RunTime.xlsx                    
                JobOutputFileName = JobOutputName.replace(" ", "_")+"-"+JobID+"-"+date.strftime("%Y%m%d")+'.xlsx'
                logging.info("Script file %s",JobInputSQL)
                logging.info("Output file %s",OUT_DIR+JobOutputFileName)
                logging.info("Final file %s",JobOutputDir+"/"+JobOutputFileName)

                logging.info("Reading SQl script %s", JobInputSQL)
                filedata = open(JobInputSQL, "r")    
                sqldata = filedata.read()
                
                logging.info("Executing sql script")
                result = pd.read_sql_query(sqldata, conn, coerce_float='False')

                #create excel file
                logging.info("Creating temp output file %s",OUT_DIR+JobOutputFileName)
                result.to_excel(OUT_DIR+JobOutputFileName, index = False)
                               
                #copy file to final location
                logging.info("Copying temp output file %s to final location %s",OUT_DIR+JobOutputFileName, JobOutputDir+"/"+JobOutputFileName)
                dest=shutil.copyfile(OUT_DIR+JobOutputFileName, JobOutputDir+"/"+JobOutputFileName)

                #clean up temp file
                logging.info("Removing temp file")
                os.remove(OUT_DIR+JobOutputFileName)

                #send notification email
                if JobEmail:
                    logging.info("Sending secure email")
                    SendEmailMessage("chanson@glfhc.org", 
                                     str(JobEmail), 
                                     str(JobEmailSubject), 
                                     str(JobEmailBody), 
                                     files=[JobOutputDir+"/"+JobOutputFileName])
                #send success messages               
                if DefaultWebHookSuccess:
                    SendTeamsMessage(DefaultWebHookSuccess,
                                     "Job "+str(JobOutputName)+" completed", 
                                     "Job ID "+str(args.job)+" - "+str(JobOutputName)+" completed successfully")
                logging.info("Job %s Completed Successfully", str(JobID))


def main():
    print("******************************")
    print('Start Execution: ' + (time.ctime()))
    print("******************************")

    try:
        # Check Directory Structure
        checkDirs()
        
        # get config values
        config = getconfig()
        args = getargs()
        
        #set Global variables
        setGlobal(config)
        
        #Start Logging
        setup_logging()
        
        #Clean Old Files
        PurgeFiles(LOG_DIR,DefaultFilePurgeDays)
        PurgeFiles(FINAL_DIR,DefaultFilePurgeDays)
        
        #Create DB Connection
        conn = createSQLConnection();
    
        # breakpoint()
        runexcel2sql(config, conn, args)
    except Exception as e:
        print("Exception in run runexcel2sql", e)
        traceback.print_exc()
        tracemsg = traceback.format_exc()
        errmsg = "Job "+str(args.job)+" failed with errors in [runexcel2sql]. Log File: "+LOG_NAME+"<br>\n\nXMLoutput: "+str(tracemsg)
        logging.error("************************************************************************************************" )
        logging.error("***** Job %s Failed",str(args.job))
        logging.error("***** xmloutput error " + "\n" + str(traceback.format_exc()))        
        
        SendEmailMessage("chanson@comcast.net", 
                         DefaultEmailOnError, 
                         "sql2excel job errors", 
                         errmsg, 
                         files=[LOG_NAME])
        SendTeamsMessage(DefaultWebHookError,
                         "sql2excel Error", 
                         errmsg, 
                         "ERROR")
    
    logging.info('Logging Stopped')    
    print("******************************")
    print('Stop Execution: ' + (time.ctime()))
    print("******************************")

if __name__ == '__main__':
    main()    