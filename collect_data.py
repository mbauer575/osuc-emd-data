import os
import sys
import time
import json
from datetime import datetime
import pandas as pd
import pyodbc, struct
from azure.identity import DefaultAzureCredential


def pullData(FTP_HOST, FTP_USER, FTP_PASS, SERVER_NUM):
    import pysftp as sftp

    # gets todays file name based on date
    now = datetime.now()
    Fdate = now.strftime("%Y%m%d")
    Fname = "Trend_Virtual_Meter_Watt_" + Fdate + "_" + str(SERVER_NUM) + ".csv"

    # if file already exists, delete it
    if os.path.exists(Fname):
        os.remove(Fname)

    print(
        "[DOWNLOAD_INFO]  Attempting to download"
        + Fname
        + " at "
        + str(datetime.now() + str(time.time()))
    )
    cnopts = sftp.CnOpts()
    cnopts.hostkeys = None

    # connect to sftp server and download file
    with sftp.Connection(
        host=FTP_HOST, username=FTP_USER, password=FTP_PASS, cnopts=cnopts, port=2222
    ) as sftp:
        sftp.chdir("trend")
        sftp.get("Trend_Virtual_Meter_Watt_" + Fdate + ".csv")
        print("[DOWNLOAD_INFO]  " + "Download successful")
        os.rename(
            "Trend_Virtual_Meter_Watt_" + Fdate + ".csv",
            "Trend_Virtual_Meter_Watt_" + Fdate + "_" + str(SERVER_NUM) + ".csv",
        )
