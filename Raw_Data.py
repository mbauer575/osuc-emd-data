import os
import time
import json
import boto3
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
#from keys import get_server_key, set_server_key

#pullData function takes in the IP address, username, password, 
# and server number and saves todays file to the current directory
def pullData(FTP_HOST,FTP_USER,FTP_PASS,SERVER_NUM):
    import pysftp as sftp
    #gets todays file name based on date
    now = datetime.now()
    Fdate= now.strftime("%Y%m%d")
    Fname = "Trend_Virtual_Meter_Watt_"+Fdate+"_"+str(SERVER_NUM)+".csv"
    
    #if file already exists, delete it
    if os.path.exists(Fname):
        remove_csv(Fname)

    
    print("[DOWNLOAD_INFO]  Attempting to download"+Fname+" at ")
    print("[DOWNLOAD_INFO]  "+str(datetime.now()))
    print("[DOWNLOAD_INFO]  "+str(time.time()))

    cnopts = sftp.CnOpts()
    cnopts.hostkeys = None

    #connect to sftp server and download file
    with sftp.Connection(host=FTP_HOST, username=FTP_USER, password=FTP_PASS, cnopts=cnopts, port=2222) as sftp:
        sftp.chdir('trend')
        sftp.get("Trend_Virtual_Meter_Watt_"+Fdate+".csv")
        print("[DOWNLOAD_INFO]  "+"Download successful")
        os.rename("Trend_Virtual_Meter_Watt_"+Fdate+".csv","Trend_Virtual_Meter_Watt_"+Fdate+"_"+str(SERVER_NUM)+".csv")

def daily_data_trim(ID):
    Fname = file_name(ID)

    df = pd.read_csv(Fname, header = 0)
    remove_csv(Fname)

    # Find number of columns with data
    search_row = df.iloc[1,:].dropna()
    num_col = len(search_row)
    num_meters = int((num_col-2) / 3)

    meters = ['Date', 'Time']

    # Make list to rename column titles
    for i in range(num_meters):
        meters.append("Server" + str(ID) + "_" + "meter" + str(i+1) + "_avg")
        meters.append("Server" + str(ID) + "_" + "meter" + str(i+1) + "_min")
        meters.append("Server" + str(ID) + "_" + "meter" + str(i+1) + "_max")

    # Preparing and saving csv
    df = df.iloc[:,:num_col]
    df.columns = meters
    df.to_csv(Fname, index = False)
    
def fill_master(ID_LIST, Last_Date):
    df_5min_master = pd.DataFrame()

    dfs = [pd.read_csv(os.getcwd() + "/" + file_name(id), index_col=False) for id in ID_LIST]
    df_5min_master = pd.concat(dfs, axis = 1)
    df_5min_master = df_5min_master.loc[:,~df_5min_master.columns.duplicated(keep='first')].copy()
    #Last_Date means only the last 5 minute point is appeaned to the master
    if Last_Date == True:
        df_5min_master = df_5min_master.iloc[-3,:]
        # print(df_5min_master)
        df_5min_master = df_5min_master.to_frame().T
        # print(df_5min_master)

            
    # absolute value the data in columns titled "Server1_meter10_avg" and "Server1_meter10_min" and "Server1_meter10_max"
    df_5min_master['Server1_meter10_avg'] = df_5min_master['Server1_meter10_avg'].abs()
    df_5min_master['Server1_meter10_min'] = df_5min_master['Server1_meter10_min'].abs()
    df_5min_master['Server1_meter10_max'] = df_5min_master['Server1_meter10_max'].abs()

    #assigns servers to the correct floor. See servermap.xlsx for more details
    df_5min_master['1st_Floor'] = df_5min_master['Server1_meter1_avg'] + df_5min_master['Server3_meter1_avg']
    df_5min_master['2nd_Floor'] = df_5min_master['Server1_meter3_avg'] + df_5min_master['Server1_meter5_avg'] + df_5min_master['Server1_meter10_avg'] + df_5min_master['Server3_meter2_avg']
    df_5min_master['3rd_Floor'] = df_5min_master['Server1_meter4_avg'] + df_5min_master['Server1_meter7_avg'] + df_5min_master['Server1_meter9_avg'] + df_5min_master['Server3_meter4_avg']
    df_5min_master['4th_Floor'] = df_5min_master['Server1_meter6_avg'] + df_5min_master['Server1_meter8_avg'] + df_5min_master['Server1_meter13_avg'] + df_5min_master['Server3_meter3_avg']
    df_5min_master['Utilities'] = df_5min_master['Server1_meter11_avg'] + df_5min_master['Server1_meter12_avg'] + df_5min_master['Server2_meter2_avg'] + df_5min_master['Server2_meter3_avg'] + df_5min_master['Server2_meter5_avg'] + df_5min_master['Server3_meter5_avg'] + df_5min_master['Server3_meter6_avg']
    df_5min_master['TOTAL'] = df_5min_master['1st_Floor'] + df_5min_master['2nd_Floor'] + df_5min_master['3rd_Floor'] + df_5min_master['4th_Floor'] + df_5min_master['Utilities']
    
    
    # calculate the Kwh for each floor and total
    df_5min_master['1st_Floor_Kwh'] = df_5min_master['1st_Floor'] * 5 / 60 / 1000
    df_5min_master['2nd_Floor_Kwh'] = df_5min_master['2nd_Floor'] * 5 / 60 / 1000
    df_5min_master['3rd_Floor_Kwh'] = df_5min_master['3rd_Floor'] * 5 / 60 / 1000
    df_5min_master['4th_Floor_Kwh'] = df_5min_master['4th_Floor'] * 5 / 60 / 1000
    df_5min_master['Utilities_Kwh'] = df_5min_master['Utilities'] * 5 / 60 / 1000
    df_5min_master['TOTAL_Kwh'] = df_5min_master['TOTAL'] * 5 / 60 / 1000

    # May not be needed
    # number of rooms on each floor:
    # first_rooms = 22
    # second_rooms = 49
    # third_rooms = 49
    # fourth_rooms = 35

    # # calculate the Kwh per room for each floor
    # df_5min_master['1st_Floor_Kwh_per_room'] = df_5min_master['1st_Floor_Kwh'] / first_rooms
    # df_5min_master['2nd_Floor_Kwh_per_room'] = df_5min_master['2nd_Floor_Kwh'] / second_rooms
    # df_5min_master['3rd_Floor_Kwh_per_room'] = df_5min_master['3rd_Floor_Kwh'] / third_rooms
    # df_5min_master['4th_Floor_Kwh_per_room'] = df_5min_master['4th_Floor_Kwh'] / fourth_rooms

    # Check to see if there are nan values in 5-minute dataframe and if Last_Date is True (meaning only the last 5 minute point is being appended to the master)
    if df_5min_master.isnull().values.any() and Last_Date == True:
        # Wait 20 seconds and try again to collect data
        print("[CAUTION/WARN]  "+"Missing Data...Waiting 20 seconds to try again...")
        # print(df_5min_master)
        #send df_5min_master to csv file named testing
        # df_5min_master.to_csv("testing.csv", index = False)
        time.sleep(20)
        download_data()
        fill_master(ID_LIST, Last_Date)
    return df_5min_master

# Function to merge 5 minute dataframe with master
def merge_master(df_5min_master):
    master = pd.read_csv(os.getcwd() + '\master.csv')
    # Merge 5 minute dataframe with master
    # If time in df_5min_master is not in master, add it to master
    print("[MERGING_INFO]  latest data point:"+df_5min_master['Time'].iloc[0])
    # if df_5min_master['Time'].iloc[0] is in master, do not add it to master
    if df_5min_master['Time'].iloc[0] in master['Time'].values:
        print("[MERGING_INFO] " + str(df_5min_master['Time'].iloc[0]) + " is already in master")
    elif master['Time'].isnull().any():
        print("[MERGING_INFO/CATUTION] master is empty, adding " + df_5min_master['Time'].iloc[0] + " to master")
        master = pd.concat([master, df_5min_master],ignore_index = False)
    else:
        master = pd.concat([master, df_5min_master],ignore_index = False)
    
    # For testing to check latest data point. Can be helpful to see if data is being collected without 
    # going thrigh the entire master dataframe
    # 
    # df_5min_master.to_csv(os.getcwd() + '\df_5min_master.csv', index = False)

    # Saves master df to master.csv
    master.to_csv(os.getcwd() + '\master.csv', index = False)
    return master, df_5min_master


def file_name(ID):
    now = datetime.now()
    Fdate= now.strftime("%Y%m%d")
    return "Trend_Virtual_Meter_Watt_"+Fdate+"_"+str(ID)+".csv"

def remove_csv(file_name): # removes file from cwd
    path = os.getcwd()+ "\\" + file_name
    if os.path.exists(path):
        os.remove(path)
        print("[FILE_INFO]  "+file_name+' cleared')
    else:
        print("[FILE_INFO]  "+file_name+' cannot be removed. Does it exist?')
    return
def send_to_space():
# set variables for Azure access
    # load access keys from secrets.json
    with open('appkeys.json') as f:
        secrets = json.load(f)
    ACCOUNT_NAME = secrets["ACCOUNT_NAME"]
    ACCOUNT_KEY = secrets["ACCOUNT_KEY"]
    CONTAINER_NAME = secrets["CONTAINER_NAME"]

    # create a BlobServiceClient object and authenticate with your Storage account key
    blob_service_client = BlobServiceClient(account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=ACCOUNT_KEY)

    # get a reference to the Blob container
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # upload the file to Blob storage
    with open("master.csv", "rb") as data:
        blob_client = container_client.upload_blob(name="master.csv", data=data, overwrite=True)

    # remove csvs
    return

def download_data():
    keys= open('appkeys.json')
    keydata = json.load(keys)

    print("[STARTUP_INFO]  "+"starting data collection...")
    pullData(keydata["IP_1"],keydata["ftp_user"],keydata["ftp_pass"],"1")
    daily_data_trim(1)
    pullData(keydata["IP_2"],keydata["ftp_user"],keydata["ftp_pass"],"2")
    daily_data_trim(2)
    pullData(keydata["IP_3"],keydata["ftp_user"],keydata["ftp_pass"],"3")
    daily_data_trim(3)


def main():


    while True:
        SERVER_IDS = [1, 2, 3]
        download_data()
        # set fillmaster to true to fill master with only the last data point
        merged_dadta = fill_master(SERVER_IDS, True)
        merged_data = merge_master(merged_dadta)
        # function to send data to s3 database
        send_to_space()
        print("[INFO]  "+"Done!")
        time.sleep(280)
        print("[INFO]  refreshing in 20 seconds...")
        time.sleep(20)

main()