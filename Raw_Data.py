import os
import sys
import time
import json
from datetime import datetime
import pandas as pd
import pyodbc, struct
from azure.identity import DefaultAzureCredential

# pullData function takes in the IP address, username, password,
# and server number and saves todays file to the current directory


def pullData(FTP_HOST, FTP_USER, FTP_PASS, SERVER_NUM):
    import pysftp as sftp

    # gets todays file name based on date
    now = datetime.now()
    Fdate = now.strftime("%Y%m%d")
    Fname = "Trend_Virtual_Meter_Watt_" + Fdate + "_" + str(SERVER_NUM) + ".csv"

    # if file already exists, delete it
    if os.path.exists(Fname):
        remove_csv(Fname)

    print("[DOWNLOAD_INFO]  Attempting to download" + Fname + " at ")
    print("[DOWNLOAD_INFO]  " + str(datetime.now()))
    print("[DOWNLOAD_INFO]  " + str(time.time()))

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


# daily_data_trim() is a function that trims the extre date and time columns from the data and renames them so there are no conflicts


def daily_data_trim(ID):
    Fname = file_name(ID)
    df = pd.read_csv(Fname, header=0)
    remove_csv(Fname)
    # Find number of columns with data
    search_row = df.iloc[1, :].dropna()
    num_col = len(search_row)
    num_meters = int((num_col - 2) / 3)
    meters = ["Date", "Time"]
    # Make list to rename column titles
    for i in range(num_meters):
        meters.append("Server" + str(ID) + "_" + "meter" + str(i + 1) + "_avg")
        meters.append("Server" + str(ID) + "_" + "meter" + str(i + 1) + "_min")
        meters.append("Server" + str(ID) + "_" + "meter" + str(i + 1) + "_max")
    # Preparing and saving csv
    df = df.iloc[:, :num_col]
    df.columns = meters
    df.to_csv(Fname, index=False)


def calculated_data(ID_LIST, fill_mode=False):
    df_5min_master = pd.DataFrame()
    dfs = [
        pd.read_csv(os.getcwd() + "/" + file_name(id), index_col=False)
        for id in ID_LIST
    ]
    df_5min_master = dfs[0]
    for df in dfs[1:]:
        df_5min_master = pd.merge(df_5min_master, df, on=["Date", "Time"], how="outer")
    df_5min_master = df_5min_master.loc[
        :, ~df_5min_master.columns.duplicated(keep="first")
    ].copy()
    # Data checking and removal
    if fill_mode == False:
        df_5min_master = df_5min_master.iloc[-3, :]
        print("[INFO]  " + "Data collected at " + str(df_5min_master["Time"]))
        df_5min_master = df_5min_master.to_frame().T
    if df_5min_master.isnull().values.any() and fill_mode == False:
        # Find the missing data point
        missing_data_point = df_5min_master[df_5min_master.isnull().any(axis=1)]
        print(
            "[CAUTION/WARN]  "
            + "Missing Data at "
            + str(missing_data_point["Time"])
            + "...Skipping this data point..."
        )
        # Remove the missing data point from the dataframe
        df_5min_master = df_5min_master.dropna()
    elif df_5min_master.isnull().values.any() and fill_mode == True:
        # Find any rows with missing data and delete the whole row
        print("[CAUTION/WARN]  " + "Missing Data...Deleting rows with missing data...")
        df_5min_master = df_5min_master.dropna(axis=0, how="any")

    # Data checking complete, continue with calculations

    # combine date and time into dateTime column

    df_5min_master["dateTime"] = pd.to_datetime(
        df_5min_master["Date"] + " " + df_5min_master["Time"]
    )
    df_5min_master.to_csv("test.csv")
    # absolute value the data in columns titled "Server1_meter10_avg" and "Server1_meter10_min" and "Server1_meter10_max"
    df_5min_master["Server1_meter10_avg"] = df_5min_master["Server1_meter10_avg"].abs()
    df_5min_master["Server1_meter10_min"] = df_5min_master["Server1_meter10_min"].abs()
    df_5min_master["Server1_meter10_max"] = df_5min_master["Server1_meter10_max"].abs()

    # assigns servers to the correct floor. See servermap.xlsx for more details
    df_5min_master["1st_Floor"] = (
        df_5min_master["Server1_meter1_avg"] + df_5min_master["Server3_meter1_avg"]
    )
    df_5min_master["2nd_Floor"] = (
        df_5min_master["Server1_meter3_avg"]
        + df_5min_master["Server1_meter5_avg"]
        + df_5min_master["Server1_meter10_avg"]
        + df_5min_master["Server3_meter2_avg"]
    )
    df_5min_master["3rd_Floor"] = (
        df_5min_master["Server1_meter4_avg"]
        + df_5min_master["Server1_meter7_avg"]
        + df_5min_master["Server1_meter9_avg"]
        + df_5min_master["Server3_meter4_avg"]
    )
    df_5min_master["4th_Floor"] = (
        df_5min_master["Server1_meter6_avg"]
        + df_5min_master["Server1_meter8_avg"]
        + df_5min_master["Server1_meter13_avg"]
        + df_5min_master["Server3_meter3_avg"]
    )
    df_5min_master["Utilities"] = (
        df_5min_master["Server1_meter11_avg"]
        + df_5min_master["Server1_meter12_avg"]
        + df_5min_master["Server2_meter2_avg"]
        + df_5min_master["Server2_meter3_avg"]
        + df_5min_master["Server2_meter5_avg"]
        + df_5min_master["Server3_meter5_avg"]
        + df_5min_master["Server3_meter6_avg"]
    )
    df_5min_master["TOTAL"] = (
        df_5min_master["1st_Floor"]
        + df_5min_master["2nd_Floor"]
        + df_5min_master["3rd_Floor"]
        + df_5min_master["4th_Floor"]
        + df_5min_master["Utilities"]
    )

    # calculate the Kwh for each floor and total
    df_5min_master["1st_Floor_Kwh"] = df_5min_master["1st_Floor"] * 5 / 60 / 1000
    df_5min_master["2nd_Floor_Kwh"] = df_5min_master["2nd_Floor"] * 5 / 60 / 1000
    df_5min_master["3rd_Floor_Kwh"] = df_5min_master["3rd_Floor"] * 5 / 60 / 1000
    df_5min_master["4th_Floor_Kwh"] = df_5min_master["4th_Floor"] * 5 / 60 / 1000
    df_5min_master["Utilities_Kwh"] = df_5min_master["Utilities"] * 5 / 60 / 1000
    df_5min_master["TOTAL_Kwh"] = df_5min_master["TOTAL"] * 5 / 60 / 1000

    df_5min_calc = pd.DataFrame()

    df_5min_calc = df_5min_master[
        [
            "dateTime",
            "1st_Floor",
            "2nd_Floor",
            "3rd_Floor",
            "4th_Floor",
            "Utilities",
            "TOTAL",
            "1st_Floor_Kwh",
            "2nd_Floor_Kwh",
            "3rd_Floor_Kwh",
            "4th_Floor_Kwh",
            "Utilities_Kwh",
            "TOTAL_Kwh",
        ]
    ].copy()
    return df_5min_calc


def file_name(ID):
    now = datetime.now()
    Fdate = now.strftime("%Y%m%d")
    return "Trend_Virtual_Meter_Watt_" + Fdate + "_" + str(ID) + ".csv"


def remove_csv(file_name):  # removes file from cwd
    path = os.getcwd() + "\\" + file_name
    if os.path.exists(path):
        os.remove(path)
        print("[FILE_INFO]  " + file_name + " cleared")
    else:
        print("[FILE_INFO]  " + file_name + " cannot be removed. Does it exist?")
    return


def get_from_space(db_table_name):
    rows = []
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP 1 * FROM {db_table_name} ORDER BY dateTime")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        return rows


def send_to_space(df_5_min_master, db_table_name):
    # set variables for Azure access
    with get_conn() as conn:
        cursor = conn.cursor()
        print("sending to space:")
        # print(df_5_min_master)
        for index, row in df_5_min_master.iterrows():
            try:
                cursor.execute(
                    f"INSERT INTO {db_table_name} (dateTime, First_Floor, Second_Floor, Third_Floor, Fourth_Floor, Utilities, TOTAL, First_Floor_Kwh, Second_Floor_Kwh, Third_Floor_Kwh, Fourth_Floor_Kwh, Utilities_Kwh, TOTAL_Kwh) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row["dateTime"],
                    row["1st_Floor"],
                    row["2nd_Floor"],
                    row["3rd_Floor"],
                    row["4th_Floor"],
                    row["Utilities"],
                    row["TOTAL"],
                    row["1st_Floor_Kwh"],
                    row["2nd_Floor_Kwh"],
                    row["3rd_Floor_Kwh"],
                    row["4th_Floor_Kwh"],
                    row["Utilities_Kwh"],
                    row["TOTAL_Kwh"],
                )
            except Exception as e:
                print("Error executing SQL statement: {}".format(e))
    return

    # remove csvs
    return


def get_conn():
    # Load the connection string and Azure credentials from appkeys.json
    with open("appkeys.json") as f:
        appkeys = json.load(f)
    connection_string = appkeys["AZURE_SQL_CONNECTIONSTRING"]
    credential = DefaultAzureCredential()

    # Get an access token for the Azure SQL Database
    token = credential.get_token("https://database.windows.net/")

    # Set the access token in the connection string
    conn_str = connection_string.format(token=token.token)

    # Connect to the Azure SQL Database
    conn = pyodbc.connect(conn_str)

    return conn


def download_data():
    keys = open("appkeys.json")
    keydata = json.load(keys)
    # daily_data_trim() is a function that trims the extre date and time columns from the data and renames them so there are no conflicts
    print("[STARTUP_INFO]  " + "starting data collection...")
    # for loop that repeats
    pullData(keydata["IP_1"], keydata["ftp_user"], keydata["ftp_pass"], "1")
    daily_data_trim(1)
    pullData(keydata["IP_2"], keydata["ftp_user"], keydata["ftp_pass"], "2")
    daily_data_trim(2)
    pullData(keydata["IP_3"], keydata["ftp_user"], keydata["ftp_pass"], "3")
    daily_data_trim(3)
    return


def setup_database(db_table_name):
    rows = []
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE TABLE {db_table_name} (dateTime dateTime,First_Floor FLOAT,Second_Floor FLOAT,Third_Floor FLOAT,Fourth_Floor FLOAT,Utilities FLOAT,TOTAL FLOAT,First_Floor_Kwh FLOAT,Second_Floor_Kwh FLOAT,Third_Floor_Kwh FLOAT,Fourth_Floor_Kwh FLOAT,Utilities_Kwh FLOAT,TOTAL_Kwh FLOAT)"
        )
    return


def check_for_duplicates(old, new, db_table_name):
    if old is None:
        old = get_from_space(db_table_name)
        if len(old) == 0:
            print("[DATABASE] No data in database")
            return True

    if isinstance(old, list):
        print("printing old")
        print(old)
        print("printing new")
        print(new)
        old_latest_date = old[0][0]
        print("old latest date: " + str(old_latest_date))
        new_latest_date = new["dateTime"].iloc[0]
        print("new latest date: " + str(new_latest_date))

        if old_latest_date == new_latest_date:
            print("[DUPLICATE_WARN] No new data")
            return False
        else:
            return True

    elif isinstance(old, pd.DataFrame):
        if old.equals(new):
            print("[DUPLICATE_WARN] No new data")
            return False
        else:
            return True


def main(argv):
    old_rocket = None
    table_name = "resTest1"
    startup_database = bool(argv)
    if startup_database == True:
        print("[STARTUP_INFO]  " + "Creating database table.")
        # *** to change database name go to setup_database() ***
        setup_database(table_name)
    else:
        print("[STARTUP_INFO]  " + "Assumeing table already exists.")

    while True:
        SERVER_IDS = [1, 2, 3]
        download_data()
        rocket = calculated_data(SERVER_IDS)

        # checks for duplicates in and sends only if new data is present
        if startup_database == False:
            data_check = check_for_duplicates(old_rocket, rocket, table_name)
        elif startup_database == True:
            # on startup unable to check for duplicates beacuse its empty, send anyway
            data_check = True
        # data_check must be true to send data to database
        if data_check == True:
            send_to_space(rocket, table_name)
            old_rocket = rocket
            print("[INFO]  " + " Rocket Liftoff!")
        print("[INFO]  " + "sleeping until next data pull...")
        time.sleep(280)
        print("[INFO]  refreshing in 20 seconds...")
        time.sleep(20)
    return


main(sys.argv[1:])
