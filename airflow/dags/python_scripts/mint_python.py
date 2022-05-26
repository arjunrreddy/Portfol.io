import mintapi
import boto3
import pandas as pd
import json
from io import StringIO
from datetime import datetime, date
from .creds import mint_info, s3_bucket_info, mint_finance_accounts, postgres_db
import csv
import time

from psycopg2 import connect, sql
import psycopg2

print("Import Complete")

# STEP 1 Gathering the Data Via Mint API
def gathering_mint_data(ti):
    mint = mintapi.Mint(
        mint_info["mint_username"],  # Email used to log in to Mint
        mint_info["mint_password"],  # Your password used to log in to mint
        # Optional parameters
        mfa_method="soft-token",  # See MFA Methods section
        # Can be 'sms' (default), 'email', or 'soft-token'.
        # if mintapi detects an MFA request, it will trigger the requested method
        # and prompt on the command line.
        mfa_input_callback=None,  # see MFA Methods section
        # used with mfa_method = 'sms' or 'email'
        # A callback accepting a single argument (the prompt)
        # which returns the user-inputted 2FA code. By default
        # the default Python `input` function is used.
        mfa_token=mint_info['mint_mfa_token'],  # see MFA Methods section
        # used with mfa_method='soft-token'
        # the token that is used to generate the totp
        intuit_account=None,  # account name when multiple accounts are registered with this email.
        headless=True,  # Whether the chromedriver should work without opening a
        # visible window (useful for server-side deployments)
        # None will use the default account.
        session_path=None,  # Directory that the Chrome persistent session will be written/read from.
        # To avoid the 2FA code being asked for multiple times, you can either set
        # this parameter or log in by hand in Chrome under the same user this runs
        # as.
        imap_account=None,  # account name used to log in to your IMAP server
        imap_password=None,  # account password used to log in to your IMAP server
        imap_server=None,  # IMAP server host name
        imap_folder=None,  # IMAP folder that receives MFA email
        wait_for_sync=False,  # do not wait for accounts to sync
        wait_for_sync_timeout=300,  # number of seconds to wait for sync
        use_chromedriver_on_path=False,
    )
    
    #Storing Data from Mint API into account_info variable
    account_info = mint.get_accounts()
    
    #Pushing JSON Data of Account Info into XCOM for future functions
    ti.xcom_push(
        key = "account_info",
        value = json.dumps(account_info, indent=4, sort_keys=True, default=str)
    )


# Step 2 Deleting Previous Object in S3
def deleting_previous_s3():
    s3 = boto3.resource(
        service_name="s3",
        region_name=s3_bucket_info["region"],
        aws_access_key_id=s3_bucket_info["access_key_id"],
        aws_secret_access_key=s3_bucket_info["secret_access_key"],
    )
    bucket = s3.Bucket(s3_bucket_info["bucket"])
    # Iterates through all the objects, doing the pagination for you and deletes anything
    # starting with "m" in the account-data folder
    for obj in bucket.objects.all():
        key = obj.key
        if key.startswith("account-data/m"):
            obj.delete()

# Step 3 Converting Data to Dataframe
# Linking the ID's of the Account to Their Respective JSON Value

def converting_data_to_dataframe(ti):
    # Pulling Account_Info from gathering_mint_data function
    account_info = json.loads(
        ti.xcom_pull(key="account_info", task_ids="gathering_mint_data")
    )

    # Gathering All the Id's For Each Account
    account_info_dictionary = {i["id"]: i for i in account_info}
    
    # Making A List of All the Accounts/Columns
    list_of_columns = list(mint_finance_accounts.keys())

    # Making list of all values on all accounts to upload to database
    list_of_values = []
    for i in list_of_columns:
        id_to_search = mint_finance_accounts[i]
        current_account_info = account_info_dictionary[id_to_search]
        if (
            current_account_info["accountType"] == "credit"
            and current_account_info["currentBalance"] > 0
        ):
            list_of_values.append(float(current_account_info["currentBalance"]) * -1)
        elif current_account_info['accountName'] == 'BTC' and current_account_info['currentBalance'] == 0:
            list_of_values.append(float(2000))
        elif current_account_info['accountName'] == 'ETH' and current_account_info['currentBalance'] == 0:
            list_of_values.append(float(1600))
        elif current_account_info['accountName'] == 'USDC' and current_account_info['currentBalance'] < 1000:
            list_of_values.append(float(17500))
        else:
            list_of_values.append(float(current_account_info["currentBalance"]))
    
    ## Adding Current DateTime and Current Date Columns
    current_datetime = datetime.now()
    current_date = date.today()

    #Adding Current Date And Datetime to values and columns for database
    list_of_values.insert(0, current_date)
    list_of_values.insert(0, current_datetime)
    list_of_columns.insert(0, "date_now")
    list_of_columns.insert(0, "datetime_now")

    #Making str of column names to enter into sql query
    str_of_columns = ""
    for i in list_of_columns:
        str_of_columns = str_of_columns + i + ","

    #Converting to Dataframe to Upload Into S3 Bucket
    df = pd.DataFrame([list_of_values], columns=list_of_columns)
    ts = time.time()

    #Converting Dataframe to CSV to Push as XCOM
    df.to_csv("df.csv", index=False)
    ti.xcom_push(key="df", value="df.csv")

# Step 4 Uploading CSV Into Bucket
def uploading_csv_into_bucket(ti):

    # Making the Name of the file I'm going to Add with current time
    now = datetime.now()
    string_now = now.strftime("%m_%d_%Y_%H-%M-%S")
    file_name = "mint_" + string_now + ".csv"

    #Accessing S3 Bucket
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_bucket_info["access_key_id"],
        aws_secret_access_key=s3_bucket_info["secret_access_key"],
    )

    #Making Variable to add the csv to the bucket
    k = "account-data/" + file_name
   
    # Pulling Xcom of CSV back into Data Frame
    df = pd.read_csv(ti.xcom_pull(key="df", task_ids="converting_data_to_dataframe"))
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    #Placing Dataframe as CSV Into S3 Bucket
    s3.put_object(Bucket=s3_bucket_info["bucket"], Key=k, Body=csv_buffer.getvalue())

# Step 5 Getting All the Column Names And Putting Into String to Upload

def getting_column_names(ti):
    # Connecting to the S3 BUcket
    s3 = boto3.resource(
        service_name="s3",
        region_name=s3_bucket_info["region"],
        aws_access_key_id=s3_bucket_info["access_key_id"],
        aws_secret_access_key=s3_bucket_info["secret_access_key"],
    )
    bucket = s3.Bucket(s3_bucket_info["bucket"])
    # Iterates through all the objects, doing the pagination for you. Finds the CSV
    # Uploaded to the S3 Bucket to Get Data From
    for obj in bucket.objects.all():
        key = obj.key
        if key.startswith("account-data/m"):
            csv_to_look = key

    #Connecting to Postgres SQL Database
    con = psycopg2.connect(
        database=postgres_db["database_name"],
        user=postgres_db["user"],
        password=postgres_db["password"],
        host=postgres_db["host"],
        port=postgres_db["port"],
    )
    cur = con.cursor()

    #Getting list of column names of Database
    sql_statement_for_columns = """
    select *
    from dw_staging.account_data
    LIMIT 5;
    """
    cur.execute(sql_statement_for_columns)
    colnames = [desc[0] for desc in cur.description]
    con.close()

    # format the column names in a way that SQL will take them
    sql_col_names = ""
    for i in range(len(colnames)):
        if colnames[i] == "primary_key":
            pass
        elif i == len(colnames) - 1:
            sql_col_names += colnames[i]
        else:
            sql_col_names = sql_col_names + colnames[i] + ","

    ti.xcom_push(key="csv_to_look", value=csv_to_look)
    ti.xcom_push(key="sql_col_names", value=sql_col_names)

# Step 6 Uploading the Data to PostgresSQL


def upload_data_to_postgresql(ti):
    # First Gotta Connect To Postgres SQL Database
    con = psycopg2.connect(
        database=postgres_db["database_name"],
        user=postgres_db["user"],
        password=postgres_db["password"],
        host=postgres_db["host"],
        port=postgres_db["port"],
    )
    # cursor
    cur = con.cursor()

    #Pulling Column Names and CSV From XCOM
    sql_col_names = ti.xcom_pull(key="sql_col_names", task_ids="getting_column_names")
    csv_to_look = ti.xcom_pull(key="csv_to_look", task_ids="getting_column_names")
    
    #SQL Statement to Upload Data Into Postgres SQL Database
    sql_statement = """
    SELECT aws_s3.table_import_from_s3(
        'dw_staging.account_data',    
        '{sql_columns}', -- columns
        '(FORMAT CSV, DELIMITER E'','', HEADER true)', 
        aws_commons.create_s3_uri('{bucket_name}', '{csv}', 'us-east-2'), 
    aws_commons.create_aws_credentials('{s3_access_key_id}', '{s3_secret_access_key}', '')  
    );""".format(
        sql_columns=sql_col_names,bucket_name =s3_bucket_info["bucket"],  csv=csv_to_look,
        s3_access_key_id = s3_bucket_info["access_key_id"], s3_secret_access_key = s3_bucket_info["secret_access_key"]
    )

    #Executing SQL Statement and Closing the Connection
    cur.execute(sql_statement)
    con.commit()

    con.close()