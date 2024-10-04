# Databricks notebook source
# pip install snowflake-connector-python

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

run_date = str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2)
print (run_date)

dbutils.widgets.text('PipelineID', '')
PipelineID = dbutils.widgets.get('PipelineID')
print (PipelineID)

dbutils.widgets.text('DQLogSnowflakeDb', '')
DQLogSnowflakeDb = dbutils.widgets.get('DQLogSnowflakeDb')
print (DQLogSnowflakeDb)

dbutils.widgets.text('DQLogSnowflakeSchema', '')
DQLogSnowflakeSchema = dbutils.widgets.get('DQLogSnowflakeSchema')
print (DQLogSnowflakeSchema)

dbutils.widgets.text('dataFileName', '')
dataFileName = dbutils.widgets.get('dataFileName')
print (dataFileName)

# COMMAND ----------

dbutils.widgets.text('sf_user', '')
dbutils.widgets.text('sf_account', '')

sf_user = dbutils.widgets.get('sf_user')
sf_account = dbutils.widgets.get('sf_account')
print(sf_user)
print(sf_account)

dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

# Configuration to connect Snowflake
import snowflake.connector

#sf_user = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-SNOWFLAKE-USERNAME")
if sf_user is None:
    print("Fail to get sf_user")
else:
    print(sf_user)
sf_password = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-SNOWFLAKEUSERID-PASSWORD")
if sf_password is None:
    print("Fail to get sf_password")
else:
    print(sf_password)

#sf_account = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-SNOWFLAKE-ACCOUNT")
if sf_account is None:
    print("Fail to get sf_account")
else:
    print(sf_account)

sf_warehouse = "SELLOUT_GBL_ETL_WH"
#sf_warehouse = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-SNOWFLAKE-WAREHOUSE")
if sf_warehouse is None:
    print("Fail to get sf_warehouse")
else:
    print(sf_warehouse)

sf_role = "SELLOUT_GBL_DDL_LOCAL"
#sf_role = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-SNOWFLAKE-ROLE")
if sf_role is None:
    print("Fail to get sf_warehouse")
else:
    print(sf_role)

snowflake_connector = {
 "sfURL": sf_account + '.snowflakecomputing.com',
"sfUser": sf_user,
"sfPassword": sf_password,
"sfDatabase": DQLogSnowflakeDb,
"sfSchema": DQLogSnowflakeSchema,
"sfWarehouse": sf_warehouse
}

# COMMAND ----------

accountName_GDL = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME") 
# adlsSecret_GDL = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# #adlsContainer_GDL = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-CONTAINER")
adlsContainer_GDL = "ct-dna-shr-sellout"

#spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountName_GDL), adlsSecret_GDL) #For Global Data Lake

containerName = "ct-dna-shr-sellout"
accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
#filePath = "dev/UIF_config/ParameterFiles/KCNA/DQ/RulesMaster_NUMERATOR.json"
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)

# COMMAND ----------

#filepath = "externalsources/KCNA/IRI/Warehouse LVL Daily Data - Latest 6 Weeks/Raw/logs/"

dataFileName_PathPart = dataFileName.rsplit('/', 1)[0]
print(dataFileName_PathPart)
Log_Path = ''
filePartList= dataFileName_PathPart.split('/Data/',1)
if len(filePartList)>1:
    Log_Path = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2)
else:
    filePartList= dataFileName_PathPart.split('/data/',1)
    Log_Path = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) 
print(Log_Path)
# Log_Path = Log_Path.split("/", 1)[1]

print(Log_Path)

adlsPath = "abfss://" + adlsContainer_GDL + "@"+ accountName_GDL+ ".dfs.core.windows.net/" + Log_Path
print(adlsPath)
# pipelineid = "e56d9e4d-cedc-4551-8c9f-01f50b937612"
# run_date = "20220715"
#dataDf = spark.read.json("abfss://" + adlsContainer_GDL + "@"+ accountName_GDL+ ".dfs.core.windows.net/" + "dev/UIF_config/ParameterFiles/KCNA/DQ/RulesMaster_NUMERATOR.json")

# COMMAND ----------

#Defining the DQ Log File Structure
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, ArrayType
DQSchema = StructType([ 
             StructField("RUN_ID",StringType())
            ,StructField("RECORD_KEY",StringType())
            ,StructField("RECORD_FIELD_NAME",StringType())
            ,StructField("FILE_NAME",StringType())
            ,StructField("RULE_ID",StringType())
            ,StructField("RULE_STATUS",StringType())
            ,StructField("RECORDS_SUCCESS",IntegerType())
            ,StructField("RECORDS_FAILED",IntegerType())
            ,StructField("DOMAIN_NAME",StringType())
            ,StructField("SOURCE_SYSTEM",StringType())
            ,StructField("ZONE_NAME",StringType())
            ,StructField("BATCH_ID",StringType())
            ,StructField("START_DATETIME",TimestampType())
            ,StructField("END_DATETIME",TimestampType())
                    ])


# COMMAND ----------

#Read DQ Logs from ADLS Log Folder 
print(PipelineID)
df_dq = spark.read.format("csv").option("header", "true").schema(DQSchema).load(adlsPath +"/DQ_logs/DQ_"+PipelineID+"*")

# COMMAND ----------

#Load the DQ Logs into the Snowflake Table
snfk = df_dq.write.format("snowflake").options(**snowflake_connector).option("dbtable","tbl_dq_log").mode("append").save()