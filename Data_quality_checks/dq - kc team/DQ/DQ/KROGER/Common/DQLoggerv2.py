# Databricks notebook source
dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType

class DQLogger: 
  'DQ logger class'
  
  LogColumns = StructType([   
    StructField('RunId', StringType(), True),
    StructField('Record_Key', StringType(), True),
    StructField('Record_Field_Name', StringType(), True),
    StructField('File_Name', StringType(), True),
    StructField('Rule_ID', StringType(), True),
    StructField('Rule_Status', StringType(), True),
    StructField('Records_Success', StringType(), True),
    StructField('Records_Failed', StringType(), True),
    StructField('Domain_Name', StringType(), True),
    StructField('Source_System', StringType(), True),
    StructField('Zone_Name', StringType(), True),
    StructField('Batch_ID', StringType(), True),
    StructField('Start_Datetime', StringType(), True),
    StructField('End_Datetime', StringType(), True)
  ])
  
  def __init__(self, RunId, Record_Key, Record_Field_Name, File_Name, Rule_ID, Rule_Status, Records_Success, Records_Failed, Domain_Name, Source_System, Zone_Name, Batch_ID, Start_Datetime, End_Datetime):
    self.RunId = RunId
    self.Record_Key= Record_Key
    self.Record_Field_Name=Record_Field_Name
    self.File_Name= File_Name
    self.Rule_ID= Rule_ID
    self.Rule_Status=Rule_Status
    self.Records_Success=Records_Success
    self.Records_Failed=Records_Failed
    self.Domain_Name= Domain_Name
    self.Source_System=Source_System
    self.Zone_Name= Zone_Name
    self.Batch_ID=Batch_ID
    self.Start_Datetime=Start_Datetime
    self.End_Datetime= End_Datetime
 
    
  #Method to perform read/write operation in checkpoint log
  def generateLogs(self, adlsSecret, accountname, adlscontainer, logPath, logFileName):
    print("********")
    print(logPath)
    print(logFileName)
    print("********")
    accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
    directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
    url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

    spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
    spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)
    
    #Temp location where DI log will be generated
    logTempPath = 'abfss://' + adlscontainer + '@'+accountname +'.dfs.core.windows.net/' + logPath+ "/temp_"+ logFileName
    
    #Declare data lake path where we want to write the data
    dqTargetLogPathADLS = 'abfss://' + adlscontainer + '@'+accountname +'.dfs.core.windows.net/' + logPath
        
    logvariables = [(self.RunId, self.Record_Key,self.Record_Field_Name,self.File_Name,self.Rule_ID,self.Rule_Status,self.Records_Success,self.Records_Failed,self.Domain_Name,self.Source_System,self.Zone_Name,self.Batch_ID,self.Start_Datetime,self.End_Datetime)]
        
    logErrorMsg=''
    
    try:
      #Create Ops Log DataFrame
      dqlogs = spark.createDataFrame(data=logvariables, schema = DQLogger.LogColumns)
      #display(dqlogs)

      # Write as json file
#       dqlogs.coalesce(1).write.format("json").option("header", "true").mode("overwrite").save(logTempPath)
      dqlogs.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(logTempPath)

      files = dbutils.fs.ls(logTempPath + "/")
#       ops_file = [file.path for file in files if file.path.endswith(".json")][0]
      ops_file = [file.path for file in files if file.path.endswith(".csv")][0]

      # Move the Json file to desired location
#       dbutils.fs.mv(ops_file, dqTargetLogPathADLS +"/" + "DQ_"+ self.RunId+"_"+logFileName+"_log.json")
      dbutils.fs.mv(ops_file, dqTargetLogPathADLS +"/" + "DQ_"+ self.RunId+"_"+logFileName+"_log.csv")

      # Remove all the audit files created by databricks
      dbutils.fs.rm(logTempPath, recurse = True)
  
    except Exception as ex:
      logErrorMsg = ex
      print(logErrorMsg)
    
      if logErrorMsg != '' or len(logErrorMsg) > 0:
        raise Exception(logErrorMsg)