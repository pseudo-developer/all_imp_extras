# Databricks notebook source
dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, ArrayType, LongType

class CommonOpsLogger: 
  'Common ops logger class'
  
  LogColumns =  StructType([   
    StructField('RUN_ID', StringType(), True),
    StructField('JOB_NAME', StringType(), True),
    StructField('RUN_DATE', StringType(), True),
    StructField('APP_ID', StringType(), True),
    StructField('DOMAIN_NAME', StringType(), True),
    StructField('SOURCE_NAME', StringType(), True),
    StructField('HOP_NAME', StringType(), True),
    StructField('START_DATETIME', StringType(), True),
    StructField('END_DATETIME', StringType(), True),
    StructField('RECORD_COUNT', ArrayType(StructType([StructField("fileName", StringType()), 
                StructField("noOfRecords", LongType()),StructField("type", StringType())])), True),
    StructField('STATUS', StringType(), True),
    StructField('MASTER_JOB_NAME', StringType(), True),
    StructField('SEQ_NUMBER', StringType(), True),
    StructField('ERROR_CODE', StringType(), True),
    StructField('ERROR_MESSAGE', StringType(), True)
 ])
  
  def __init__(self, run_id, job_name, run_date, app_id, domain_name, source_name, hop_name, start_datetime, end_datetime, record_count, status, master_job_name, seq_number, error_code='', error_message=''):
    self.run_id= run_id
    self.job_name=job_name
    self.run_date= run_date
    self.app_id= app_id
    self.domain_name=domain_name
    self.source_name=source_name
    self.hop_name=hop_name
    self.start_datetime= start_datetime
    self.end_datetime=end_datetime
    self.record_count= record_count
    self.status=status
    self.master_job_name=master_job_name
    self.seq_number= seq_number
    self.error_code=error_code
    self.error_message=error_message
    
  #Method to perform read/write operation in checkpoint log
  def generateLogs(self, adlsSecret, accountname, adlscontainer, logPath, logFileName):
    #spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountname), adlsSecret)
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
    opsTargetLogPathADLS = 'abfss://' + adlscontainer + '@'+accountname +'.dfs.core.windows.net/' + logPath
    
    logvariables =[(self.run_id, self.job_name, self.run_date, self.app_id, self.domain_name,self.source_name,self.hop_name, self.start_datetime, self.end_datetime, self.record_count, self.status, self.master_job_name, self.seq_number, self.error_code, self.error_message)]
    
    logErrorMsg=''
    
    try:
      #Create Ops Log DataFrame
      opslogs = spark.createDataFrame(data=logvariables, schema = CommonOpsLogger.LogColumns)
      #display(opslogs)

      # Write as json file
      opslogs.coalesce(1).write.format("json").option("header", "true").mode("overwrite").save(logTempPath)

      files = dbutils.fs.ls(logTempPath + "/")
      ops_file = [file.path for file in files if file.path.endswith(".json")][0]

      # Move the Json file to desired location
      dbutils.fs.mv(ops_file, opsTargetLogPathADLS +"/" + "OP_"+ self.run_id+"_"+logFileName+"_log.json")

      # Remove all the audit files created by databricks
      dbutils.fs.rm(logTempPath, recurse = True)
  
    except Exception as ex:
      logErrorMsg = ex
      print(logErrorMsg)
    
    if logErrorMsg != '' or len(logErrorMsg) > 0:
      raise Exception(logErrorMsg)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, ArrayType, LongType

class CommonOpsLogger1: 
  'Common ops logger class'
  
  LogColumns =  StructType([ 
    StructField("OP_LOG",StructType([ 
    StructField('RUN_ID', StringType(), True),
    StructField('JOB_NAME', StringType(), True),
    StructField('RUN_DATE', StringType(), True),
    StructField('APP_ID', StringType(), True),
    StructField('DOMAIN_NAME', StringType(), True),
    StructField('SOURCE_NAME', StringType(), True),
    StructField('HOP_NAME', StringType(), True),
    StructField('START_DATETIME', StringType(), True),
    StructField('END_DATETIME', StringType(), True),
    StructField('RECORD_COUNT', ArrayType(StructType([StructField("fileName", StringType()), 
                StructField("noOfRecords", LongType()),StructField("type", StringType())])), True),
    StructField('STATUS', StringType(), True),
    StructField('MASTER_JOB_NAME', StringType(), True),
    StructField('SEQ_NUMBER', StringType(), True),
    StructField('ERROR_CODE', StringType(), True),
    StructField('ERROR_MESSAGE', StringType(), True)
 ]))])
  
  def __init__(self, run_id, job_name, run_date, app_id, domain_name, source_name, hop_name, start_datetime, end_datetime, record_count, status, master_job_name, seq_number, error_code='', error_message=''):
    self.run_id= run_id
    self.job_name=job_name
    self.run_date= run_date
    self.app_id= app_id
    self.domain_name=domain_name
    self.source_name=source_name
    self.hop_name=hop_name
    self.start_datetime= start_datetime
    self.end_datetime=end_datetime
    self.record_count= record_count
    self.status=status
    self.master_job_name=master_job_name
    self.seq_number= seq_number
    self.error_code=error_code
    self.error_message=error_message
    
  #Method to perform read/write operation in checkpoint log
  def generateLogs(self, adlsSecret, accountname, adlscontainer, logPath, logFileName):
    #spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountname), adlsSecret)
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
    opsTargetLogPathADLS = 'abfss://' + adlscontainer + '@'+accountname +'.dfs.core.windows.net/' + logPath
    
    logvariables =[(self.run_id, self.job_name, self.run_date, self.app_id, self.domain_name,self.source_name,self.hop_name, self.start_datetime, self.end_datetime, self.record_count, self.status, self.master_job_name, self.seq_number, self.error_code, self.error_message)]
    
    logErrorMsg=''
    
    try:
      #Create Ops Log DataFrame
      opslogs = spark.createDataFrame(data=logvariables, schema = CommonOpsLogger1.LogColumns)
      #display(opslogs)

      # Write as json file
      opslogs.coalesce(1).write.format("json").option("header", "true").mode("overwrite").save(logTempPath+'temp')

      
  
    except Exception as ex:
      logErrorMsg = ex
      print(logErrorMsg)
    
    if logErrorMsg != '' or len(logErrorMsg) > 0:
      raise Exception(logErrorMsg)

# COMMAND ----------

