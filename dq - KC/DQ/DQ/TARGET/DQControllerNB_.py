# Databricks notebook source
dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./Adapters/KC-DQ-RGMRuleAdapter"

# COMMAND ----------

# MAGIC %run "./Common/KC-CommonOpsLoggerClass"

# COMMAND ----------

# MAGIC %run "./Common/DQLoggerv2"

# COMMAND ----------

adlsSecret = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw")
#adlsSecret= dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# accountsecret = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# accountname = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
# #containername = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-CONTAINER") 
containername = "ct-dna-shr-sellout"
# spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountname), accountsecret) 

# COMMAND ----------

accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)
spark.conf.set('spark.sql.execution.arrow.enabled', 'true')

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import concat,concat_ws, col, lit, when

def cleaning_flagged_src_file(srcflagged_df):
 
  ## create two lists for DQ flag and Desc
  listHeader = srcflagged_df.columns
  dqFlagCols = []
  dqDescCols = []
  allDqCols = []
  dqFlagCols_final = []

  for col in listHeader:
    if col[:2] == "DQ" and col[-4:] == "DESC":
      dqDescCols.append(col)
      allDqCols.append(col)
    elif  col[:2] == "DQ" and col[-4:] != "DESC":
      dqFlagCols.append(col)
      allDqCols.append(col)
    else:
      pass

  dqFlagCols_final = [i.replace("DQ_","") for i in dqFlagCols]

  print(listHeader)
  print(dqFlagCols)
  print(dqDescCols)
  print(dqFlagCols_final)
      
  # Replacing Null with NA in DQ cols

#   testdF = srcflagged_df.fillna(value="NA",subset=[c for c in dqDescCols])
#   testdF = testdF.fillna(value="",subset=[c for c in dqFlagCols])
  srcflagged_df.show()
#   srcflagged_df.select([count(when(isnan('DQ_RGM_SA_DO_CASHRETAILER_01_DESC'),True))]).show()

  testdF = srcflagged_df.fillna(value="Blank",subset=dqDescCols)
#   print("after NA")
#   display(testdF)
  testdF = testdF.fillna(value="",subset=dqFlagCols)
#   print("after empty")
#   display(testdF)

  # update DQ flag field

  testdF1 = testdF.withColumn('DQ_Flag', concat(*[c for c in dqFlagCols]))
  testdF2 = testdF1.withColumn('DQ_Flag', when((testdF1.DQ_Flag).contains('Failed with Error'), 'Error').when((testdF1.DQ_Flag).contains('Failed with Warning'), 'Warning').otherwise(''))

#   Within for loop Adding new DQ Desc flag field

  testdF3 = testdF2.withColumn('DQ_Description', concat_ws("|",*[c for c in dqDescCols]))
#   display(testdF3)
  testdF4 = testdF3.withColumn('DQ_Description', f.regexp_replace(testdF3.DQ_Description,"Blank\|",''))
  testdF5 = testdF4.withColumn('DQ_Description', f.regexp_replace(testdF4.DQ_Description,'\|Blank',''))
  testdF6 = testdF5.withColumn('DQ_Description', f.regexp_replace(testdF5.DQ_Description,'\|','\ | '))
  testdF7 = testdF6.withColumn('DQ_Description', f.regexp_replace(testdF6.DQ_Description,"Blank",''))

#   display(testdF6.select("DQ_Flag","DQ_Description"))

  # Drop extra DQ fields
  final_df = testdF7.drop(*[c for c in allDqCols])
  display(final_df)
  return final_df

# COMMAND ----------

accountname = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
#containername = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-CONTAINER") 
containername = "ct-dna-shr-sellout"
containerName = "ct-dna-shr-sellout"

# COMMAND ----------

import json
from re import search
from datetime import datetime
import uuid
import pandas as pd
from pyspark.sql import SQLContext
import pyspark.pandas as ps

class DQController:
  rulesStr = ''
  iriAdapterNB = ''
  inputDataViewName = ''  

  def __init__(self):
    pass   # No instance variables to initialise

  def create_global_temp_view_source(self, FileNameWpath, viewName, fileType): 
    print("INSIDE global_view printing file name with path ",FileNameWpath)   
    if (fileType == "CSV"):
      viewdF = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + FileNameWpath, header=True, sep="|")  
      display(viewdF)
      viewdF.createOrReplaceGlobalTempView(viewName)
    elif (fileType == "EXCEL"):
      pandasDF=ps.read_excel("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=0)
      viewdF=pandasDF.to_spark() 
      display(viewdF)
      viewdF.createOrReplaceGlobalTempView(viewName)
      # viewName=dbutils.notebook.run("Read_Excel", 60)["viewName"]
    return viewName
  
  def saveLogFileInADLS(self, df, accountName, containerName, accountSecret, filePath, dataFileName,fileFormat):  ##
    accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
    directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
    url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

    spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
    spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)
    
    # fileFormat=""
    # if search(".xlsx",dataFileName):
    #   fileFormat="EXCEL"
    outputTempPath = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + filePath + "/temp_"+ dataFileName    
    # df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(outputTempPath) Mahesh changes
    if fileFormat == "EXCEL":
      df.to_excel(outputTempPath)
      files = dbutils.fs.ls(outputTempPath + "/")
      ops_file = [file.path for file in files if file.path.endswith(".csv")][0]
      errorFilePath = filePath + "/" + dataFileName + ".csv"
      opTargetPathADLS = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + errorFilePath 
      # print("Printing opTargetPathADLS",opTargetPathADLS)
      # print("Printing File name ",ops_file)
      # Move the csv file to desired location
      dbutils.fs.mv(ops_file, opTargetPathADLS)
      # Remove all the extra files created by databricks
      dbutils.fs.rm(outputTempPath, recurse = True)
    else:
      df.coalesce(1).write.format("csv").option("header", "true").option("delimiter","|").option('nullValue', None).mode("overwrite").save(outputTempPath)
      files = dbutils.fs.ls(outputTempPath + "/")
      ops_file = [file.path for file in files if file.path.endswith(".csv")][0]
      errorFilePath = filePath + "/" + dataFileName + ".csv"
      opTargetPathADLS = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + errorFilePath 
      # print("Printing opTargetPathADLS",opTargetPathADLS)
      # print("Printing File name ",ops_file)
      # Move the csv file to desired location
      dbutils.fs.mv(ops_file, opTargetPathADLS)
      # Remove all the extra files created by databricks
      dbutils.fs.rm(outputTempPath, recurse = True)
    return errorFilePath
  
  def get_rulesby_datafile(self,_dataFileName):
    try:       
      self.inputDataViewName = 'src_' + datetime.now().strftime("%Y%m%d%H%M%S")
      print('Input data view name '+ self.inputDataViewName)
      print(_dataFileName)
      fileType = ""
      if search(".xlsx",_dataFileName):
        fileType = "EXCEL"
      else:
        fileType = "CSV"
      if search("Source_Files", _dataFileName):
        print("line 40")
        print("printing data filr name",_dataFileName)
        if search("TARGET",_dataFileName):
            a = TargetKCNARuleAdapter()
            rulesStr = a.get_all_Active_Rules(_dataFileName,fileType)
            print("printing rule string",rulesStr)
            print(_dataFileName)
            if rulesStr !='':          
              x = self.create_global_temp_view_source(_dataFileName, self.inputDataViewName, fileType) #Create source data global view   
              data_file = spark.sql("select * from global_temp." + x )
            return rulesStr
      else:
        rulesStr = "Unrecognized Program Name"
        return rulesStr
        dbutils.notebook.exit("Unrecognized Program Name")
    except Exception as ex:
      logErrorMsg = ex
      print(logErrorMsg)
    
      if logErrorMsg != '' or len(logErrorMsg) > 0:
        raise Exception(logErrorMsg)
  
  
  def run_with_retry(self,notebook, timeout, args = {}, max_retries = 3):
    num_retries = 0
    while True:
      try:
        return dbutils.notebook.run(notebook, timeout, args)
      except Exception as e:
        if num_retries > max_retries:
          raise e
        else:
          print("Retrying error", e)
          num_retries += 1
          
  def isGTCFile(self,_dataFileName):
    if search("GTC", _dataFileName):
      return True
    else:
      return False

  def generateDQLog(self, resultstr, pipelineId):
    for i in range(len(resultstr)):
      Record_Key = resultstr[i]['Record_Key']
#       Record_Field_Name = resultstr[i]['Record_Field_Name']
      val=resultstr[i]['Record_Field_Name']
      if len(val)>1:
        field_names = val.split(",")
        Record_Field_Name='|'.join(field_names)
      else:
        Record_Field_Name=val
      File_Name = resultstr[i]['File_Name'].split("/")[-1]
      Rule_ID = resultstr[i]['RuleID']
      
      Rule_Status = '' 
      Unexpected_percentage = float(resultstr[i]['Unexpected percentage'])
      
      Rule_Action = resultstr[i]['Rule_action']
      RaiseException = False
      
      if Unexpected_percentage == -1:
        Rule_Status = "Schema Validation - Failed"
        if Rule_Action == "Error":
          print("Raise Exception after generating the logs")
          RaiseException = True
      else:
        Expected_percentage = 100 - Unexpected_percentage
        Rule_Threshold = int(resultstr[i]['Rule_Threshold'])
        if Expected_percentage >= Rule_Threshold:
          Rule_Status = "Success"
        else:
          Rule_Status = "Failed"
      Records_Success = int(resultstr[i]['Records_Success'])
      Records_Failed = int(resultstr[i]['Records_Failed'])
      Domain_Name = resultstr[i]['Domain_Name']
      Source_System = resultstr[i]['Source_System']
      Zone_Name = resultstr[i]['Zone_Name']
      Batch_ID = resultstr[i]['Batch_ID']
      Start_Datetime = resultstr[i]['Start_Datetime']
      End_Datetime = resultstr[i]['End_Datetime']
      
      RunId = pipelineId
      
      dataFileName = resultstr[i]['File_Name']
      dataFileName_PathPart = dataFileName.rsplit('/', 1)[0]
      Log_Path = ''
      filePartList= dataFileName_PathPart.split('/Data/',1)
      if len(filePartList)>1:
        Log_Path = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/DQ_logs/" 
      else:
        filePartList= dataFileName_PathPart.split('/data/',1)
        Log_Path = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/DQ_logs/" 
      print(Log_Path)
      adlscontainer= Log_Path.split("/")[0]
      Log_Path= str(Log_Path[Log_Path.find("/")+1:])
      
      filename = Rule_ID + "_" + dataFileName.split("/")[-1].split(".")[0]
      
#     #Initialize DQ Log Object
      logObject= DQLogger(RunId, Record_Key,Record_Field_Name,File_Name,Rule_ID,Rule_Status,Records_Success,Records_Failed,Domain_Name,Source_System,Zone_Name,Batch_ID,Start_Datetime,End_Datetime) 
      print(Log_Path)
      print(filename)
      logObject.generateLogs(adlsSecret, accountname, adlscontainer, Log_Path, filename)
      if RaiseException == True:
        print("Logger::Exception Raised")
#         raise Exception("Rule Execution Error")
    
  def generateOpsLog(self, resultstr, pipelineId, raiseException):    
    
    dataFileName = resultstr[0]['File_Name']
    dataFileName_FilePart = dataFileName.split("/")[-1]
    dataFileName_PathPart = dataFileName.rsplit('/', 1)[0]
    
    
    DQTargetLogPath = ''
    print(dataFileName_PathPart)
    filePartList= dataFileName_PathPart.split('/Data/',1)
    
    if len(filePartList)>1:
      DQTargetLogPath = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/audit_logs/" 
    else:
      filePartList= dataFileName_PathPart.split('/data/',1)
      DQTargetLogPath = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/audit_logs/" 
    print(DQTargetLogPath)

    Log_Path = DQTargetLogPath 
    RunId = pipelineId
    Job_Name = "PL_DATA_QUALITY_CHILD"
    Run_Date = str(datetime.today().strftime("%Y/%m/%d"))
    AppId = pipelineId
    Domain_Name = resultstr[0]['Domain_Name'] 
    Source_Name = resultstr[0]['Source_System'] 
    Hop_Name = resultstr[0]['Zone_Name'] 
    Start_Datetime = resultstr[0]['Start_Datetime'] 
    End_Datetime =  resultstr[0]['End_Datetime']
    sourceFileName = dataFileName_FilePart
    
    targetFileName = '' #dataFileName_FilePart.split('.')[0]  
    
    RowsWritten = int(resultstr[0]['Records_Success']) + int(resultstr[0]['Records_Failed'])
    RowsRead = int(resultstr[0]['Records_Success']) + int(resultstr[0]['Records_Failed'])
    record_count=[]
    record_count.append({"fileName":sourceFileName, "noOfRecords":int(RowsRead), "type":"source"})
    
    Unexpected_percentage = float(resultstr[0]['Unexpected percentage'])
    Exected_percentage = 100 - Unexpected_percentage
    Rule_Threshold = int(resultstr[0]['Rule_Threshold'])
    
    Status = "Failed" 
    if len(resultstr) > 0:
      if raiseException == "":
        Status = "Success"
      
    Master_Job_Name = ""
    Seq_Number = 1
    Error_Code = ""
    Error = raiseException
    
    adlscontainer= Log_Path.split("/")[0]
    #spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountname), adlsSecret)

    Log_Path= str(Log_Path[Log_Path.find("/")+1:])

#     #Initialize Ops Log Object
    logObject= CommonOpsLogger(RunId,Job_Name,Run_Date,AppId,Domain_Name,Source_Name,Hop_Name,Start_Datetime,End_Datetime,record_count, Status, Master_Job_Name, Seq_Number, Error_Code, Error)
   
    fileName=sourceFileName.split(".")[0]
    print(fileName)
    logObject.generateLogs(adlsSecret, accountname, adlscontainer, Log_Path, fileName)
        

  def execute_rulesby_ruleID_and_dataFileName(self,rulesStr,_dataFileName,PipelineID):
    ruleList = json.loads(rulesStr)
    print(ruleList)
    mydictionary = {}
    mydictionary["dqresults"] = []
    dqresults = mydictionary["dqresults"]
    errorFilePath = ''
    filePartList= _dataFileName.split('/Data/',1)  
    fileFormat = ""
    if search(".xlsx",_dataFileName):
      fileFormat = "EXCEL"
    else:
      fileFormat = "CSV" 
    if len(filePartList) > 1:
      errorFilePath = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/error_logs" 
    else:
      filePartList= _dataFileName.split('/data/',1)
      errorFilePath = filePartList[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/error_logs" 
    fileName = _dataFileName.rsplit('/', 1)[1]
    sourceFolder= _dataFileName.rsplit('/', 1)[0]
    refineFolder = _dataFileName.rsplit('/', 1)[0].replace("Raw","Refine") 
    
    for rc in ruleList:
      if rc[0] == "Consistency":
        print("Calling Consistency NB to process Rule: " + rc[1])
        try:                    
          saveErrorLog = 'true'  
          adlsDetailsJSON= {}
          adlsDetailsJSON['accountname']= accountname 
          adlsDetailsJSON['containername']= containername
          adlsDetailsJSON['accountsecret']= adlsSecret
          adlsDetailsJSON['filepath']= errorFilePath
          errorFileName= 'ERROR_'+ PipelineID + '_' + rc[1] + '_' + fileName.split('.')[0]
          adlsDetailsJSON['filename']= errorFileName
          if(len(rc)<=13):
            adlsDetailsJSON['rule_desc']= ''
            flagSourceFile = 'false'
          else:
            adlsDetailsJSON['rule_desc']= rc[14] 
            
            adlsDetailsJSON['source_file']= fileName
#             adlsDetailsJSON['rule_priority']= rc[-1]   # rule_priority added
            flagSourceFile = rc[15] 
          print(adlsDetailsJSON)
    
          print("This is srcGlbViewName"+rc[3])
          print("This is refDataGlbViewName"+rc[5])
          print("This is inputDataViewName"+self.inputDataViewName)
          
          res = dbutils.notebook.run("./KC-DQ-ConsistencyCheckNBv3", 3600, {"RuleID":rc[1], "rule_label":rc[2],"srcGlbViewName":rc[3],"srcCol":rc[4],"refDataGlbViewName":rc[5],"refCol":rc[6], "saveErrorLog": saveErrorLog, "adlsDetails": json.dumps(adlsDetailsJSON), "dataFileGlbViewName": self.inputDataViewName, "flagInSource": flagSourceFile, "rulePriority": rc[16],"env":env})  ##
  
          dqresult = json.loads(res)
          dqresult['Zone_Name'] = rc[7]
          dqresult['Rule_Threshold'] = rc[8]
          dqresult['Domain_Name'] = rc[9]
          dqresult['Source_System'] = rc[10]
          dqresult['Rule_action'] = rc[11]
          dqresult['File_Name'] = "ct-dna-shr-sellout" + _dataFileName # rc[11] #"globaldatalake" + rc[11]
          dqresult['Rule_Priority'] = rc[16]
          
          if(len(rc)<=13):   
            dqresult['Rule_Name'] = ''   
            dqresult['Rule_Desc'] = ''   
          else:
            dqresult['Rule_Name'] = rc[13]  
            dqresult['Rule_Desc'] = rc[14] 
            
          dqresults.append(dqresult)
        except Exception as e:
          print(e)
      elif rc[0] == "Validity":
        print("Calling Validity NB to process Rule: " + rc[1])
        try:                    
          saveErrorLog = 'true'  
          adlsDetailsJSON= {}
          adlsDetailsJSON['accountname']= accountname 
          adlsDetailsJSON['containername']= containername
          adlsDetailsJSON['accountsecret']= adlsSecret
          adlsDetailsJSON['filepath']= errorFilePath
          errorFileName= 'ERROR_'+ PipelineID + '_' + rc[1] + '_' + fileName.split('.')[0]
          adlsDetailsJSON['filename']= errorFileName
          if(len(rc)<=13):
            adlsDetailsJSON['rule_desc']= ''
            flagSourceFile = 'false'
          else:
            adlsDetailsJSON['rule_desc']= rc[14] 
            
            adlsDetailsJSON['source_file']= fileName
#             adlsDetailsJSON['rule_priority']= rc[-1]   # rule_priority added
            flagSourceFile = rc[15] 
          print(adlsDetailsJSON)
    
          print("This is srcGlbViewName"+rc[3])
          print("This is refDataGlbViewName"+rc[5])
          print("This is inputDataViewName"+self.inputDataViewName)
          
          print(rc[8])
          res = dbutils.notebook.run("./KC-DQ-ValidityCheckNB", 3600, {"RuleID":rc[1], "rule_label":rc[2],"srcGlbViewName":rc[3],"srcCol":rc[4],"refDataGlbViewName":rc[5],"refCol":rc[6], "saveErrorLog": saveErrorLog, "adlsDetails": json.dumps(adlsDetailsJSON), "dataFileGlbViewName": self.inputDataViewName, "flagInSource": flagSourceFile, "rulePriority": rc[16], "ruleThreshold":rc[8],"env":env})  ##
  
          dqresult = json.loads(res)
          dqresult['Zone_Name'] = rc[7]
          dqresult['Rule_Threshold'] = rc[8]
          dqresult['Domain_Name'] = rc[9]
          dqresult['Source_System'] = rc[10]
          dqresult['Rule_action'] = rc[11]
          dqresult['File_Name'] = "ct-dna-shr-sellout" + _dataFileName # rc[11] #"globaldatalake" + rc[11]
          dqresult['Rule_Priority'] = rc[16]
          
          if(len(rc)<=13):   
            dqresult['Rule_Name'] = ''   
            dqresult['Rule_Desc'] = ''   
          else:
            dqresult['Rule_Name'] = rc[13]  
            dqresult['Rule_Desc'] = rc[14] 
            
          dqresults.append(dqresult)
        except Exception as e:
          print(e)
      elif rc[0] == "Completeness":
        print("Calling Completeness NB to process Rule: " + rc[1])
        try:                  
          saveErrorLog = 'true'  ##
          adlsDetailsJSON= {}
          adlsDetailsJSON['accountname']= accountname 
          adlsDetailsJSON['containername']= containername
          adlsDetailsJSON['accountsecret']= adlsSecret
          adlsDetailsJSON['filepath']= errorFilePath
          errorFileName= 'ERROR_'+ PipelineID + '_' + rc[1] + '_' + fileName.split('.')[0]
          adlsDetailsJSON['filename']= errorFileName
          if(len(rc)<=13):
            adlsDetailsJSON['rule_desc']= ''
            flagSourceFile = 'false'
          else:
            adlsDetailsJSON['rule_desc']= rc[14] 
            
            adlsDetailsJSON['source_file']= fileName
            flagSourceFile = rc[15] 
#             adlsDetailsJSON['rule_priority']= rc[-1]   # rule_priority added
          print(adlsDetailsJSON)
                    
          res = self.run_with_retry("./KC-DQ-CompletenessCheckNB", 1200, {"RuleID":rc[1], "rule_label":rc[2],"srcGlbViewName":rc[3],"srcCol":rc[4],"refDataGlbViewName":rc[5],"refCol":rc[6], "saveErrorLog": saveErrorLog, "adlsDetails": json.dumps(adlsDetailsJSON), "dataFileGlbViewName": self.inputDataViewName, "flagInSource": flagSourceFile, "rulePriority": rc[16],"env":env}) ##
          dqresult = json.loads(res)
          dqresult['Zone_Name'] = rc[7]
          dqresult['Rule_Threshold'] = rc[8]
          dqresult['Domain_Name'] = rc[9]
          dqresult['Source_System'] = rc[10]
          dqresult['Rule_action'] = rc[11]
          dqresult['File_Name'] = "ct-dna-shr-sellout" + _dataFileName # rc[11] #"globaldatalake" + rc[11]
          dqresult['Rule_Priority'] = rc[16]
          
          if(len(rc)<=13):   
            dqresult['Rule_Name'] = ''   
            dqresult['Rule_Desc'] = ''   
          else:
            dqresult['Rule_Name'] = rc[13]  
            dqresult['Rule_Desc'] = rc[14] 
          
          dqresults.append(dqresult)
        except Exception as e:
          print(e)
      elif rc[0] == "Integrity":
        print("Calling Integrity NB to process Rule: " + rc[1])
        res = self.run_with_retry("./KC-DQ-IntegrityCheckNB", 60, {"RuleID":rc[1], "dataFileName":_dataFileName}, max_retries = 2)
      elif rc[0] == "Accuracy":
        print("Calling Accuracy NB to process Rule: " + rc[1])
        res = self.run_with_retry("./KC-DQ-AccuracyCheckNB", 60, {"RuleID":rc[1], "dataFileName":_dataFileName}, max_retries = 2)
      elif rc[0] == "Uniqueness":
        print("Calling Uniqueness NB to process Rule: " + rc[1])
#         res = self.run_with_retry("./KC-DQ-UniquenessCheckNB", 60, {"RuleID":rc[1], "dataFileName":_dataFileName}, max_retries = 2)
        try:                  
          saveErrorLog = 'true'  ##
          adlsDetailsJSON= {}
          adlsDetailsJSON['accountname']= accountname 
          adlsDetailsJSON['containername']= containername
          adlsDetailsJSON['accountsecret']= adlsSecret
          adlsDetailsJSON['filepath']= errorFilePath
          errorFileName= 'ERROR_'+ PipelineID + '_' + rc[1] + '_' + fileName.split('.')[0]
          adlsDetailsJSON['filename']= errorFileName
          if(len(rc)<=13):
            adlsDetailsJSON['rule_desc']= ''
            flagSourceFile = 'false'
          else:
            adlsDetailsJSON['rule_desc']= rc[14] 
            
            adlsDetailsJSON['source_file']= fileName
            flagSourceFile = rc[15] 
#             adlsDetailsJSON['rule_priority']= rc[-1]   # rule_priority added
          print(adlsDetailsJSON)
                    
          res = self.run_with_retry("./KC-DQ-UniquenessCheckNB", 1200, {"RuleID":rc[1], "rule_label":rc[2],"srcGlbViewName":rc[3],"srcCol":rc[4],"refDataGlbViewName":rc[5],"refCol":rc[6], "saveErrorLog": saveErrorLog, "adlsDetails": json.dumps(adlsDetailsJSON), "dataFileGlbViewName": self.inputDataViewName, "flagInSource": flagSourceFile, "rulePriority": rc[16],"env":env}) ##
          dqresult = json.loads(res)
          dqresult['Zone_Name'] = rc[7]
          dqresult['Rule_Threshold'] = rc[8]
          dqresult['Domain_Name'] = rc[9]
          dqresult['Source_System'] = rc[10]
          dqresult['Rule_action'] = rc[11]
          dqresult['File_Name'] = "ct-dna-shr-sellout" + _dataFileName # rc[11] #"globaldatalake" + rc[11]
          dqresult['Rule_Priority'] = rc[16]
          
          if(len(rc)<=13):   
            dqresult['Rule_Name'] = ''   
            dqresult['Rule_Desc'] = ''   
          else:
            dqresult['Rule_Name'] = rc[13]  
            dqresult['Rule_Desc'] = rc[14] 
          
          dqresults.append(dqresult)
        except Exception as e:
          print(e)
      elif rc[0] == "Timeliness":
        print("Calling Timeliness NB to process Rule: " + rc[1])
        res = self.run_with_retry("./KC-DQ-TimelinessCheckNB", 60, {"RuleID":rc[1], "dataFileName":_dataFileName}, max_retries = 2)
      else:
        print("Unknown Rule Category")

    print('Saving Source File with Rule Flag in Source Folder')
    #Save Source data file with Rule flag
    dataFileWithRuleFlagDf= spark.sql("select * from global_temp." + self.inputDataViewName)
    
    # called Cleaning on flagged source file
    dataFileWithRuleFlagDf = cleaning_flagged_src_file(dataFileWithRuleFlagDf)
    
    srcFileNameWFlag = 'DQ_SRC_'+ fileName.split('.')[0]
    # print("TESTING")
    self.saveLogFileInADLS(dataFileWithRuleFlagDf, accountname, containername, adlsSecret, refineFolder, srcFileNameWFlag,fileFormat)

#This code block belongs to consolidated DQ Error Failure Notification
    outputPath = _dataFileName.split('/Data')[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/error_logs"
    print(outputPath)

    try:
      output_path_mount = dbutils.fs.ls('abfss://' + containername + '@' + accountname + '.dfs.core.windows.net/' + outputPath)
      summary_error_log = merge_error_log(containername, accountname, PipelineID, outputPath)
      merged_file = 'InvalidRecords_' + PipelineID + "_" + fileName.split('.')[0] + "_" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2)
      self.saveLogFileInADLS(summary_error_log, accountname, containername, adlsSecret, outputPath, merged_file,fileFormat)

      ruleListColumns = ["CATEGORY", "DQ_RULE_ID", "DQ_RULE_LABEL", "SRC_VIEW_NAME", "SOURCE_COL", "REF_VIEW_NAME", "REF_DATA", "RULE_ZONE", "THRESHOLD", "DOMAIN", "SOURCE_SYSTEM", "RULE_ACTION", "SOURCE_FILE_PATH", "RULE_NAME", "DQ_RULE_DESC", "RULE_ACTIVE_FLAG", "RULE_PRIORITY"]
      ruleListDF = spark.createDataFrame(ruleList, schema = ruleListColumns)
      ruleListCategory = ruleListDF.select("CATEGORY", "DQ_RULE_ID", "RULE_PRIORITY")

      groupedData = summary_error_log.groupBy("DQ_RULE_ID").agg({'DQ_RULE_DESC':'max','DQ_RULE_ID':'count'}).withColumnRenamed("max(DQ_RULE_DESC)", "DQ Error Description").withColumnRenamed("count(DQ_RULE_ID)", "# Records Failed")
      groupedData = groupedData.withColumn("DQ Error Description", f.regexp_replace(groupedData["DQ Error Description"], "Field:", "Column:"))
      tempdf = groupedData.join(ruleListCategory,['DQ_RULE_ID'],"inner")
      tempdf = tempdf.withColumnRenamed("CATEGORY", "DQ Dimension")
      tempdf = tempdf.select('DQ Error Description', '# Records Failed', 'DQ Dimension', 'RULE_PRIORITY').withColumnRenamed("RULE_PRIORITY", "Rule Priority")
      tempdf = tempdf.withColumn('Rule Priority', when(tempdf['Rule Priority'] == '1', f.regexp_replace(tempdf['Rule Priority'], '1', 'Error')).when(tempdf['Rule Priority'] == '2', f.regexp_replace(tempdf['Rule Priority'], '2', 'Warning')))
      file_name_summary = 'ERROR_LOG_SUMMARY_' + PipelineID + "_" + fileName.split('.')[0] + "_" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) 
      self.saveLogFileInADLS(tempdf, accountname, containername, adlsSecret, outputPath, file_name_summary,fileFormat)
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        # print(e)
        print('Inside execute_rulesby_ruleID_and_dataFileName The path does not exist')  
        
    return(mydictionary["dqresults"]) 

# COMMAND ----------

def merge_error_log(containerName, accountName, pipelineID, filePath):
  outputTempPath = 'abfss://' + containerName + '@' + accountName + '.dfs.core.windows.net/' + filePath
  raw_file_list = dbutils.fs.ls(outputTempPath)
  print(raw_file_list)
  paths = []
  for i in range(len(raw_file_list)):
    paths.append(raw_file_list[i].path)
  file_list = []
  for i in range(len(paths)):
    path = [paths[i].split('error_logs/')[1]]
    for j in path:
      if(j.startswith('ERROR') and j.split('_')[1] == pipelineID):
        file_list.append(paths[i])
  print(file_list)
  df_data = spark.read.csv(file_list, header = 'true')
  return(df_data)

# COMMAND ----------

# dataFileName="/externalsources/KCNA/IRI/Costco_Category_Totals/Raw/Data/2022/08/25/Costco Category Totals.csv"

# COMMAND ----------

