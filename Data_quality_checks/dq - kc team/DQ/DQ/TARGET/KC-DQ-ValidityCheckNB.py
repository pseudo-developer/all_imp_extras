# Databricks notebook source
# MAGIC %md
# MAGIC # KC Data Quality Framework - Validity Checker
# MAGIC ## Overview
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Notebook Name | Validity Check Notebook |
# MAGIC |Summary | DQ Runner notebook orchastrates the quality dimesions notebooks based on input parameters  |
# MAGIC |Created By | Sudeep Kumar (sudeep.kumar@kcc.com) |
# MAGIC |Input Parameters |<ul><li>RuleID</li><li>File name with path</li></ul>|
# MAGIC |Input Data Source |Azure Data Lake Gen 2 |
# MAGIC
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |June 28 2022 | Sudeep Kumar | Initial Version (Moved Validity checks from consistency notebook) |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load GE

# COMMAND ----------

#pip install great-expectations

# COMMAND ----------

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext
# from sqlalchemy.dialects import registry

try:
#   registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
  
  ### Setup GE Data Context #####
  project_config = DataContextConfig(
      config_version=2,
      plugins_directory=None,
      config_variables_file_path=None,

      datasources={
          "my_spark_datasource": {
              "data_asset_type": {
                  "class_name": "SparkDFDataset",
                  "module_name": "great_expectations.dataset",
              },
              "class_name": "SparkDFDatasource",
              "module_name": "great_expectations.datasource",
              "batch_kwargs_generators": {},
          },
        "my_pandas_datasource": {
              "data_asset_type": {
                  "class_name": "PandasDataset",
                  "module_name": "great_expectations.dataset",
              },
              "class_name": "PandasDatasource",
              "module_name": "great_expectations.datasource",
              "batch_kwargs_generators": {},
          }
      },
      stores={
      "expectations_store": {
          "class_name": "ExpectationsStore",
          "store_backend": {
              "class_name": "TupleFilesystemStoreBackend",
              "base_directory": "/dbfs/FileStore/expectations/",  # TODO: replace with the path to your Expectations Store on DBFS
          },
      },
      "validations_store": {
          "class_name": "ValidationsStore",
          "store_backend": {
              "class_name": "TupleFilesystemStoreBackend",
              "base_directory": "/dbfs/FileStore/validations/",  # TODO: replace with the path to your Validations Store on DBFS
          },
      },
      "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
   },
   expectations_store_name="expectations_store",
   validations_store_name="validations_store",
   evaluation_parameter_store_name="evaluation_parameter_store",
   data_docs_sites={
      "local_site": {
          "class_name": "SiteBuilder",
          "store_backend": {
              "class_name": "TupleFilesystemStoreBackend",
              "base_directory": "/dbfs/FileStore/docs/",  # TODO: replace with the path to your DataDocs Store on DBFS
          },
          "site_index_builder": {
              "class_name": "DefaultSiteIndexBuilder",
              "show_cta_footer": True,
          },
      }
   },
   validation_operators={
      "action_list_operator": {
          "class_name": "ActionListValidationOperator",
          "action_list": [
              {
                  "name": "store_validation_result",
                  "action": {"class_name": "StoreValidationResultAction"},
              },
              {
                  "name": "store_evaluation_params",
                  "action": {"class_name": "StoreEvaluationParametersAction"},
              },
              {
                  "name": "update_data_docs",
                  "action": {"class_name": "UpdateDataDocsAction"},
              },
          ],
      }
   },
   anonymous_usage_statistics={
    "enabled": True
   }
   )
except Exception as ex:
  logErrorMsg = ex
  print(logErrorMsg)
    
  if logErrorMsg != '' or len(logErrorMsg) > 0:
    raise Exception(logErrorMsg)

context = BaseDataContext(project_config=project_config)
context.create_expectation_suite("my_new_suite", overwrite_existing=True)
context.list_datasources()

# COMMAND ----------

dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./Common/CommonUtil"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('RuleID', '')
dbutils.widgets.text('rule_label', '')
dbutils.widgets.text('srcGlbViewName', '')
dbutils.widgets.text('srcCol', '')
dbutils.widgets.text('refDataGlbViewName', '')
dbutils.widgets.text('refCol', '')
dbutils.widgets.text('saveErrorLog', '') 
dbutils.widgets.text('adlsDetails', '')  
dbutils.widgets.text('dataFileGlbViewName', '')  
dbutils.widgets.text('flagInSource', '')  
dbutils.widgets.text('rulePriority', '')
dbutils.widgets.text('ruleThreshold', '')

RuleID = dbutils.widgets.get('RuleID')
rule_label = dbutils.widgets.get('rule_label')
srcGlbViewName = dbutils.widgets.get('srcGlbViewName')
srcCol = dbutils.widgets.get('srcCol')
refDataGlbViewName = dbutils.widgets.get('refDataGlbViewName')
refCol = dbutils.widgets.get('refCol')
saveErrorLog= dbutils.widgets.get('saveErrorLog')  
adlsDetails= dbutils.widgets.get('adlsDetails')  # Expects a JSON input
dataFileGlbViewName = dbutils.widgets.get('dataFileGlbViewName')  
flagInSource = dbutils.widgets.get('flagInSource')
rulePriority = dbutils.widgets.get('rulePriority')
ruleThreshold = dbutils.widgets.get('ruleThreshold')

# COMMAND ----------

import json
from datetime import datetime
import uuid
from re import search
from pyspark.sql.functions import col,lit,count, when
from pyspark.sql.types import StringType

class ValidityChecker:
  
  def saveLogFileInADLS(self, df, accountName, containerName, accountSecret, filePath, dataFileName):  ##
    #spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountName), accountSecret)
    accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
    directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
    url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

    spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
    spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)
    outputTempPath = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + filePath + "/temp_"+ dataFileName
    print(outputTempPath)
    df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(outputTempPath)
    files = dbutils.fs.ls(outputTempPath + "/")
    ops_file = [file.path for file in files if file.path.endswith(".csv")][0]
    errorFileName= dataFileName + ".csv"
    errorFilePath = filePath + "/" + errorFileName
    opTargetPathADLS = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + errorFilePath 
    # Move the csv file to desired location
    dbutils.fs.mv(ops_file, opTargetPathADLS)
    # Remove all the extra files created by databricks
    dbutils.fs.rm(outputTempPath, recurse = True)
    return errorFileName
  
  def execute_rule_by_rule_label(self, RuleID, rule_label,srcGlbViewName,source_col,saveLog, adlsDetails, dataViewName, flagInSource, rulePriority, ruleThreshold, refDataGlbViewName=None,refCol=None):
    try:
 #################   Rule - expect_specfic_string_in_start_and_end  ########################
      if rule_label.lower() == "expect_specfic_string_in_start_and_end":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "^https://promocreatives.numerator.com/adcompare/"
        regex_2 = ".jpg$"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
          res_1 = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_2,mostly=1,result_format={'result_format': 'COMPLETE'})
          
          # result of Not matched records
          jsonResult_1 = res_1.to_json_dict()
          unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
          if (unexpected_percent_1==None):
            unexpected_percent_1 = 0
          Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
          
          print("Records_Failed : ")
          print(Records_Failed)
          print("Records_Success")
          print(Records_Success)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']
          unexp_list_1= jsonResult_1['result']['unexpected_list'] 
          unexp_list = unexp_list + unexp_list_1
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
            
#################   Rule - expect_column_values_to_match_strftime_format  ########################
      if rule_label.lower() == "expect_column_values_to_match_strftime_format":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        dataDfBatch = context.get_batch({"dataset": dataDf,"datasource":"my_spark_datasource",},"my_new_suite")
        
        schemaError = False        
        pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")          
          strftimeFormat= refDataGlbViewName #Assuming refDataGlbViewName will contain the date format in strftime format. ex- %d.%m.%Y, %m/%d/%Y, %Y%m%d 
          #Run rule 
          res = dataDfBatch.expect_column_values_to_match_strftime_format(source_col,strftimeFormat,result_format={'result_format': 'COMPLETE'})  
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']  
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))              
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))   
                
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')           
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')
            
          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent),"Error_File_Path":errorFilePath}))          
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')
          
          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent),"Error_File_Path":''}))  
  
  #################   Rule - expect_column_values_to_be_number_only  ########################
      elif rule_label.lower() == "expect_column_values_to_be_number_only":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "[\d]+"
        regex_2 = "[\D]+"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
          res_1 = dataDfBatch.expect_column_values_to_not_match_regex(source_col,regex_2,mostly=1,result_format={'result_format': 'COMPLETE'})
          
          # result of Not matched records
          jsonResult_1 = res_1.to_json_dict()
          unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
          if (unexpected_percent_1==None):
            unexpected_percent_1 = 0
          Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']
          unexp_list_1= jsonResult_1['result']['unexpected_list'] 
          unexp_list = unexp_list + unexp_list_1
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
          
    #################   Rule - expect_column_values_to_be_number_with_negativesign  ########################
      elif rule_label.lower() == "expect_column_values_to_be_number_with_negativesign":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "[\d]+"
        regex_2 = "^[-]{1}"
        regex_3 = "[\D]+"
        reg_list = [regex_1,regex_2]
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex_list(source_col,reg_list,match_on='all',result_format={'result_format': 'COMPLETE'})
          res_1 = dataDfBatch.expect_column_values_to_not_match_regex(source_col,regex_3,mostly=1,result_format={'result_format': 'COMPLETE'})
        
          # result of Not matched records
          jsonResult_1 = res_1.to_json_dict()
          unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
          if (unexpected_percent_1==None):
            unexpected_percent_1 = 0
          Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']
          unexp_list_1= jsonResult_1['result']['unexpected_list'] 
          unexp_list = unexp_list + unexp_list_1
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')                 
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
          
        else:
            print("Schema Validation - Failed")
            unexpected_percent = -1
            totalRecords = 0
            Records_Failed = 0
            Records_Success = 0

            Record_Key = str(uuid.uuid4())
            Record_Field_Name = source_col
            notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
            End_Datetime = str(datetime.now())

            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

            # Exit Notebook
            dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))

#################   Rule - expect_the_first_few_chars_in_column_to_be_number_only  ########################
      elif rule_label.lower() == "expect_the_first_few_chars_in_column_to_be_number_only":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "\A[\d]{7,7}"
       # regex_2="[,]*" [ \w]*
       # reg_list= [regex_1,regex_2]
        regex_2="\A[\d]{8,}"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
          res_1 = dataDfBatch.expect_column_values_to_not_match_regex(source_col,regex_2,mostly=1,result_format={'result_format': 'COMPLETE'})
        
          # result of Not matched records
          jsonResult_1 = res_1.to_json_dict()
          unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
          if (unexpected_percent_1==None):
            unexpected_percent_1 = 0
          Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']
          unexp_list_1= jsonResult_1['result']['unexpected_list'] 
          unexp_list = unexp_list + unexp_list_1
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
          
#################   Rule - expect_percentage_sign_at_the_end  ########################
      elif rule_label.lower() == "expect_percentage_sign_at_the_end":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "[0-9]+%$"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
  
#           # result of Not matched records
#           jsonResult_1 = res_1.to_json_dict()
#           unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
#           if (unexpected_percent_1==None):
#             unexpected_percent_1 = 0
#           Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
#           unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
#           Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']  
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
          
#################   Rule - expect_leading_zeros_at_the_beginning  ########################
      elif rule_label.lower() == "expect_leading_zeros_at_the_beginning":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "^[0]*[\d]+$"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          #  Run rule 
          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
  
#           # result of Not matched records
#           jsonResult_1 = res_1.to_json_dict()
#           unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
#           if (unexpected_percent_1==None):
#             unexpected_percent_1 = 0
#           Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
#           unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
#           Records_Failed = int(Records_Failed_1) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']  
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
          
#################   Rule - expect_table_row_count_to_be_between  ########################
      elif rule_label.lower() == "expect_table_row_count_to_be_between":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
        
        if refDataGlbViewName.lower() in sqlContext.tableNames("global_temp"):
          # 1. Get src Glb View Name and Create context Dataframe
          dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
          pandasdf = dataDf.toPandas()
          dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
          
          schemaError = False        
          #pandasdf = dataDf.limit(2).toPandas()
          pdfcols = pandasdf.columns
          dataFileSet = set(pdfcols)
          
          sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
          adlsJson= json.loads(adlsDetails)
          
          refDataDf = spark.sql("select " + refCol + " from global_temp." + refDataGlbViewName)
          total_srcCount = sourceDataDf.count()
          total_refCount = refDataDf.count()
          print('total_refCount :',total_refCount)
          # Check -/+ 10%
          minValue = int(total_refCount) - int(total_refCount) * int(ruleThreshold)/100
          maxValue = int(total_refCount) + int(total_refCount) * int(ruleThreshold)/100
          
          if source_col in dataFileSet:
            print("Schema Validation - Passed")
  
            #  Run rule 
            res = dataDfBatch.expect_table_row_count_to_be_between(min_value=int(minValue), \
                                                                 max_value=int(maxValue), \
                                                                 result_format={'result_format': 'COMPLETE'})
    
  #           # result of Not matched records
  #           jsonResult_1 = res_1.to_json_dict()
  #           unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
  #           if (unexpected_percent_1==None):
  #             unexpected_percent_1 = 0
  #           Records_Failed_1 = jsonResult_1['result']['unexpected_count']
            
            # Joining the result of matched with unmatched records
            jsonResult = res.to_json_dict()
            if (jsonResult['success'] == True):
              Records_Failed = 0
              unexpected_percent = 0.0
            else:
              Records_Failed = int(total_srcCount)
              unexpected_percent = float(100)
            totalRecords = int(total_srcCount)
            Records_Success = total_srcCount - int(Records_Failed)
  #          unexpected_percent = jsonResult['result']['unexpected_percent']
  #          if (unexpected_percent==None):
  #            unexpected_percent = 0
  #           unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
  #          totalRecords = jsonResult['result']['element_count']
  #          Records_Failed = jsonResult['result']['unexpected_count']
  #           Records_Failed = int(Records_Failed_1) + int(Records_Failed)
  #          Records_Success = int(totalRecords) - int(Records_Failed)
          
            Record_Key = str(uuid.uuid4())
            Record_Field_Name = source_col
            notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
            End_Datetime = str(datetime.now())
            
            # -------Save Error Log -------#
            errorFilePath = ''          
            #unexp_list= jsonResult['result']['unexpected_list']  
            if (jsonResult['success'] == False):   ##            
              if(saveLog=='true'):
                print('Saving Error Log for Rule ID {0}'.format(RuleID))               
                refCols = [source_col]
                print(source_col)
                #unexpList= [[x] for x in unexp_list]
                #unexpecteddF = spark.createDataFrame(data=dataDf, schema = refCols)
                unexpecteddF = dataDf
                source_col_list = list(dataDf.columns)
                
                unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
                #Join the two dataframes on source column
                errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col_list).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                  withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                  withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))
                display(errorRecordsDf)
                
                errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
                
                # ------- Flag rule result in the input data view    -------- # 
                if(flagInSource == 'true'):
                  if(rulePriority == "1"):
                    unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                    sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col_list,'left')  #Left Join on source column
                    sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                    print('Rule ID column with flag appended to input data view')
                  else:
                    unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                    sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col_list,'left')  #Left Join on source column
                    sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                    print('Rule ID column with flag appended to input data view')
            elif(flagInSource == 'true'):  
              sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
              sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
              print('Rule ID Column appended to input data view')
           # Exit Notebook
            dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
          else:
            print("Schema Validation - Failed")
            unexpected_percent = -1
            totalRecords = 0
            Records_Failed = 0
            Records_Success = 0
          
            Record_Key = str(uuid.uuid4())
            Record_Field_Name = source_col
            notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
            End_Datetime = str(datetime.now())
            
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')
  
            # Exit Notebook
            dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##
      
        else:
          dbutils.notebook.exit("Reference Data is empty")

#################   Rule - expect_transaction_day_within_six_weeks  ########################
      elif rule_label.lower() == "expect_transaction_day_within_six_weeks":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        sourceDataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        DF = spark.sql("select " + source_col + " from global_temp." + srcGlbViewName)
        # Difference in day
        max_day = DF.agg({source_col: "max"}).collect()[0]
        min_day = DF.agg({source_col: "min"}).collect()[0]
        max_dt = str(max_day[0]).replace("1 day ending ","")
        min_dt = str(min_day[0]).replace("1 day ending ","")
        max = datetime.strptime(max_dt, "%m-%d-%Y")
        min = datetime.strptime(min_dt, "%m-%d-%Y")
        to_day = datetime.today().strftime("%m-%d-%Y")
        c_date = datetime.strptime(to_day, "%m-%d-%Y")
        delta1 = max - min
        print(delta1.days)
        delta2 = c_date - max
        print(delta2.days)
        list_of_TD = [t[0] for t in DF.collect()]
        TD = set(list_of_TD)
        dataDF = spark.createDataFrame(TD, StringType())
        dataDF = dataDF.withColumnRenamed("value","TRANSACTION_DAY")
        pandasdf = DF.toPandas()
        pandasdf_1 = dataDF.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf_1,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        total_srcCount = sourceDataDf.count()
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          #  Run rule 
          res = dataDfBatch.expect_table_row_count_to_be_between(42, 70, result_format={'result_format': 'COMPLETE'})
  
#           # result of Not matched records
#           jsonResult_1 = res_1.to_json_dict()
#           unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
#           if (unexpected_percent_1==None):
#             unexpected_percent_1 = 0
#           Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          if (jsonResult['success'] == True):
            Records_Failed = 0
            unexpected_percent = 0.0
          else:
            Records_Failed = int(total_srcCount)
            unexpected_percent = float(100)
          totalRecords = int(total_srcCount)
          Records_Success = total_srcCount - int(Records_Failed)
#          unexpected_percent = jsonResult['result']['unexpected_percent']
#          if (unexpected_percent==None):
#            unexpected_percent = 0
#           unexpected_percent = float(unexpected_percent)+float(unexpected_percent_1)
#          totalRecords = jsonResult['result']['element_count']
#          Records_Failed = jsonResult['result']['unexpected_count']
#           Records_Failed = int(Records_Failed_1) + int(Records_Failed)
#          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          #unexp_list= jsonResult['result']['unexpected_list']  
          if (jsonResult['success'] == False):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              #unexpList= [[x] for x in unexp_list]
              #unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              unexpecteddF = sourceDataDf
              source_col_list = list(sourceDataDf.columns)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= sourceDataDf.join(unexpectedDisctinctDf,source_col_list).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col_list,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col_list,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

         # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##

  #################   Rule - expect_column_values_to_be_number_or_blank  ########################
      elif rule_label.lower() == "expect_column_values_to_be_number_or_blank":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
             
        # 1. Get src Glb View Name and Create context Dataframe
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        pandasdf = dataDf.toPandas()
        dataDfBatch = context.get_batch({"dataset": pandasdf,"datasource":"my_pandas_datasource",},"my_new_suite")
        
        schemaError = False        
        #pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
        
        regex_1 = "^[-]?[\d]+?"
        #regex_2 = "[\D]+"
        regex_3 = "[\s]"
        #regex_4 = "^[-]{1}"
        
        if source_col in dataFileSet:
          print("Schema Validation - Passed")

          res = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_1,mostly=1,result_format={'result_format': 'COMPLETE'})
          #res_1 = dataDfBatch.expect_column_values_to_not_match_regex(source_col,regex_2,mostly=1,result_format={'result_format': 'COMPLETE'})
          res_2 = dataDfBatch.expect_column_values_to_not_match_regex(source_col,regex_3,mostly=1,result_format={'result_format': 'COMPLETE'})
          #res_3 = dataDfBatch.expect_column_values_to_match_regex(source_col,regex_4,mostly=1,result_format={'result_format': 'COMPLETE'})
          
          # result of Not matched records
          #jsonResult_1 = res_1.to_json_dict()
          #unexpected_percent_1 = jsonResult_1['result']['unexpected_percent']
          #if (unexpected_percent_1==None):
          #  unexpected_percent_1 = 0
          #Records_Failed_1 = jsonResult_1['result']['unexpected_count']
          
          jsonResult_2 = res_2.to_json_dict()
          unexpected_percent_2 = jsonResult_2['result']['unexpected_percent']
          if (unexpected_percent_2==None):
            unexpected_percent_2 = 0
          Records_Failed_2 = jsonResult_2['result']['unexpected_count']
          
          #jsonResult_3 = res_3.to_json_dict()
          #unexpected_percent_3 = jsonResult_3['result']['unexpected_percent']
          #if (unexpected_percent_3==None):
          #  unexpected_percent_3 = 0
          #Records_Failed_3 = jsonResult_3['result']['unexpected_count']
          
          # Joining the result of matched with unmatched records
          jsonResult = res.to_json_dict()
          unexpected_percent = jsonResult['result']['unexpected_percent']
          if (unexpected_percent==None):
            unexpected_percent = 0
          unexpected_percent = float(unexpected_percent)+float(unexpected_percent_2)
          totalRecords = jsonResult['result']['element_count']
          Records_Failed = jsonResult['result']['unexpected_count']
          Records_Failed = int(Records_Failed_2) + int(Records_Failed)
          Records_Success = int(totalRecords) - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          unexp_list= jsonResult['result']['unexpected_list']
          unexp_list_1= jsonResult_2['result']['unexpected_list'] 
          unexp_list = unexp_list + unexp_list_1
          if (len(unexp_list)>0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))               
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
              
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpectedDisctinctDf.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
          elif(flagInSource == 'true'):  
            sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
            sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
            print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          unexpected_percent = -1
          totalRecords = 0
          Records_Failed = 0
          Records_Success = 0
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())
          
          sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Failed')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))
          sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
          print('Rule ID Column appended to input data view')

          # Exit Notebook
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent), "Error_File_Path": ''}))  ##        
#        
      else:
        print("Rule Label doesn't exist")
        dbutils.notebook.exit("Rule Label doesn't exist")
   
    except Exception as ex:
      dbutils.notebook.exit(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process rules by rule label

# COMMAND ----------

cc = ValidityChecker()
cc.execute_rule_by_rule_label(RuleID,rule_label,srcGlbViewName,srcCol,saveErrorLog, adlsDetails, dataFileGlbViewName,flagInSource,rulePriority,ruleThreshold,refDataGlbViewName,refCol)