# Databricks notebook source
# MAGIC %md
# MAGIC # KC Data Quality Framework - Consistency Checker
# MAGIC ## Overview
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Notebook Name | Consistency Check Notebook |
# MAGIC |Summary | DQ Runner notebook orchastrates the quality dimesions notebooks based on input parameters  |
# MAGIC |Created By | Jay Akhawri (jay.akhawri@kcc.com) |
# MAGIC |Input Parameters |<ul><li>RuleID</li><li>File name with path</li></ul>|
# MAGIC |Input Data Source |Azure Data Lake Gen 2 |
# MAGIC
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |Feb 9 2021 | Jay Akhawri | Revising notebook to add comments |
# MAGIC |Feb 28 2021 | Jay Akhawri | Refactored to Python class for unit testability |

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
              "base_directory": "/dbfs/FileStore/expectations_kroger/",  # TODO: replace with the path to your Expectations Store on DBFS
          },
      },
      "validations_store": {
          "class_name": "ValidationsStore",
          "store_backend": {
              "class_name": "TupleFilesystemStoreBackend",
              "base_directory": "/dbfs/FileStore/validations_kroger/",  # TODO: replace with the path to your Validations Store on DBFS
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
              "base_directory": "/dbfs/FileStore/docs_kroger/",  # TODO: replace with the path to your DataDocs Store on DBFS
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

# COMMAND ----------

import json
from datetime import datetime
import uuid
from re import search
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
from pyspark.sql.functions import col,lit, when

class ConsistencyChecker:
  
  def saveLogFileInADLS(self, df, accountName, containerName, accountSecret, filePath, dataFileName):  ##
    #spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountName), accountSecret)
    accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
    directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
    url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

    spark.conf.set("fs.azure.account.auth.type.accountName.dfs.core.windows.net", "OAuth")
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
  
  def execute_rule_by_rule_label(self, RuleID, rule_label,srcGlbViewName,source_col,saveLog, adlsDetails, dataViewName, flagInSource, rulePriority, refDataGlbViewName=None,refCol=None):
    try:
#################   Rule - expect_keyvalues_set_present_in_column_values    ########################
      if rule_label.lower() == "expect_keyvalues_set_present_in_column_values":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        Start_Datetime = str(datetime.now())
        
        if refDataGlbViewName.lower() in sqlContext.tableNames("global_temp"):
          # 1. Get src Glb View Name and Create context Dataframe
          dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
          dataDfBatch = context.get_batch({"dataset": dataDf,"datasource":"my_spark_datasource",},"my_new_suite")
          
          # 2. Get Ref Glb View Name and create a RefDataList (RefDataList should have one column named refCol)
          refDataDf = spark.sql("SELECT " + refCol + " FROM global_temp." + refDataGlbViewName)
          #print("refData frame")
          #display(refDataDf)
                 
          refDataList = [(row[refCol]) for row in refDataDf.collect()]
          #print(refDataList)
          
          # 4. Run rule 
          res = dataDfBatch.expect_column_values_to_be_in_set(source_col,refDataList,result_format={'result_format': 'COMPLETE'})
          
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
          sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
          unexp_list= jsonResult['result']['unexpected_list']  
          if (len(unexp_list)>0):   ## 
            if(saveLog=='true'):            
              print('Saving Error Log for Rule ID {0}'.format(RuleID))
              adlsJson= json.loads(adlsDetails)
              refCols = [source_col]
              unexpList= [[x] for x in unexp_list]
              unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
  
              unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).   \
                                          withColumn('DQ_SOURCE_COLUMN',lit(source_col)). withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).  \
                                          withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))    
                
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
              
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent),"Error_File_Path":errorFilePath})) 
          
        else:
          dbutils.notebook.exit("Reference Data is empty")
        
#################   Rule - expect_column_values_to_be_in_set  ########################
      elif rule_label.lower() == "expect_column_values_to_be_in_set":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
        
        for x in sqlContext.tableNames("global_temp"):
          print(x)
        
        if refDataGlbViewName.lower() in sqlContext.tableNames("global_temp"):
          # 1. Get src Glb View Name and Create context Dataframe
          if search(",",source_col):
            dataDf = spark.sql("select " + source_col + " as new_column, * from global_temp." + srcGlbViewName)
          else :
            dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
          dataDfBatch = context.get_batch({"dataset": dataDf,"datasource":"my_spark_datasource",},"my_new_suite")
          
          schemaError = False        
          pandasdf = dataDf.limit(2).toPandas()
          pdfcols = pandasdf.columns
          dataFileSet = set(pdfcols)
          
          if search(",",source_col):
            sourceDataDf= spark.sql("select " + source_col + " as new_column,* from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
          else :
            sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view  
          adlsJson= json.loads(adlsDetails)  
          
          if search(",",source_col):
            source_col = 'new_column'

          if source_col in dataFileSet:
            print("Schema Validation - Passed")
            refDataDf = spark.sql("SELECT * FROM global_temp." + refDataGlbViewName)

            refDataList = [row[source_col] for row in refDataDf.collect()]
            
            #  Run rule 
            res = dataDfBatch.expect_column_values_to_be_in_set(source_col,refDataList,result_format={'result_format': 'COMPLETE'})  
          
            jsonResult = res.to_json_dict()
            #print(jsonResult)
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
            if (len(unexp_list)>0):   
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
                    sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus,source_col,'left')  #Left Join on source column
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
            
        else:
          dbutils.notebook.exit("Reference Data is empty")
#################   Rule - expect_column_values_to_be_in_set_trim  ########################
      elif rule_label.lower() == "expect_column_values_to_be_in_set_trim":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
        
        for x in sqlContext.tableNames("global_temp"):
          print(x)
        
        if refDataGlbViewName.lower() in sqlContext.tableNames("global_temp"):
          # 1. Get src Glb View Name and Create context Dataframe
          dataDf = spark.sql("select LTRIM(" + source_col + ",'0') as new_column, * from global_temp." + srcGlbViewName)
          dataDfBatch = context.get_batch({"dataset": dataDf,"datasource":"my_spark_datasource",},"my_new_suite")
          
          schemaError = False        
          pandasdf = dataDf.limit(2).toPandas()
          pdfcols = pandasdf.columns
          dataFileSet = set(pdfcols)
          sourceDataDf= spark.sql("select LTRIM(" + source_col + ",'0') as new_column,* from global_temp." + dataViewName) #Get data in spark dataframe from common input 
          adlsJson= json.loads(adlsDetails)  
          
          source_col = 'new_column'
          #refCol = 'new_column'

          if source_col in dataFileSet:
            print("Schema Validation - Passed")
            refDataDf = spark.sql("SELECT LTRIM(" + refCol + ",'0') as new_column,* FROM global_temp." + refDataGlbViewName)

            refDataList = [row[source_col] for row in refDataDf.collect()]
            
            #  Run rule 
            res = dataDfBatch.expect_column_values_to_be_in_set(source_col,refDataList,result_format={'result_format': 'COMPLETE'})  
          
            jsonResult = res.to_json_dict()
            #print(jsonResult)
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
            if (len(unexp_list)>0):   
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
                    sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus,source_col,'left')  #Left Join on source column
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
            
        else:
          dbutils.notebook.exit("Reference Data is empty")
          
#################   Rule - expect_column_values_to_be_in_set_and_not_null  ########################
      elif rule_label.lower() == "expect_column_values_to_be_in_set_and_not_null":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
        
        if refDataGlbViewName.lower() in sqlContext.tableNames("global_temp"):
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
            refDataDf = spark.sql("SELECT " + refCol + " FROM global_temp." + refDataGlbViewName)
            if search("`", refCol): 
              refDataList = [str(row[refCol.strip("`")]) for row in refDataDf.collect()]
            else:
              refDataList = [row[refCol] for row in refDataDf.collect()]
          
            # 4. Run rule 
            res = dataDfBatch.expect_column_values_to_be_in_set(source_col,refDataList,result_format={'result_format': 'COMPLETE'})  
            
            jsonResult = res.to_json_dict()
            failed_count=int(jsonResult['result']['unexpected_count'])+ int(jsonResult['result']['missing_count']) #missing_count contains number of records with null values
            totalRecords = jsonResult['result']['element_count']
            unexpected_percent = (failed_count/int(totalRecords))*100         
            Records_Failed = str(failed_count)
            Records_Success = int(totalRecords) - int(Records_Failed)
            print(jsonResult)
          
            Record_Key = str(uuid.uuid4())
            Record_Field_Name = source_col
            notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
            End_Datetime = str(datetime.now())
            
            # -------Save Error Log -------#
            errorFilePath = ''          
            unexp_list= jsonResult['result']['unexpected_list']  
            if(failed_count>0):   ## 
              if(saveLog=='true'):
                if(len(unexp_list)>0):
                  print('Saving Error Log for Rule ID {0}'.format(RuleID))                
                  refCols = [source_col]
                  unexpList= [[x] for x in unexp_list]
                  unexpecteddF = spark.createDataFrame(data=unexpList, schema = refCols)
  
                  unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
                  #Join the two dataframes on source column
                  errorRecordsDfNotNull= dataDf.join(unexpectedDisctinctDf,source_col).withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                    withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                    withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))
                  errorRecordsDfNull= dataDf.where(col(source_col).isNull()).   \
                                    withColumn('DQ_RULE_ID',lit(str(RuleID))).withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).   \
                                    withColumn('DQ_SOURCE_COLUMN',lit(source_col)). withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).  \
                                    withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime)) 
                  errorRecordsDf = errorRecordsDfNotNull.unionByName(errorRecordsDfNull)  #Test this
                else:  #For null values
                  errorRecordsDf= dataDf.where(col(source_col).isNull()).withColumn('DQ_RULE_ID',lit(str(RuleID))).withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).   \
                                          withColumn('DQ_SOURCE_COLUMN',lit(source_col)). withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).  \
                                          withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))                                      
                
                errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
                # ------- Flag rule result in the input data view    -------- # 
                if(flagInSource == 'true'):
                  if(rulePriority == "1"):
                    sourceDataWRuleDetail= sourceDataDf.withColumn('DQ_'+ str(RuleID), when(col(source_col).isin(unexp_list)==True,'Failed with Error').when(col(source_col).isNull()==True,'Failed with Error').otherwise('')).withColumn('DQ_'+ str(RuleID) + '_DESC',when(col(source_col).isin(unexp_list)==True,lit(adlsJson['rule_desc'])).when(col(source_col).isNull()==True,lit(adlsJson['rule_desc'])).otherwise(''))
                    sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                    print('Rule ID column with flag appended to input data view')
                  else:
                    sourceDataWRuleDetail= sourceDataDf.withColumn('DQ_'+ str(RuleID), when(col(source_col).isin(unexp_list)==True,'Failed with Warning').when(col(source_col).isNull()==True,'Failed with Warning').otherwise('')).withColumn('DQ_'+ str(RuleID) + '_DESC',when(col(source_col).isin(unexp_list)==True,lit(adlsJson['rule_desc'])).when(col(source_col).isNull()==True,lit(adlsJson['rule_desc'])).otherwise(''))
                    sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                    print('Rule ID column with flag appended to input data view')          
            elif(flagInSource == 'true'):  
              sourceDataWRuleDetail = sourceDataDf.withColumn('DQ_'+ str(RuleID),lit('Blank')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit('Blank'))
              sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
              print('Rule ID Column appended to input data view')
              
            # Exit Notebook
            dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent)}))
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
            dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(unexpected_percent)}))
            
        else:
          dbutils.notebook.exit("Reference Data is empty")

#################   Rule - expect_column_values_to_be_in_set_pd  ########################
      elif rule_label.lower() == "expect_column_values_to_be_in_set_pd":
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
          
          if source_col in dataFileSet:
            print("Schema Validation - Passed")
            refDataDf = spark.sql("SELECT " + refCol + " FROM global_temp." + refDataGlbViewName)
            if search("`", refCol): 
              refDataList = [str(row[refCol.strip("`")]) for row in refDataDf.collect()]
            else:
              refDataList = [row[refCol] for row in refDataDf.collect()]
          
            #  Run rule 
            res = dataDfBatch.expect_column_values_to_be_in_set(source_col,refDataList,result_format={'result_format': 'COMPLETE'})   ##
          
            jsonResult = res.to_json_dict()
            print(jsonResult)
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
                    
#################   Rule - expect_table_columns_to_match_exactly_set  ########################
      elif rule_label.lower() == "expect_table_columns_to_match_exactly_set":
        print("Processing " + RuleID + " for rule type - " + rule_label)
        
        Start_Datetime = str(datetime.now())
        print(source_col)
        
        dataDf = spark.sql("select * from global_temp." + srcGlbViewName)
        total_srcCount = dataDf.count()
        dataDfBatch = context.get_batch({"dataset": dataDf,"datasource":"my_spark_datasource",},"my_new_suite")      

        dataDfBatch.get_column_count()

        schemaError = False        
        pandasdf = dataDf.limit(2).toPandas()
        pdfcols = pandasdf.columns
        dataFileSet = set(pdfcols)
        print(dataFileSet)
        
        source_col_n = source_col.split(",")
        source_col_n = [i.replace("'","").strip(" ") for i in source_col_n]
        
#        schemaError = False        
#        #pandasdf = dataDf.limit(2).toPandas()
#        pdfcols = pandasdf.columns
#        dataFileSet = set(pdfcols)
#        print(dataFileSet)
#        print(source_col)
        
        sourceDataDf= spark.sql("select * from global_temp." + dataViewName) #Get data in spark dataframe from common input data view
        adlsJson= json.loads(adlsDetails)
      
        columns_ref = refCol.split(',')
        print(columns_ref)
        print(dataDfBatch)
        
        if (all(x in dataFileSet for x in source_col_n)):
          print("Schema Validation - Passed")
          
          Records_Failed = 0
          unexpected_percent = 0.0

          totalRecords = int(total_srcCount)
          Records_Success = total_srcCount - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())

          # Exit Notebook
          errorFilePath = ''
          dbutils.notebook.exit(json.dumps({"Batch_ID": str(Batch_ID), "RuleID": str(RuleID), "Unexpected percentage": str(unexpected_percent),"Record_Key": str(Record_Key),"Record_Field_Name": str(Record_Field_Name), "Records_Failed": str(Records_Failed),"Records_Success": str(Records_Success), "Start_Datetime": Start_Datetime,"End_Datetime": End_Datetime,"Expected_percentage":str(100-unexpected_percent), "Error_File_Path":errorFilePath})) ##
        else:
          print("Schema Validation - Failed")
          Records_Failed = int(total_srcCount)
          unexpected_percent = 100.0
          totalRecords = int(total_srcCount)
          Records_Success = total_srcCount - int(Records_Failed)
        
          Record_Key = str(uuid.uuid4())
          Record_Field_Name = source_col
          notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
          Batch_ID = notebook_info["tags"]["jobId"] if notebook_info["tags"]["jobId"] != None else ''
          End_Datetime = str(datetime.now())#
          
          # -------Save Error Log -------#
          errorFilePath = ''          
          #unexp_list= jsonResult['result']['unexpected_list']  
          if (unexpected_percent == 100.0):   ##            
            if(saveLog=='true'):
              print('Saving Error Log for Rule ID {0}'.format(RuleID))
              #unexpList= [[x] for x in unexp_list]
              unexpecteddF = dataDf
              source_col_list = list(dataDf.columns)
                            
              #unexpectedDisctinctDf= unexpecteddF.distinct()  #To return distinct values
              #Join the two dataframes on source column
              errorRecordsDf= unexpecteddF.withColumn('DQ_RULE_ID',lit(str(RuleID))).   \
                                withColumn('DQ_RULE_DESC',lit(adlsJson['rule_desc'])).withColumn('DQ_SOURCE_COLUMN',lit(source_col)).   \
                                withColumn('DQ_SOURCE_FILE',lit(adlsJson['source_file'])).withColumn('DQ_RULE_EXC_TIME',lit(End_Datetime))  
              display(errorRecordsDf)
              errorFilePath = self.saveLogFileInADLS(errorRecordsDf, adlsJson['accountname'], adlsJson['containername'], adlsJson['accountsecret'], adlsJson['filepath'], adlsJson['filename'])
              
              # ------- Flag rule result in the input data view    -------- # 
              if(flagInSource == 'true'):
                if(rulePriority == "1"):
                  unexpectedDisctinctDfWRuleStatus= unexpecteddF.withColumn('DQ_'+ str(RuleID),lit('Failed with Error')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed with error'
                  sourceDataWRuleDetail= sourceDataDf.join(unexpectedDisctinctDfWRuleStatus, source_col_list,'left')  #Left Join on source column
                  sourceDataWRuleDetail.createOrReplaceGlobalTempView(dataViewName)
                  print('Rule ID column with flag appended to input data view')
                else:
                  unexpectedDisctinctDfWRuleStatus= unexpecteddF.withColumn('DQ_'+ str(RuleID),lit('Failed with Warning')).withColumn('DQ_'+ str(RuleID) + '_DESC',lit(adlsJson['rule_desc']))  # Flag error records for the rule as 'Failed with Warning'
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
        print("Rule Label doesn't exist")
        dbutils.notebook.exit("Rule Label doesn't exist")
   
    except Exception as ex:
      dbutils.notebook.exit(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process rules by rule label

# COMMAND ----------

cc = ConsistencyChecker()
cc.execute_rule_by_rule_label(RuleID,rule_label,srcGlbViewName,srcCol,saveErrorLog, adlsDetails, dataFileGlbViewName,flagInSource,rulePriority,refDataGlbViewName,refCol)

# COMMAND ----------
