# Databricks notebook source
# MAGIC %md
# MAGIC # KC Data Quality Framework - Runner
# MAGIC ## Overview
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Notebook Name | Runner |
# MAGIC |Summary | DQ Runner notebook orchastrates the quality diemsions notebooks based on input parameters  |
# MAGIC |Created By | Jay Akhawri (jay.akhawri@kcc.com) |
# MAGIC |Input Parameters |<ul><li>PipelineID</li><li>File name with path</li></ul>|
# MAGIC |Output DQ Log Path |<ul><li> Base File Path (Until Zone e.g RAW)/**Logs**/DQ/YYYY/MM/DD</li></ul>|
# MAGIC |Input Data Source |Azure Data Lake Gen 2 |
# MAGIC |Output Data Source |Azure Data Lake Gen 2 |
# MAGIC
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |Feb 9 2021 | Jay Akhawri | Revising notebook to add comments |
# MAGIC |Feb 12 2021 | Jay Akhawri | Added Nielsen Adapter |
# MAGIC |Feb 12 2021 | Jay Akhawri | Added DQ Logger  |
# MAGIC |Feb 21 2021 | Jay Akhawri | Abstracted Runner Methods to DQRunner Class |

# COMMAND ----------

# %run "../KC-Collibra/KC-Collibra-DQ-RuleMaster"

# COMMAND ----------

#pip install snowflake-connector-python

# COMMAND ----------


#pip install great-expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set & Get Widgets

# COMMAND ----------

# Databricks Runner Notebook
dbutils.widgets.removeAll()
dbutils.widgets.text('filewithpath', '')
dbutils.widgets.text('PipelineID', '')
dbutils.widgets.text('DQLogSnowflakeDb', '')
dbutils.widgets.text('DQLogSnowflakeSchema', '')
dbutils.widgets.text('sf_user', '')
dbutils.widgets.text('sf_account', '')
dbutils.widgets.text('env', '')
dbutils.widgets.text('delimiter', '')

PipelineID = dbutils.widgets.get('PipelineID') #11340
dataFileName = dbutils.widgets.get('filewithpath')
DQLogSnowflakeDb = dbutils.widgets.get('DQLogSnowflakeDb') 
DQLogSnowflakeSchema = dbutils.widgets.get('DQLogSnowflakeSchema')
sf_user = dbutils.widgets.get('sf_user')
sf_account = dbutils.widgets.get('sf_account')
env = dbutils.widgets.get('env')
delimiter = dbutils.widgets.get('delimiter')


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Load Rule Engine Common Configurations 

# COMMAND ----------

# MAGIC %run "./Common/CommonUtil"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Selection

# COMMAND ----------

# MAGIC %run "./DQControllerNB_"

# COMMAND ----------

#  file = dataFileName
#  r = DQController()
#  rulesStr = r.get_rulesby_datafile(file)
#  print(file)
#  print(rulesStr)

# COMMAND ----------

dbutils.widgets.text('filewithpath', '')
dataFileName = dbutils.widgets.get('filewithpath')

print(dataFileName)

try: 
  #file = "/" + dataFileName.split("/", 1)[1]
  file = dataFileName
  r = DQController()
  rulesStr = r.get_rulesby_datafile(file)
  print(file)
  print(rulesStr)
  if rulesStr !='' and rulesStr !='Unrecognized Program Name':
    ruleList = json.loads(rulesStr)
    print("Rules to process " + rulesStr + " for " + dataFileName)    
  else:
    dbutils.notebook.exit("No rules to process")  
except Exception as e: 
  dbutils.notebook.exit(e)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Execution 

# COMMAND ----------

#try: 
if(rulesStr!=None and rulesStr!= 'null'):
  #file = "/" + dataFileName.split("/", 1)[1]
  file = dataFileName
  print(file)
  res = r.execute_rulesby_ruleID_and_dataFileName(rulesStr,file,PipelineID)
  #print(res)
  if res == None:
    raise Exception("Rule Execution Failed") #Generate exception
else:
  raise Exception("Rule Selection Failed")  #Generate exception
#except Exception as e: 
  #dbutils.notebook.exit(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate DQ Logs

# COMMAND ----------

try:
  r.generateDQLog(res, PipelineID)
except Exception as e: 
  print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Ingestion into Snowflake

# COMMAND ----------

dbutils.notebook.run("DQLogSnowflakeIngestion",300,{"PipelineID":PipelineID,"DQLogSnowflakeDb":DQLogSnowflakeDb,"DQLogSnowflakeSchema":DQLogSnowflakeSchema,"dataFileName":dataFileName,"sf_user":sf_user,"sf_account":sf_account,"env":env})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Operational Log

# COMMAND ----------

raiseException = False
  
for rc in ruleList:
  i=0
  if float(res[i]['Unexpected percentage']) == -1 :
    if rc[11] == "Error":
      raiseException = True
      break      
  i = i + 1

if raiseException == True:
  r.generateOpsLog(res, PipelineID, "Rule Execution Error")
  raise Exception("Rule Execution Error")
else:
  r.generateOpsLog(res, PipelineID, "")
  dbutils.notebook.exit(res)

# COMMAND ----------

