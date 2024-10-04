# Databricks notebook source
# pip install snowflake-connector-python

# COMMAND ----------

# import org.apache.spark.sql._
# import com.crealytics.spark.excel._
from pyspark.sql import SQLContext
import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('sf_user', '')
dbutils.widgets.text('sf_account', '')

sf_user = dbutils.widgets.get('sf_user')
sf_account = dbutils.widgets.get('sf_account')

dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../Common/CommonUtil"

# COMMAND ----------

containerName = "ct-dna-shr-sellout"
adlsContainer_GDL = "ct-dna-shr-sellout"
accountName = "stctdna"+env[0]+"eus21"
accountName_GDL = "stctdna"+env[0]+"eus21"
accountname = "stctdna"+env[0]+"eus21"
filePath = "NA/"+ env + "/UIF_config/ParameterFiles/RulesMaster_TARGET.json"
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)

# COMMAND ----------

import snowflake.connector
import pyspark

class DAL:
  def __init__(self):
    pass
  
  def get_connection_snowflake(self):
    
    print("connecting to snowflake in adapter")
    #sf_user = "APP_AZURE_CT_DNA_D"
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
    
    
    return snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse, 
        role=sf_role
    )
  
  def getData_from_snowflake_table(self, tableName, colName):
    result = []

    try:
      ctx = self.get_connection_snowflake()
      cursor = ctx.cursor()
      result_string = " "

      # Execute a statement that will generate a result set.
      sql = "SELECT "+ colName + " from " + tableName 
      print(sql)
      cursor.execute(sql)
          
      # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
      df = cursor.fetch_pandas_all()
      result = df.values.tolist()
    except Exception as e:
      print(e)
    finally:
      cursor.close()
      ctx.close()
    return result 
  
  def getData_from_adls_csv(self, accountName, containerName, filePath):    
    print("Adaptor - line72::getdata from adls csv")
    print(filePath)
    dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True ,sep = "|")
    return dataDf
  
  def getData_from_adls_csv_only(self, accountName, containerName, filePath):    
    print("Adaptor - line79::getdata from adls csv")
    print(filePath)
    dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True )
    return dataDf

  def getData_from_adls_excel(self, accountName, containerName, filePath):
    print("Adaptor - line79::getdata from adls excel")
    print(filePath)
    # pandasDF=pd.read_excel("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath)
    pandasDF=ps.read_excel("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=0)
    dataDf=pandasDF.to_spark()
    # dataDf = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.spark.useHeader", "true").load("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath)
    # dataDf = spark.read.format("com.crealytics.spark.excel").option("header","true").load("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath)
    # dataDF=dbutils.notebook.run("Read_Excel", 60)["dataDf"]
    return dataDf

# COMMAND ----------

from re import search

class RuleVMGenerator:
  def __init__(self):
    pass  # No instance variables to initialise
  
  def create_global_temp_view_source(self,FileNameWpath,viewName,fileFormat):
    dal= DAL()
    print(FileNameWpath)
    if fileFormat == "EXCEL":
        viewdF= dal.getData_from_adls_excel(accountName_GDL,adlsContainer_GDL,FileNameWpath)
    else:
        viewdF= dal.getData_from_adls_csv(accountName_GDL,adlsContainer_GDL,FileNameWpath)
    viewdF.createOrReplaceGlobalTempView(viewName)
    return viewName
          
  def create_global_temp_view_ref(self,refDataType, refData, refColumn, viewName, sourcefilePath):
    if refDataType == "":
      pass
    elif refDataType == "Snowflake":
      dal= DAL()
      tableName= refData
      print('refColumn from RuleAdapter: ',refColumn)
      refDataListFromDAL = dal.getData_from_snowflake_table(tableName, refColumn)
      #refColumn= "RefColumn"
      if refDataListFromDAL == []:
        pass
      else:
        refCols = []
        refCols.append(refColumn)
    
        finalviewdF = spark.createDataFrame(data=refDataListFromDAL, schema = refCols)
        finalviewdF.createOrReplaceGlobalTempView(viewName)  
    elif refDataType == "DateFormat":
      dateFormat= refData
      viewName = dateFormat  #Pass DateFormat instead of view Name
    elif refDataType == "Value":
      valueArray= refData.split(",") #Assuming multiple values will be seperated by ',' 
      refDataList= [[x] for x in valueArray]
      refColumn= "RefColumn"
      refCols = []
      refCols.append(refColumn)      
      finalviewdF = spark.createDataFrame(data=refDataList, schema = refCols)
      finalviewdF.createOrReplaceGlobalTempView(viewName)
    elif refDataType == "ADLS":  #Assuming ref data is a csv file
      dal= DAL()
      FileNameWpath=refData  #Assuming file path will be provided in reference data
      viewdF= dal.getData_from_adls_csv_only(accountName_GDL,adlsContainer_GDL,FileNameWpath)
      viewdF.createOrReplaceGlobalTempView(viewName)
    elif refDataType == "ADLS-Related":  #Assuming ref data is a csv file
      refDataFilePath= sourcefilePath.rsplit('/', 1)[0]      
      dal= DAL()
      FileNameWpath=refDataFilePath+'/'+refData  #Assuming file name will be provided in reference data
      print(FileNameWpath)
      viewdF= dal.getData_from_adls_csv(accountName_GDL,adlsContainer_GDL,FileNameWpath)
      viewdF.createOrReplaceGlobalTempView(viewName)
    else:
      print("Unrecognized Reference Data Type")
    return viewName
  
  def build_ruleInfoview_for_DimFile(self,_dataFileName,ActiveRulesList, fileFormat):
     try:
      finalRuleList = []
        
      for i in range(len(ActiveRulesList)):
        finalRuleList.append([])
        ruleId = str(ActiveRulesList[i][1])
        refCol = str(ActiveRulesList[i][4])
        srcCol = str(ActiveRulesList[i][3])
        refData = str(ActiveRulesList[i][5])
        refDataType=str(ActiveRulesList[i][11])
        print(refDataType)
        
        finalRuleList[i].append(str(ActiveRulesList[i][0])) # Rule_Category
        finalRuleList[i].append(ruleId) # Rule_Id
        finalRuleList[i].append(str(ActiveRulesList[i][2])) # Rule_label
        
        #Source Global View Name, Source Column (Assuming Source File as csv in ADLS)
        srcGlbViewName = self.create_global_temp_view_source(_dataFileName, "srcGlobalView_" + ruleId, fileFormat)
        finalRuleList[i].append(srcGlbViewName) # srcGlbViewName
        finalRuleList[i].append(str(ActiveRulesList[i][3]))  #SrcColumn     
        
        #Ref Global View Name, Ref Column        
        refDataGlbViewName= self.create_global_temp_view_ref(refDataType, refData, refCol, "refGlobalView_" + ruleId, _dataFileName)
        finalRuleList[i].append(refDataGlbViewName)  # refGlbViewName
        if refDataType == "Snowflake" or refDataType == "Value":
          finalRuleList[i].append(str(ActiveRulesList[i][4]))
          #finalRuleList[i].append("RefColumn") #RefColumn
        else:
          finalRuleList[i].append(str(ActiveRulesList[i][4])) #RefColumn
          
        #row.Rule_Zone, row.Rule_Threshold, row.Domain, row.Source_System, row.Rule_Action
        finalRuleList[i].append(str(ActiveRulesList[i][6]))
        finalRuleList[i].append(str(ActiveRulesList[i][7]))
        finalRuleList[i].append(str(ActiveRulesList[i][8]))
        finalRuleList[i].append(str(ActiveRulesList[i][9]))
        finalRuleList[i].append(str(ActiveRulesList[i][10]))
        finalRuleList[i].append(_dataFileName)
        
        finalRuleList[i].append(str(ActiveRulesList[i][12])) 
        finalRuleList[i].append(str(ActiveRulesList[i][13])) 
#         finalRuleList[i].append('10000000')   # Rule_priority
        flagSourceFile = 'true'  # To flag the records for the rule in source file
        finalRuleList[i].append(flagSourceFile)   ##
        finalRuleList[i].append(str(ActiveRulesList[i][14]))
        
        
      return finalRuleList
     except Exception as e:
      print(e)
      
  def get_Active_Rules_for_DimFile(self,_dataFileName,sourceFileType, fileFormat):
    try:
      print("adapter get active rules for dim file")
      print(_dataFileName)
      dataFileName_FilePart = _dataFileName.split("/")[-1]
      print(dataFileName_FilePart)
      
      dataFileName_PathPart = _dataFileName.rsplit('/', 1)[0]
      File_Name = dataFileName_FilePart           
      
      # How many Active Rules are there in RuleMaster file for the WMA Retail file i.e. sourceFileType = WMA Retail
      ActiveRulesDF = spark.sql("select Rule_Category, Rule_Id,Rule_Metadata.Rule_label,Rule_Metadata.source_col_name, Rule_Metadata.RefDataType, Rule_Metadata.RefData, Rule_Metadata.RefColumn, Rule_Zone, Rule_Threshold, Domain, Source_System,Rule_Action,Rule_Name,Rule_Desc,Rule_Priority from rules where Rule_Active_Flag = 'Active' and Source_File ='" + sourceFileType + "'")
          
      if ActiveRulesDF.count() > 0 :
        rulesStr = ''    
        
        ActiveRulesList = list([(row.Rule_Category,row.Rule_Id,row.Rule_label,row.source_col_name,row.RefColumn, row.RefData, row.Rule_Zone, row.Rule_Threshold, row.Domain, row.Source_System, row.Rule_Action, row.RefDataType, row.Rule_Name, row.Rule_Desc, row.Rule_Priority) for row in ActiveRulesDF.collect()])
          
        finalRuleList = self.build_ruleInfoview_for_DimFile(_dataFileName, ActiveRulesList, fileFormat)
        rulesStr = json.dumps(finalRuleList)
        print()
        return rulesStr
      else:
        print("No active rule for the file exists")
        rulesStr = ""
        return rulesStr
    except Exception as e:
      print(e)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
from re import search
import json


class TargetKCNARuleAdapter:
  
  def __init__(self):
    pass
  
  def get_sourceFileType(self,dataFileName):
    sourceFileType = ''

    try: 
      if search("Source_Files", dataFileName): 
        print("16: source files")
        if search("TARGET/RSI/",dataFileName):
          print("18:TARGET")
          print(dataFileName)
          if search("Forecast", dataFileName):   
            sourceFileType = "Target_DC_Forecast" 
            return sourceFileType
          elif search("CTGY_DAILY_POS", dataFileName):   
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Category Daily POS" 
            return sourceFileType
          elif search("CTGY_OMNICHANNEL​", dataFileName):  
            sourceFileType = "Target Category Omni Channel Extract" 
            return sourceFileType
          elif search("CTGY_PRODUCT_MASTER", dataFileName):   
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Category Product" 
            return sourceFileType
          elif search("CTGY_STORE_MASTER", dataFileName):  
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Category Store" 
            return sourceFileType
          elif search("PRODUCT_MASTER", dataFileName):  
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Product" 
            return sourceFileType
          elif search("INVENTORY_INDICATORS", dataFileName):   
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Inventory Indicator" 
            return sourceFileType
          elif search("OMNICHANNEL", dataFileName):  
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Daily Omni POS Data" 
            return sourceFileType
          elif search("STORE_MASTER", dataFileName):  
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - Store" 
            return sourceFileType
          elif search("DC_MASTER", dataFileName): 
            sourceFileType = "SNOWFLAKE_Kimberly ClarkTarget - DC" 
            return sourceFileType
          else:
            # print("Printing unable to find")
            print("Unrecognized data source")
            return None
        else:
          print("Unrecognized data source")
          return None
      else:
        print("Unrecognized Program Name. Supported RegionalDL")
        return None
    except Exception as e:  
      print(e) 
  
  
  # Load the temporary view for Rules
  def load_RuleMasterTemporaryView(self):
    try:      
      rulemasterFiledf = spark.read.option("multiline","true").format("json").load("abfss://" + adlsContainer_GDL + "@"+ accountName_GDL + ".dfs.core.windows.net/" + ruleMasterFileName_KCNA)

    except Exception as e:  
      print(e)  
      dbutils.notebook.exit("Failed Reading Rule Master from " + ruleMasterFileName_KCNA)

    # Create Temporary View for all Rules
    rulemasterFiledf.createOrReplaceTempView("rules")

  
  def get_all_Active_Rules(self, _dataFileName, fileFormat):  
    try:
      sourceFileType = self.get_sourceFileType(_dataFileName)
      print("printing FIle type",sourceFileType)
      if(sourceFileType != None):
        self.load_RuleMasterTemporaryView()
        RuleVMGen = RuleVMGenerator()
        rulesStr = RuleVMGen.get_Active_Rules_for_DimFile(_dataFileName, sourceFileType, fileFormat)    
        return rulesStr
      else:
        return ''
    except Exception as e:
      print(e)

# COMMAND ----------

# dataFileName = "NA/dev/Source_Files/TARGET/RSI/PRODUCT_MASTER/Raw/Data/2023/06/07/Kimberly ClarkTarget - Product Master - 1353-10186361.txt"
# file = "/" + dataFileName.split("/", 1)[1]
# a = TARGETKCNARuleAdapter()
# rulesStr = a.get_all_Active_Rules(dataFileName)
# print(rulesStr)

# COMMAND ----------

from pyspark.sql.functions import *
from re import search
import json


class KCNARuleAdapter:
  
  def __init__(self):
    pass
  
  def get_sourceFileType(self,dataFileName):
    sourceFileType = ''

    try: 
      if search("externalsources", dataFileName): 
        if search("KCNA",dataFileName):
          if search("Costco_Category_Totals", dataFileName):  #Added Supplier Master Report Daily 
            sourceFileType = "Costco Category Totals" 
            return sourceFileType
          elif search("Warehouse LVL Daily Data - Latest 6 Weeks", dataFileName):
            sourceFileType = "Warehouse LVL Daily Data - Latest 6 Weeks" 
            return sourceFileType
          else:
            print("Unrecognized data source")
            return None
        else:
          print("Unrecognized data source")
          return None
      else:
        print("Unrecognized Program Name. Supported RegionalDL")
        return None
    except Exception as e:  
      print(e) 
  
  
  # Load the temporary view for Rules
  def load_RuleMasterTemporaryView(self):
    try:      
      rulemasterFiledf = spark.read.option("multiline","true").format("json").load("abfss://" + adlsContainer_GDL + "@"+ accountName_GDL + ".dfs.core.windows.net/" + ruleMasterFileName_KCNA)

    except Exception as e:  
      print(e)  
      dbutils.notebook.exit("Failed Reading Rule Master from " + ruleMasterFileName_KCNA)

    # Create Temporary View for all Rules
    rulemasterFiledf.createOrReplaceTempView("rules")

  
  def get_all_Active_Rules(self, _dataFileName):  
    try:
      sourceFileType = self.get_sourceFileType(_dataFileName)
      print(sourceFileType)
      if(sourceFileType != None):
        self.load_RuleMasterTemporaryView()

        RuleVMGen = RuleVMGenerator()
        rulesStr = RuleVMGen.get_Active_Rules_for_DimFile(_dataFileName, sourceFileType)      
        return rulesStr
      else:
        return ''
    except Exception as e:
      print(e)

# COMMAND ----------

