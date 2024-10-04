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
from pyspark.sql.types import StructField, StructType, StringType,LongType,DateType,TimestampType,DecimalType ,IntegerType,FloatType

class DQController:
  rulesStr = ''
  iriAdapterNB = ''
  inputDataViewName = ''  

  def __init__(self):
    pass   # No instance variables to initialise

  def create_global_temp_view_source(self, FileNameWpath, viewName, fileType):     
    if (fileType == "CSV"):
      if search("item_master_", FileNameWpath):
        custom_schema = StructType([
        StructField("RETAILER_ID", StringType(), True),
        StructField("REF_DATE", StringType(), True),
        StructField("DIV_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("CONS_UPC_DESC", StringType(), True),
	      StructField("CONS_UPC_SIZE_DESC", StringType(), True),
	      StructField("CONS_ACTIVE_CD", StringType(), True),
	      StructField("CONS_QTY_UOM", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
	      StructField("BASE_UPC_DESC", StringType(), True),
	      StructField("DEFAULT_CASE_UPC", StringType(), True),
	      StructField("DEPT_CD", StringType(), True),
	      StructField("DEPT_DESC", StringType(), True),
	      StructField("SUB_DEPT_CD", StringType(), True),
	      StructField("SUB_DEPT_DESC", StringType(), True),
	      StructField("COMMODITY_CD", StringType(), True),
	      StructField("COMMODITY_DESC", StringType(), True),
	      StructField("SUB_COMMODITY_CD", StringType(), True),
	      StructField("SUB_COMMODITY_DESC", StringType(), True),
	      StructField("DEFAULT_WHS", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + FileNameWpath, header=False, sep="|" ,schema= custom_schema) 
      elif search("comp_weekly_pos", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("WE_POS_DATE", DateType(), True),
	      StructField("SALES_UNIT_QTY", IntegerType(), True),
	      StructField("SALES_UNIT_AMT", DecimalType(15,2), True),
	      StructField("POS_AVE_UNIT_PRC", DecimalType(8,2), True),
		  StructField("SALES_LBS_QTY", DecimalType(15,2), True),
		  StructField("SALES_LBS_AMT", DecimalType(15,2), True),
		  StructField("POS_AVE_LBS_PRC", DecimalType(8,2), True),
		  StructField("CONS_UPC_DESC", StringType(), True),
		  StructField("CONS_UPC_SIZE_DESC", StringType(), True),
		  StructField("COMMODITY_CD", StringType(), True),
		  StructField("COMMODITY_DESC", StringType(), True),
		  StructField("SUB_COMMODITY_CD", StringType(), True),
		  StructField("SUB_COMMODITY_DESC", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("KrogerCL", FileNameWpath):
        custom_schema = StructType([
          StructField("DATE_KEY", StringType(), True),
          StructField("PRIMARY_DEPARTMENT", StringType(), True),
          StructField("RECAP_DEPARTMENT", StringType(), True),
	      StructField("DEPARTMENT", StringType(), True),
	      StructField("COMMODITY", StringType(), True),
	      StructField("SUBCOMMODITY", StringType(), True),
	      StructField("UPC", StringType(), True),
	      StructField("DESCRIPTION", StringType(), True),
		  StructField("KC_PRIMARY_BRAND", StringType(), True),
		  StructField("KC_PRIMARY_MANUFACTURER", StringType(), True),
		  StructField("KC_PACK_SIZE", StringType(), True),
		  StructField("KC_PACKING_TYPE", StringType(), True),
		  StructField("STO_CLICKLIST", StringType(), True),
		  StructField("STO_NAME", StringType(), True),
		  StructField("STO_ZIP", StringType(), True),
		  StructField("HOMEDELIVERY_SCANNED_RETAIL_DOLLARS_CUR", FloatType(), True),
		  StructField("HOMEDELIVERY_SCANNED_MOVEMENT_CUR", FloatType(), True),
		  StructField("CLICKLIST_SCANNED_RETAIL_DOLLARS_CUR", FloatType(), True),
		  StructField("CLICKLIST_SCANNED_MOVEMENT_CUR", FloatType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=True ,sep = ","  )	
      elif search("case_master", FileNameWpath):
        custom_schema = StructType([
          StructField("VENDOR_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
          StructField("CASE_UPC", StringType(), True),
	      StructField("KLN", StringType(), True),
	      StructField("CASE_DESC", StringType(), True),
	      StructField("UNITS_PER_CASE", IntegerType(), True),
	      StructField("REF_DATE", DateType(), True),
	      StructField("CORP_VEND_CODE", StringType(), True),
	      StructField("CORP_VEND_NAME", StringType(), True),
	      StructField("DISCONTINUE_DATE", DateType(), True),
	      StructField("STORE_PACK", IntegerType(), True),
	      StructField("INNER_PACK_QTY", IntegerType(), True),
	      StructField("CONS_UPC", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("daily_audit", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
	      StructField("OBS_DATE", DateType(), True),
	      StructField("OBS_TIME", StringType(), True),
	      StructField("FUTURE_MEASURE", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("daily_pos", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("POS_DATE", DateType(), True),
	      StructField("SALES_QTY", DecimalType(15,2), True),
	      StructField("SALES_QTY_UOM", StringType(), True),
		  StructField("POS_AVE_UNIT_PRC", DecimalType(8,2), True),
		  StructField("SALES_AMT", DecimalType(15,2), True),
		  StructField("GROSS_MARGIN_AMT", DecimalType(15,2), True),
		  StructField("INV_MULTIPLIER", IntegerType(), True),
		  StructField("ESTIMATED_FLAG", StringType(), True),
		  StructField("RESTATEMENT_FLAG", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("daily_pos_restate", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("POS_DATE", DateType(), True),
	      StructField("SALES_QTY", DecimalType(15,2), True),
	      StructField("SALES_QTY_UOM", StringType(), True),
		  StructField("POS_AVE_UNIT_PRC", DecimalType(8,2), True),
		  StructField("SALES_AMT", DecimalType(15,2), True),
		  StructField("GROSS_MARGIN_AMT", DecimalType(15,2), True),
		  StructField("INV_MULTIPLIER", IntegerType(), True),
		  StructField("ESTIMATED_FLAG", StringType(), True),
		  StructField("RESTATEMENT_FLAG", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("expected_price", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("PRICE_DATE", DateType(), True),
	      StructField("LOYALTY_UNIT_PRICE", DecimalType(8,2), True),
	      StructField("RETAIL_UNIT_PRICE", DecimalType(8,2), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      
      elif search("kdp_store_forecast", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
	      StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
	      StructField("FCAST_CREATE_DATE", DateType(), True),
		  StructField("FCAST_DATE", DateType(), True),
	      StructField("DELIV_DAY_FL", StringType(), True),
          StructField("ORDER_DAY_FL", StringType(), True),
		  StructField("FCAST_BOD_BOH", IntegerType(), True),
		  StructField("FCAST_EOD_BOH", IntegerType(), True),
		  StructField("BASE_FCAST", DecimalType(15,4), True),
          StructField("TURN_FCAST", DecimalType(15,4), True),
		  StructField("FCAST", DecimalType(15,4), True),
		  StructField("FCAST_UNIT_PRICE", DecimalType(8,2), True),
		  StructField("FCAST_DISPLAY_FL", StringType(), True),
		  StructField("FCAST_ORDER_QTY", IntegerType(), True),
		  StructField("REG_UNIT_PRICE", DecimalType(8,2), True),
		  StructField("DV_AD_TYPE", StringType(), True),
		  StructField("FCAST_DELIV_QTY", IntegerType(), True),
		  StructField("ON_ORDER_QTY", IntegerType(), True),
		  StructField("PROMO_FL", StringType(), True),
		  StructField("EXP_OOS_FL", StringType(), True),
		  StructField("EXP_EXCESS_INV_FL", StringType(), True),
		  StructField("EXP_OOS_LOST_AMT", DecimalType(9,2), True),
		  StructField("EXP_EXCESS_INV_VAL", DecimalType(9,2), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("kdp_store", FileNameWpath):
        custom_schema = StructType([
                           StructField("RETAILER_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
          StructField("DIV_NO", StringType(), True),
	      StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
	      StructField("DV_DATE", DateType(), True),
	      StructField("SALES_QTY", DecimalType(15,2), True),
		  StructField("SALES_AMT", DecimalType(15,2), True),
		  StructField("QTY_UOM", StringType(), True),
		  StructField("BASE_FCAST", DecimalType(15,4), True),
		  StructField("TURN_FCAST", DecimalType(15,4), True),
		  StructField("FCAST", DecimalType(15,4), True),
		  StructField("REG_UNIT_PRICE", DecimalType(8,2), True),
		  StructField("BEST_OFFER_UNIT_PRICE", DecimalType(8,2), True),
		  StructField("DATE_FIRST_SEEN", DateType(), True),
		  StructField("LAST_SOLD_DATE", DateType(), True),
		  StructField("LAST_DELIVERY_DATE", DateType(), True),
		  StructField("PLAN_UNIT_PRICE", DecimalType(8,2), True),
		  StructField("PLANNED_DISPLAY_FL", StringType(), True),
		  StructField("DV_AD_TYPE", StringType(), True),
		  StructField("INVENTORY_VALUE_ALERT", StringType(), True),
		  StructField("OOS_ALERT_FL", StringType(), True),
		  StructField("SHORT_SALES_ALERT_FL", StringType(), True),
		  StructField("PROMO_EXEC_ALERT_FL", StringType(), True),
		  StructField("EXCESS_INV_ALERT_FL", StringType(), True),
		  StructField("LOW_INV_PROMO_ALERT_FL", StringType(), True),
		  StructField("UNEXP_PRC_DECR_ALERT_FL", StringType(), True),
		  StructField("ORDER_DAY_FL", StringType(), True),
		  StructField("ORDER_QTY", IntegerType(), True),
		  StructField("DELIV_QTY", IntegerType(), True),
		  StructField("DISTR_QTY", IntegerType(), True),
		  StructField("EST_BOD_BOH", IntegerType(), True),
		  StructField("EST_EOD_BOH", IntegerType(), True),
		  StructField("EST_DOS", DecimalType(6,2), True),
		  StructField("TARGET_EOD_BOH", DecimalType(11,2), True),
		  StructField("TURN_FCAST_STD_ERR", DecimalType(11,4), True),
		  StructField("PROMO_FCAST_STD_ERR", DecimalType(11,4), True),
		  StructField("DV_STORE_AUTH", StringType(), True),
		  StructField("SHORT_SALES_ALERT_VAL", DecimalType(9,2), True),
		  StructField("PROMO_EXEC_ALERT_VAL", DecimalType(9,2), True),
		  StructField("EXCESS_INV_ALERT_VAL", DecimalType(9,2), True),
		  StructField("LOW_INV_PROMO_ALERT_VAL", DecimalType(9,2), True),
		  StructField("UNEXP_PRC_DECR_ALERT_VAL", DecimalType(9,2), True),
		  StructField("OOS_LOST_UNITS", DecimalType(9,2), True),
		  StructField("OOS_LOST_AMT", DecimalType(9,2), True),
		  StructField("PROMO_FL", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )
      elif search("kdp_whs_forecast", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("CASE_UPC", StringType(), True),
		  StructField("WHS_NO", StringType(), True),
	      StructField("FCAST_CREATE_DATE", DateType(), True),
		  StructField("FCAST_DATE", DateType(), True),
	      StructField("ORDER_CASE_PACK", IntegerType(), True),
	      StructField("FCAST_STORE_ORDER_QTY", IntegerType(), True),
		  StructField("FCAST_STORE_DEMAND_QTY", IntegerType(), True),
	      StructField("ORDER_QTY_UOM", StringType(), True),
		  StructField("DEMAND_QTY_UOM", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("kdp_whs", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
	      StructField("DV_DATE", DateType(), True),
	      StructField("CASE_UPC", StringType(), True),
	      StructField("INVENTORY_QTY", IntegerType(), True),
		  StructField("QTY_UOM", StringType(), True),
	      StructField("FCAST_DOS", DecimalType(6,2), True),
		  StructField("FCAST_STORE_ORDER_QTY", IntegerType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	

      elif search("sath", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
		  StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
		  StructField("AUTHORIZATION_CODE", StringType(), True),
	      StructField("AUTHORIZATION_FLAG", StringType(), True),
	      StructField("POG_NAME", StringType(), True),
		  StructField("POG_REVISION_DATE", TimestampType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("store_deliveries", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
		  StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
		  StructField("CASE_UPC", StringType(), True),
	      StructField("WHS_NO", StringType(), True),
	      StructField("DELIVERY_DATE", DateType(), True),
		  StructField("SHIP_DATE", DateType(), True),
		  StructField("DELIVERY_QTY", IntegerType(), True),
		  StructField("ORDER_QTY", IntegerType(), True),
		  StructField("QTY_UOM", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("store_master", FileNameWpath):
        custom_schema = StructType([
          StructField("DIV_NO", StringType(), True),
		  StructField("STORE_NO", StringType(), True),
	      StructField("STORE_BANNER", StringType(), True),
		  StructField("CHECK_LANE_QTY", IntegerType(), True),
	      StructField("SALES_SQUARE_FEET", IntegerType(), True),
	      StructField("PHONE_NO", StringType(), True),
		  StructField("ADDRESS", StringType(), True),
		  StructField("CITY", StringType(), True),
		  StructField("COUNTY", StringType(), True),
		  StructField("STATE", StringType(), True),
		  StructField("ZIP", StringType(), True),
          StructField("OPEN_DATE", DateType(), True),
          StructField("OPEN_FLAG", StringType(), True),
          StructField("CLOSE_DATE", DateType(), True),
          StructField("MAJOR_TYPE_CODE", StringType(), True),
          StructField("MAJOR_TYPE_DESC", StringType(), True),
          StructField("SUB_CLASS_CODE", StringType(), True),
          StructField("SUB_CLASS_DESC", StringType(), True),
          StructField("ACC_ACTIVE_CODE", StringType(), True),
          StructField("TOTAL_SQUARE_FEET", IntegerType(), True),
          StructField("MSA_CODE", StringType(), True),
          StructField("MSA_DESC", StringType(), True),
          StructField("BLO_NO", StringType(), True),
          StructField("ADV_WEEK_START", StringType(), True),
          StructField("MGT_ZONE", StringType(), True),
          StructField("DIV_ID", IntegerType(), True),
          StructField("DIV_DESC", StringType(), True),
          StructField("MGT_DIV_ID", IntegerType(), True),
          StructField("MGT_DIV_NO", StringType(), True),
          StructField("MGT_DIV_DESC", StringType(), True),
          StructField("REGION_ID", IntegerType(), True),
          StructField("REGION_CODE", StringType(), True),
          StructField("REGION_DESC", StringType(), True),
          StructField("GAS_OPEN_DATE", DateType(), True),
          StructField("GAS_CLOSE_DATE", DateType(), True),
          StructField("GAS_FLAG", StringType(), True),
          StructField("STORE_OPEN_HOUR_MON", StringType(), True),
          StructField("STORE_CLOSE_HOUR_MON", StringType(), True),
          StructField("STORE_OPEN_HOUR_TUE", StringType(), True),
          StructField("STORE_CLOSE_HOUR_TUE", StringType(), True),
          StructField("STORE_OPEN_HOUR_WED", StringType(), True),
          StructField("STORE_CLOSE_HOUR_WED", StringType(), True),
          StructField("STORE_OPEN_HOUR_THU", StringType(), True),
		  StructField("STORE_CLOSE_HOUR_THU", StringType(), True),
		  StructField("STORE_OPEN_HOUR_FRI", StringType(), True),
		  StructField("STORE_CLOSE_HOUR_FRI", StringType(), True),
          StructField("STORE_OPEN_HOUR_SAT", StringType(), True),
		  StructField("STORE_CLOSE_HOUR_SAT", StringType(), True),
		  StructField("STORE_OPEN_HOUR_SUN", StringType(), True),
		  StructField("STORE_CLOSE_HOUR_SUN", StringType(), True),
		  
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("store_product", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
		  StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
		  StructField("CONS_UPC", StringType(), True),
	      StructField("CASE_UPC", StringType(), True),
	      StructField("KLN", StringType(), True),
		  StructField("WHS_NO", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("store_shelf", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
		  StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
		  StructField("INVENTORY_DATE", DateType(), True),
	      StructField("INVENTORY_TIME", StringType(), True),
	      StructField("INVENTORY_QTY", StringType(), True),
		  StructField("QTY_UOM", StringType(), True),
		  StructField("ORDER_FLAG", StringType(), True),
		  StructField("POS_HOST_STATUS", StringType(), True),
		  StructField("CONSUMER_CASE_PACK", IntegerType(), True),
		  StructField("SHELF_CAPACITY", IntegerType(), True),
		  StructField("SHELF_MIN", IntegerType(), True),
		  StructField("AISLE_LOCATION", StringType(), True),
		  StructField("AISLE", StringType(), True),
		  StructField("SHELF_NO", IntegerType(), True),
		  StructField("POSITION_NO", IntegerType(), True),		  
		  StructField("FACINGS", IntegerType(), True),		
		  StructField("AISLE_ORIENTATION", StringType(), True),		  
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("warehouse_master", FileNameWpath):
        custom_schema = StructType([
          StructField("WHS_NO", StringType(), True),
          StructField("WHS_DESC", StringType(), True),
      ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("whs_boh", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
		  StructField("CASE_UPC", StringType(), True),
	      StructField("KLN", StringType(), True),
		  StructField("INVENTORY_DATE_TIME", TimestampType(), True),
	      StructField("INVENTORY_QTY", IntegerType(), True),
	      StructField("QTY_UOM", StringType(), True),
		  StructField("OPEN_STORE_ORDER_QTY", IntegerType(), True),
		  StructField("QTY_ON_ORDER", IntegerType(), True),
		  StructField("INNER_PACK_QTY", IntegerType(), True),
		  StructField("STORE_PACK", IntegerType(), True),
		       ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("whs_orders", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
		  StructField("CASE_UPC", StringType(), True),
	      StructField("PURCHASE_ORDER_NO", StringType(), True),
		  StructField("ORDER_QTY", IntegerType(), True),
	      StructField("QTY_UOM", StringType(), True),
	      StructField("ORDER_DUE_DATE", DateType(), True),
		  StructField("REF_DATE", DateType(), True),
		  StructField("KLN", StringType(), True),
		  StructField("COST_PER_QTY", DecimalType(15,2), True),
		  StructField("INNER_PACK_QTY", IntegerType(), True),
		  StructField("STORE_PACK", IntegerType(), True),
		  StructField("DISCONTINUE_DATE", DateType(), True),		  
		       ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
      elif search("whs_substitutions", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("WHS_NO", StringType(), True),
		  StructField("CASE_UPC", StringType(), True),
	      StructField("BEGIN_DATE", DateType(), True),
		  StructField("END_DATE", DateType(), True),
	      StructField("REPLACEMENT_CASE_UPC", StringType(), True),
	      StructField("SUBSTITUTION_METHOD", IntegerType(), True),
		  StructField("SUBSTITUTION_MULTIPLE", IntegerType(), True),
		  StructField("DIV_NO", StringType(), True),	  
		       ])
        viewdF = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + FileNameWpath,header=False ,sep = "|" ,schema= custom_schema )	
																						
      elif search("sales_plans", FileNameWpath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("EFF_DATE", StringType(), True),
	      StructField("SALES_PLAN_UNIT_PRICE", DecimalType(8,2), True),
	      StructField("UNIT_PRICE_UOM", StringType(), True),
		  StructField("DISPLAY_INDICATOR", StringType(), True),
		  StructField("DISPLAY_PRIORITY", IntegerType(), True),
		  StructField("AD_TYPE_CODE_1", StringType(), True),
		  StructField("AD_TYPE_CODE_2", StringType(), True),
		  StructField("AD_VERSION", IntegerType(), True),
		  StructField("AD_DESCRIPTION", StringType(), True),
		  StructField("AD_POSITION", IntegerType(), True),
		  StructField("AD_PRIORITY", IntegerType(), True),
		  StructField("AD_SEQUENCE", IntegerType(), True),
		  StructField("PROGRAM_ID", IntegerType(), True),
		  StructField("AD_SECTION_DESC", StringType(), True),
		  StructField("MEDIA_TYPE_CODE", StringType(), True),
		  StructField("DV_AD_TYPE", IntegerType(), True),
      ]) 
        viewdF = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + FileNameWpath, header=False, sep="|" ,schema= custom_schema) 
      else:
        viewdF = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + FileNameWpath, header=False, sep="|" )  
      display(viewdF)
      viewdF.createOrReplaceGlobalTempView(viewName)
    return viewName
  
  def saveLogFileInADLS(self, df, accountName, containerName, accountSecret, filePath, dataFileName):  ##
    accountName = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
    directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
    url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

    spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
    spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)
    
    outputTempPath = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + filePath + "/temp_"+ dataFileName    
    df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(outputTempPath)
    files = dbutils.fs.ls(outputTempPath + "/")
    ops_file = [file.path for file in files if file.path.endswith(".csv")][0]
    errorFilePath = filePath + "/" + dataFileName + ".csv"
    opTargetPathADLS = 'abfss://' + containerName + '@'+accountName +'.dfs.core.windows.net/' + errorFilePath 
    print(opTargetPathADLS)
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
      if search("Source_Files", _dataFileName):
        print("DQ controller line 40")
        print(_dataFileName)
        if search("KROGER",_dataFileName):
            a = KrogerKCNARuleAdapter()
            rulesStr = a.get_all_Active_Rules(_dataFileName)
            print(rulesStr)
            if rulesStr !='':         
              x = self.create_global_temp_view_source(_dataFileName, self.inputDataViewName, "CSV") #Create source data global view   
              data_file = spark.sql("select * from global_temp." + x )
              print("*********************data_file***********")
              display(data_file)
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
      print("Line 128:"+ File_Name)
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
      print("dataFileName"+dataFileName)
      print("dataFileName_PathPart"+dataFileName_PathPart)
      Log_Path = ''
      filePartList= dataFileName_PathPart.split('/Data/',1)
      print("filePartList[0]" + filePartList[0])
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
          dataFileWithRuleFlagDf= spark.sql("select * from global_temp." + self.inputDataViewName)
          print("******************dataFileWithRuleFlagDf before notebook call *****************")
          display(dataFileWithRuleFlagDf)
          print("*************************before uniqueness notebook call***************************************")          
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
          print("*************************after uniqueness notebook call***************************************")  
          dqresults.append(dqresult)
          print("*************************appending uniqueness result***************************************")  
        except Exception as e:
          print(e)
      elif rc[0] == "Timeliness":
        print("Calling Timeliness NB to process Rule: " + rc[1])
        res = self.run_with_retry("./KC-DQ-TimelinessCheckNB", 60, {"RuleID":rc[1], "dataFileName":_dataFileName}, max_retries = 2)
      else:
        print("Unknown Rule Category")
    print("*************************save file***************************************")  
    print('Saving Source File with Rule Flag in Source Folder')
    #Save Source data file with Rule flag
    dataFileWithRuleFlagDf= spark.sql("select * from global_temp." + self.inputDataViewName)
    print("******************dataFileWithRuleFlagDf  *****************")
    #display(dataFileWithRuleFlagDf)
    
    # called Cleaning on flagged source file
    dataFileWithRuleFlagDf = cleaning_flagged_src_file(dataFileWithRuleFlagDf)
    
    srcFileNameWFlag = 'DQ_SRC_'+ fileName.split('.')[0]
    self.saveLogFileInADLS(dataFileWithRuleFlagDf, accountname, containername, adlsSecret, refineFolder, srcFileNameWFlag)

#This code block belongs to consolidated DQ Error Failure Notification

    outputPath = _dataFileName.split('/Data')[0] + "/logs/DQ/" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2) + "/error_logs"
    print(outputPath)

    try:
      output_path_mount = dbutils.fs.ls('abfss://' + containername + '@' + accountname + '.dfs.core.windows.net/' + outputPath)
      summary_error_log = merge_error_log(containername, accountname, PipelineID, outputPath)
      merged_file = 'InvalidRecords_' + PipelineID + "_" + fileName.split('.')[0] + "_" + str(datetime.now().year) + str(datetime.now().month).zfill(2) +  str(datetime.now().day).zfill(2)
      self.saveLogFileInADLS(summary_error_log, accountname, containername, adlsSecret, outputPath, merged_file)

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
      self.saveLogFileInADLS(tempdf, accountname, containername, adlsSecret, outputPath, file_name_summary)
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        print('The path does not exist')  
        
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

#dataFileName="/externalsources/KCNA/IRI/Costco_Category_Totals/Raw/Data/2022/08/25/Costco Category Totals.csv"