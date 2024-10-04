# Databricks notebook source
#pip install snowflake-connector-python

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
filePath = "NA/"+ env + "/UIF_config/ParameterFiles/KROGER_M6_RULE_MASTER.json"
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)

# COMMAND ----------

import snowflake.connector
from re import search
import pyspark
from pyspark.sql.types import StructType,StructField, StringType,LongType, TimestampType,DateType,FloatType,IntegerType,DecimalType

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
      #if search:
      if search(",",colName):
      	sql = "SELECT "+ colName + " as new_column from " + tableName 
      else:
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
    if search("item_master_", filePath):

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
        dataDf = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + filePath, header=False, sep="|" ,schema= custom_schema) 
    elif search("comp_weekly_pos", filePath):
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
        dataDf = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + filePath, header=False, sep="|" ,schema= custom_schema)
    elif search("KrogerCL", filePath) :
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
        dataDf = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + filePath, header=True, sep="," )		
    elif search("case_master", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    elif search("daily_audit", filePath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("BASE_UPC", StringType(), True),
	      StructField("OBS_DATE", DateType(), True),
	      StructField("OBS_TIME", StringType(), True),
	      StructField("FUTURE_MEASURE", StringType(), True),
      ])
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    elif search("daily_pos", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    elif search("daily_pos_restate", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    elif search("expected_price", filePath):
        custom_schema = StructType([
          StructField("RETAILER_ID", StringType(), True),
          StructField("DIV_NO", StringType(), True),
          StructField("STORE_NO", StringType(), True),
	      StructField("CONS_UPC", StringType(), True),
	      StructField("PRICE_DATE", DateType(), True),
	      StructField("LOYALTY_UNIT_PRICE", DecimalType(8,2), True),
	      StructField("RETAIL_UNIT_PRICE", DecimalType(8,2), True),
      ])
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    
    elif search("kdp_store_forecast", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("kdp_store", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )
    elif search("kdp_whs_forecast", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("kdp_whs", filePath):
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


        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("sath", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("store_deliveries", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("store_master", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("store_product", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("store_shelf", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("warehouse_master", filePath):
        custom_schema = StructType([
          StructField("WHS_NO", StringType(), True),
          StructField("WHS_DESC", StringType(), True),
      ])
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("whs_boh", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("whs_orders", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
    elif search("whs_substitutions", filePath):
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
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" ,schema= custom_schema )	
																						
    elif search("sales_plans", filePath):
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
        dataDf = spark.read.csv("abfss://" + containername + "@"+ accountname+ ".dfs.core.windows.net/" + filePath, header=False, sep="|" ,schema= custom_schema) 
    else:
        dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep = "|" )
    return dataDf
  
  def getData_from_adls_csv_only(self, accountName, containerName, filePath):    
    print("Adaptor - line79::getdata from adls csv")
    print(filePath)
    dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True )
    return dataDf

# COMMAND ----------

from re import search
from pyspark.sql.types import StructField, StructType, StringType,LongType,DateType,TimestampType
from pyspark.sql.functions import hash
from pyspark.sql.functions import col,lit
import pandas as pd 

class RuleVMGenerator:
  def __init__(self):
    pass  # No instance variables to initialise
  
  def create_global_temp_view_source(self,FileNameWpath,viewName):
    dal= DAL()
    print(FileNameWpath)
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
      print("Adapter: refDataListFromDAL")
      #print(refDataListFromDAL)
      #refColumn= "RefColumn"
      if refDataListFromDAL == []:
        pass
      else:
        refCols = []
        #refCols.append(refColumn)
        refCols = refColumn
        ref_col_n = refCols.split(",")
        if search(",",refCols):
            print("multiple cols")
            ref_col_n = refCols.split(",")
        #print(len(ref_col_n))
        #print("ref_col_n :   " ,ref_col_n)
            header_ref = []
            i=1
            while i<=len(ref_col_n):
                each_val = "_" + str(i)
                header_ref.append(each_val)
                i+=1
            finalviewdF = spark.createDataFrame(data=refDataListFromDAL)
            finalviewdF = finalviewdF.withColumn('new_column', hash(array(header_ref)))
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
  
  def build_ruleInfoview_for_DimFile(self,_dataFileName,ActiveRulesList):
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
        srcGlbViewName = self.create_global_temp_view_source(_dataFileName, "srcGlobalView_" + ruleId)
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
      
  def get_Active_Rules_for_DimFile(self,_dataFileName,sourceFileType):
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
          
        finalRuleList = self.build_ruleInfoview_for_DimFile(_dataFileName, ActiveRulesList)
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

# from pyspark.sql.types import StructField, StructType, StringType,LongType,DateType,TimestampType
# from pyspark.sql.functions import hash
# from pyspark.sql.functions import col,lit
# import pandas as pd 
# d = DAL()
# ctx = d.get_connection_snowflake()
# cursor = ctx.cursor()
# result_string = " "
# sql = "SELECT DIV_NO,STORE_NO from SELLOUT_GBL.STAGING_NA_KROGER.KROGER_M6_STORE_MASTER WHERE DIV_NO='703' AND STORE_NO='00001'"
# print(sql)
# cursor.execute(sql)
# df = cursor.fetch_pandas_all()
# result = df.values.tolist()
# #print(result)
# #refcol = "DIV_NO,STORE_NO"
# refCols = []
# refCols.append(refcol)
# refCols=refcol
# if search(refCols,","):
#     print("multiple cols")
# ref_col_n = refCols.split(",")
# #print(len(ref_col_n))
# #print("ref_col_n :   " ,ref_col_n)
# header = []
# i=1
# while i<=len(ref_col_n):
#     each_val = "_" + str(i)
#     header.append(each_val)
#     i+=1
#     #print("i is:" , i)
# print("header: ", header)
# #array(header).collect()
# #print("header: ", lit(header))
# #header = lit(header)
# finalviewdF = spark.createDataFrame(data=result)
# display(finalviewdF)
# #finalviewdF = finalviewdF.withColumn('hashed_name', hash('_1','_2'))
# finalviewdF = finalviewdF.withColumn('new_column', hash(array(header)))
# display(finalviewdF)


# COMMAND ----------

# from re import search
# from pyspark.sql.types import StructField, StructType, StringType,LongType,DateType,TimestampType
# from pyspark.sql.functions import hash
# from pyspark.sql.functions import col,lit
# import pandas as pd 
# refCols = []
# refcol="DIV_NO  STORE_NO"
# refCols=refcol
# if search(",",refCols):
#     print("multiple cols")
#     ref_col_n = refCols.split(",")
#     print(ref_col_n)
#     #ref_col_n = [i.replace("'","").strip(" ") for i in ref_col_n]
#     print(" length of ref_col_n is:  ",len(ref_col_n))
# print(ref_col_n[1])
# part1 = "custom_schema = StructType([ "
# part2 = []
# for i in ref_col_n:
#     part = "StructField(" + "\"" + i + "\"" + ", StringType(), True),"
#     part2.append(part)
#      #print(i)
# #print("part2 :  ",part2)
# part3 = " ])"
# print(part1 , part2 ,part3)

#custom_schema = StructType([
#         StructField("DIV_NO", StringType(), True),
#         StructField("STORE_NO", StringType(), True),
#          ])

# COMMAND ----------

from pyspark.sql.functions import *
from re import search
import json


class KrogerKCNARuleAdapter:
  
  def __init__(self):
    pass
  
  def get_sourceFileType(self,dataFileName):
    sourceFileType = ''

    try: 
      if search("Source_Files", dataFileName): 
        print("16: source files")
        if search("KROGER/M6/M6_CATEGORY/Raw/Data",dataFileName):
          print("18:KROGER")
          if search("item_master_", dataFileName):  
            sourceFileType = "item_master_" 
            return sourceFileType
          elif search("case_master", dataFileName):  
            sourceFileType = "case_master" 
            return sourceFileType
          elif search("comp_weekly_pos", dataFileName):  
            sourceFileType = "comp_weekly_pos" 
            return sourceFileType
          elif search("daily_audit", dataFileName):  
            sourceFileType = "daily_audit" 
            return sourceFileType
          elif search("daily_pos", dataFileName):  
            sourceFileType = "daily_pos" 
            return sourceFileType
          elif search("daily_pos_restate", dataFileName):  
            sourceFileType = "daily_pos_restate" 
            return sourceFileType
          elif search("expected_price", dataFileName):  
            sourceFileType = "expected_price" 
            return sourceFileType
          elif search("kdp_store_forecast", dataFileName):  
            sourceFileType = "kdp_store_forecast" 
            return sourceFileType
          elif search("kdp_store", dataFileName):  
            sourceFileType = "kdp_store" 
            return sourceFileType
          elif search("kdp_whs_forecast", dataFileName):  
            sourceFileType = "kdp_whs_forecast" 
            return sourceFileType
          elif search("kdp_whs", dataFileName):  
            sourceFileType = "kdp_whs" 
            return sourceFileType
          elif search("manifest", dataFileName):  
            sourceFileType = "manifest" 
            return sourceFileType
          elif search("sales_plans", dataFileName):  
            sourceFileType = "sales_plans" 
            return sourceFileType       
          elif search("sath", dataFileName):  
            sourceFileType = "sath" 
            return sourceFileType   
          elif search("store_deliveries", dataFileName):  
            sourceFileType = "store_deliveries" 
            return sourceFileType   
          elif search("store_master", dataFileName):  
            sourceFileType = "store_master" 
            return sourceFileType   
          elif search("store_product", dataFileName):  
            sourceFileType = "store_product" 
            return sourceFileType     
          elif search("store_shelf", dataFileName):  
            sourceFileType = "store_shelf" 
            return sourceFileType  
          elif search("warehouse_master", dataFileName):  
            sourceFileType = "warehouse_master" 
            return sourceFileType  
          elif search("whs_boh", dataFileName):  
            sourceFileType = "whs_boh" 
            return sourceFileType  
          elif search("whs_orders", dataFileName):  
            sourceFileType = "whs_orders" 
            return sourceFileType  
          elif search("whs_sustitutions", dataFileName):  
            sourceFileType = "whs_sustitutions" 
            return sourceFileType  
          else:
            print("Unrecognized data source")
            return None
        elif search("KROGER/M6/ECOM_DAILY_POS/Raw/Data",dataFileName):
            if search("KrogerCL", dataFileName):  
                sourceFileType = "ecom_daily" 
                return sourceFileType    
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

# dataFileName = "NA/dev/Source_Files/KROGER/M6/M6_CATEGORY/Raw/Data/2023/05/30/Filtered Unzip Files/4020385.KimberlyClark.zip/item_master_4020385_20230530120110.fil"
# file = "/" + dataFileName.split("/", 1)[1]
# a = KrogerKCNARuleAdapter()
# rulesStr = a.get_all_Active_Rules(dataFileName)
# print(rulesStr)