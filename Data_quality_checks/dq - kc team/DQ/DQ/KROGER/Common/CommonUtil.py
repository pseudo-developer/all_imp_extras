# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

#print(env[0])


# COMMAND ----------

containerName = "ct-dna-shr-sellout"
accountName = "stctdna" + env[0] + "eus21"
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+ env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)

ruleMasterFileName_KCNA = "NA/"+ env + "/UIF_config/ParameterFiles/KROGER_M6_RULE_MASTER.json"


# COMMAND ----------

filePath = "NA/dev/Source_Files/KROGER/M6/M6_CATEGORY/Raw/Data/2023/07/26/Filtered Unzip Files/4100209.KimberlyClark.zip/comp_weekly_pos_4100209_20230724120418.fil"
dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep ="|")
display(dataDf)
# dataDf.createOrReplaceTempView('view1')
# data_file = spark.sql("select * from temp.view1" )
#print(dataDf['_c6'].unique())
#rows= dataDf.count()
#print(rows)

# COMMAND ----------

# filePath = "/NA/dev/Source_Files/KROGER/M6/M6_CATEGORY/Raw/Data/2023/06/26/34000031.KimberlyClark.zip/item_master_4000031_20230512120111.fil"
# dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep="|" )
# display(dataDf)

# COMMAND ----------

# val df = spark.read
#           .format("org.zuinnote.spark.office.excel")
#           .option("read.spark.useHeader", "true")  
# 		  .load("NA/dev/Source_Files/COSTCO/AOR_LIST_FORMAT_HIST_20230301.xlsx")

# COMMAND ----------

#filePath = "NA/dev/Source_Files/KROGER/M6/M6_CATEGORY/Raw/Data/2023/05/30/Filtered Unzip Files/4020385.KimberlyClark.zip/item_master_4020385_20230530120110.fil"
#dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=False ,sep ="|")
#display(dataDf)

# COMMAND ----------

# dataDf.count()