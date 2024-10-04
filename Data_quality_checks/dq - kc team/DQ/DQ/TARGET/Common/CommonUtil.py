# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text('env', '')
env = dbutils.widgets.get('env')

# COMMAND ----------

print(env)

# COMMAND ----------

containerName = "ct-dna-shr-sellout"
accountName = "stctdna" + env[0] + "eus21"
directory_id = dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+ env[0]+"-1-directory-id")
url = "https://login.microsoftonline.com/"  + directory_id + "/oauth2/token"
print(accountName,directory_id,url)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1"))
spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net",dbutils.secrets.get("ct-dna-key-vault-secret-scope","sp-ct-dna-"+env[0]+"-1-pw"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", url)

ruleMasterFileName_KCNA = "NA/"+ env + "/UIF_config/ParameterFiles/RulesMaster_TARGET.json"

# COMMAND ----------

#filePath = "dev/UIF_config/ParameterFiles/KCNA/DQ/RulesMaster_NUMERATOR.json"
#dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True )

# COMMAND ----------

# filePath = "dev/Source_Files/Promo_Data/Raw/Data/2023/05/02/NMR_RETAILER_PROMOTIONS_WEEK-04282023-2022-2.txt"
# dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True )


# COMMAND ----------

#filePath = "dev/Source_Files/Promo_Data/Raw/Data/2023/05/02/NMR_RETAILER_PROMOTIONS_WEEK-04282023-2022-2.txt"
#dataDf = spark.read.csv("abfss://" + containerName + "@"+ accountName+ ".dfs.core.windows.net/" + filePath,header=True )

# COMMAND ----------

# val df = spark.read
#           .format("org.zuinnote.spark.office.excel")
#           .option("read.spark.useHeader", "true")  
# 		  .load("NA/dev/Source_Files/COSTCO/AOR_LIST_FORMAT_HIST_20230301.xlsx")

# COMMAND ----------

# dataDf.count()

# COMMAND ----------

# accountname = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME") 
# adlsSecret= dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")

# spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountname), adlsSecret)

# COMMAND ----------

# accountName_GDL = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME") 
# adlsSecret_GDL = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# adlsContainer_GDL = "ct-dna-shr-syn"

# spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(accountName_GDL), adlsSecret_GDL) #For Global Data Lake


# COMMAND ----------

# adlsSecret= dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# accountsecret = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-SECRET-KEY")
# accountname = dbutils.secrets.get("ct-dna-key-vault-secret-scope","SECRET-ADLS-STORAGE-ACCOUNT-NAME")
# containername = "ct-dna-shr-syn"