# Databricks notebook source
#service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.blobstorey.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.blobstorey.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.blobstorey.dfs.core.windows.net", "8be1b78c-d170-4dc7-b7a5-cb46820a6907")
spark.conf.set("fs.azure.account.oauth2.client.secret.blobstorey.dfs.core.windows.net", "sN~8Q~aJkA7hsuyES79omlrxgxYyt7TRBJl5ydj-")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.blobstorey.dfs.core.windows.net", "https://login.microsoftonline.com/3a5e2629-6803-439e-b5b3-8160f2773b05/oauth2/token")

# COMMAND ----------

df=spark.read.csv("abfs://raw@blobstorey.dfs.core.windows.net/Sample_Crime_Data_from_2020_to_Present.csv")

# COMMAND ----------

df.write.format("parquet").saveAsTable("sample_crime_data")

# COMMAND ----------

sample_crime_data="Sample_Crime_Data_from_2020_to_Present_csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `sample_crime_data`

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.blobstorey.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.blobstorey.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.blobstorey.dfs.core.windows.net", "?sv=2021-12-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2023-03-15T10:32:48Z&st=2023-03-15T02:32:48Z&spr=https&sig=7oGiNRWgUbo0btV0%2FooAioEMNSNmotgqMC11OkhtfXY%3D")

# COMMAND ----------

df1=spark.read.csv("abfs://raw@blobstorey.dfs.core.windows.net/Sample_Crime_Data_from_2020_to_Present.csv")
