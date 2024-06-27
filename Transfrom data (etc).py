# Databricks notebook source
# Read ChartHits1980 tabel data
df = spark.sql("SELECT * FROM ChartHits1980")



# COMMAND ----------

# Remove duplicates
df_clean = df.dropDuplicates(["ChartPosition", "ReachedNumberOne", "Artists", "Single", "RecordLabel"])


# COMMAND ----------


# Validate data types & formats
from pyspark.sql.functions import col, to_date

# Convert ReachedNumberOne to date
df_clean = df_clean.withColumn("ReachedNumberOne", to_date(col("ReachedNumberOne"), "yyyy-MM-dd"))

# Filter out rows with invalid data types
df_clean = df_clean.filter(col("ChartPosition").cast("int").isNotNull() & col("WeeksAtNumberOne").cast("int").isNotNull())



# COMMAND ----------

# Remove incomplete/wrong data
df_clean = df_clean.dropna(subset=["ChartPosition", "ReachedNumberOne", "Artists"])


# COMMAND ----------


# Data normalisation (data standaardiseren)
from pyspark.sql.functions import trim, lower

df_clean = df_clean.withColumn("Artists", lower(trim(col("Artists")))) \
                   .withColumn("Single", lower(trim(col("Single")))) \
                   .withColumn("RecordLabel", lower(trim(col("RecordLabel"))))



# COMMAND ----------


# Apply Business Rules
df_clean = df_clean.withColumn("ChartPosition", abs(col("ChartPosition")))


# COMMAND ----------

# Voorbeeld van het filteren van de data
filtered_df = df.filter(df.WeeksAtNumberOne > 2)

filtered_df.show()

# COMMAND ----------

# Slaat de gegevens op als Delta tabel voor Power Bi
filtered_df.write.format("delta").mode("overwrite").saveAsTable("filtered_chart_hits")


# COMMAND ----------

# Dit werkt helaas niet omdat Databricks mij niet de juiste machtigingen gaf.

# Configuratie voor het mounten van de Databricks storage account met SAS-token
clean_storage_account_name = "dbstorageeirwy2z2i6lyn6"
clean_container_name = "<clean-container-name>"
clean_sas_token = "<clean-sas-token>" # Heb ik niet

dbutils.fs.mount(
  source = f"abfss://{clean_container_name}@{clean_storage_account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/clean",
  extra_configs = {f"fs.azure.sas.{clean_container_name}.{clean_storage_account_name}.dfs.core.windows.net": clean_sas_token}
)

# Schrijf de verwerkte data naar de Databricks storage
filtered_df.write.format("delta").save("/mnt/clean/<path-to-save-clean-data>")

