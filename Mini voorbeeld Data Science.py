# Databricks notebook source
# Laad de gegevens uit de SQL-tabel in een DataFrame
df = spark.sql("SELECT * FROM ChartHits1980")

# Toon de eerste paar rijen van de DataFrame
df.show()


# COMMAND ----------

# Bekijk de schema van de DataFrame
df.printSchema()


# COMMAND ----------

# Basisstatistieken van de DataFrame
df.describe().show()


# COMMAND ----------

# Filter de data (bijvoorbeeld alleen de hits met meer dan 4 weken op nummer 1)
filtered_df = df.filter(df.WeeksAtNumberOne > 4)
filtered_df.show()


# COMMAND ----------

import matplotlib.pyplot as plt

# Converteer DataFrame naar Pandas DataFrame voor visualisatie
pdf = df.toPandas()

# Maak een plot
plt.figure(figsize=(10, 5))
plt.bar(pdf['Artists'], pdf['WeeksAtNumberOne'])
plt.xlabel('Artists')
plt.ylabel('Weeks at Number One')
plt.title('Weeks at Number One by Artist')
plt.xticks(rotation=90)
plt.show()


# COMMAND ----------

# Maak een bar chart met Spark DataFrame API
display(df.groupBy("Artists").sum("WeeksAtNumberOne").orderBy("sum(WeeksAtNumberOne)", ascending=False))


# COMMAND ----------

# Mini voorbeeld machine learning

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Voorbeeld feature engineering
assembler = VectorAssembler(inputCols=["ChartPosition"], outputCol="features") #(Om meerdere kolommen samen te voegen tot 1 vector kolom)
data = assembler.transform(df)

# Maak een lineaire regressie model
lr = LinearRegression(featuresCol='features', labelCol='WeeksAtNumberOne')
lr_model = lr.fit(data)

# Samenvatting van het model
training_summary = lr_model.summary
print("RMSE: %f" % training_summary.rootMeanSquaredError)
print("r2: %f" % training_summary.r2)


# COMMAND ----------

# Sla de gefilterde data op als een nieuwe Delta-tabel
filtered_df.write.format("delta").saveAsTable("filtered_chart_hits")

