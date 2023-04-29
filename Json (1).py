# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/awsnly/raw/

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/awsnly/raw/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/awsnly/raw/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("topping",explode("topping"))\
    .withColumn("topping_id",col("topping.id"))\
        .withColumn("topping_type",col("topping.type"))\
            .drop("topping")\
    .withColumn("batters",explode("batters.batter"))\
    .withColumn("batter_id",col("batters.id"))\
        .withColumn("batter_type",col("batters.type"))\
            .drop("batters")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("json")

# COMMAND ----------

# MAGIC %sql
# MAGIC create view jsonview as SELECT * from json

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

df1.createOrReplaceTempView("jsontempview")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jsontempview

# COMMAND ----------

df1.createOrReplaceGlobalTempView("jsonglobaltempview")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.jsonglobaltempview

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog first

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sample.student2(id int primary key, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample.student

# COMMAND ----------


