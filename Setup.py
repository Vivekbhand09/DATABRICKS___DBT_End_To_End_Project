# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data`