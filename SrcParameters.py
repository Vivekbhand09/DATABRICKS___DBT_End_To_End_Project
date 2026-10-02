# Databricks notebook source
src_array=[
    {"src":"airports"},
    {"src":"bookings"},
    {"src":"flights"},
    {"src":"customers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key",value=src_array)