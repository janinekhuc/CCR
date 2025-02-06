# Databricks notebook source
# MAGIC %md
# MAGIC # (Py)spark introduction
# MAGIC
# MAGIC **What is Apache Spark?**
# MAGIC Apache Spark is an open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. It is designed to process large-scale data efficiently.
# MAGIC
# MAGIC **How Spark Works Under the Hood**
# MAGIC - Cluster Computing: Spark runs on a cluster of machines, distributing data and computations across multiple nodes.
# MAGIC - In-Memory Processing: Spark processes data in memory, which makes it much faster than traditional disk-based processing frameworks like Hadoop MapReduce.
# MAGIC - Resilient Distributed Datasets (RDDs): Spark's core abstraction is the RDD, which represents a distributed collection of objects that can be processed in parallel.
# MAGIC
# MAGIC **SQL vs. PySpark**
# MAGIC Spark provides two main APIs for data processing: SQL and PySpark (the Python API for Spark).
# MAGIC
# MAGIC **SQL in Spark**
# MAGIC - Declarative Language: SQL is a declarative language, meaning you specify what you want to do with the data, and the underlying engine determines how to do it.
# MAGIC - Ease of Use: SQL is widely known and easy to use for those familiar with relational databases.
# MAGIC - Integration: Spark SQL integrates with various data sources, including Hive, Avro, Parquet, ORC, JSON, and JDBC.
# MAGIC - Optimization: Spark SQL uses the Catalyst optimizer to automatically optimize queries for better performance.
# MAGIC
# MAGIC **PySpark**
# MAGIC - Imperative Language: PySpark is an imperative language, meaning you specify how to perform operations step by step.
# MAGIC - Flexibility: PySpark provides more flexibility and control over data transformations and actions.
# MAGIC - Python Integration: PySpark allows you to leverage the full power of Python, including libraries like Pandas, NumPy, and SciPy.
# MAGIC - Custom Functions: PySpark supports User-Defined Functions (UDFs) for custom transformations.
# MAGIC
# MAGIC **Benefits of Using SQL in Spark**
# MAGIC - Simplicity: SQL is easy to learn and use, especially for those with a background in relational databases.
# MAGIC - Readability: SQL queries are often more readable and concise than equivalent PySpark code.
# MAGIC - Optimization: The Catalyst optimizer can automatically optimize SQL queries for better performance.
# MAGIC
# MAGIC **Benefits of Using PySpark**
# MAGIC - Flexibility: PySpark provides more control over data transformations and allows for complex operations that may be difficult to express in SQL.
# MAGIC - Python Ecosystem: PySpark allows you to use Python libraries and integrate with other Python code.
# MAGIC - Custom Functions: PySpark supports UDFs for custom transformations and operations.
# MAGIC
# MAGIC Resources: 
# MAGIC - [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
# MAGIC - [Databricks Documentation](https://docs.databricks.com/)
# MAGIC --------------------------
# MAGIC
# MAGIC Basic dataframe opeartions used in amif cdc
# MAGIC - Selecting Columns: select
# MAGIC - Filtering Rows: filter, withColumn
# MAGIC - Joining DataFrames: join
# MAGIC - Window Functions: Window.partitionBy, F.row_number().over
# MAGIC - Creating New Columns: withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic dataframe operations

# COMMAND ----------

# Creating dataframes
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema=columns)
df.show()

# COMMAND ----------

# Reading data from files
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe Transformations and actions
# MAGIC
# MAGIC ### Transformations
# MAGIC Transformations are operations that describe how data should be transformed. They are lazy, meaning they do not execute immediately. Instead, they build up a logical plan of transformations that Spark will execute when an action is called.

# COMMAND ----------

# Select columns
df.select("Name", "Age").show()

# Filter rows
df.filter(df.Age > 30).show()

# Group by and aggregate
df.groupBy("Name").agg(avg("Age")).show()


# COMMAND ----------

# Joining dataframes on a column
data1 = [("Alice", 34), ("Bob", 45)]
data2 = [("Alice", "F"), ("Bob", "M")]

df1 = spark.createDataFrame(data1, ["Name", "Age"])
df2 = spark.createDataFrame(data2, ["Name", "Gender"])

df_joined = df1.join(df2, on="Name", how="inner")
df_joined.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actions
# MAGIC Actions are operations that trigger the execution of the transformations. They return a result to the driver program or write data to an external storage system.

# COMMAND ----------

# Actions
df.show()

# Collect data
data = df.collect()

# Count rows
count = df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding sparks lazy evaluation
# MAGIC
# MAGIC **What is Lazy Evaluation?**
# MAGIC
# MAGIC - In Spark, transformations are lazy, meaning they do not execute immediately. Instead, they build up a logical plan of transformations that Spark will execute when an action is called.
# MAGIC - This allows Spark to optimize the execution plan and improve performance.
# MAGIC
# MAGIC **Benefits of Lazy Evaluation**
# MAGIC
# MAGIC - Optimization: Spark can optimize the execution plan by combining transformations and minimizing data shuffling.
# MAGIC - Efficiency: Reduces the number of passes over the data, improving performance.
# MAGIC - Fault Tolerance: Allows Spark to recover from failures by recomputing only the necessary transformations.

# COMMAND ----------

# Define transformations (lazy)
df_filtered = df.filter(df.Age > 30)
df_transformed = df_filtered.withColumn("NewAge", col("Age") + 1)

# No execution happens until an action is called
# Call an action to trigger execution
df_transformed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schemas
# MAGIC
# MAGIC - Definition: A schema defines the structure of a DataFrame, including column names and data types.
# MAGIC - Importance: Schemas ensure data consistency and help Spark optimize query execution.
# MAGIC

# COMMAND ----------

# Manual schema definition 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.printSchema()

# COMMAND ----------

# Automatic schema definition
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df.printSchema()

# COMMAND ----------

# Define schema for csv file and read data
import pyspark.sql.types as T

schema = T.StructType([
    T.StructField(name="Name", dataType=T.StringType(), nullable=True),
    T.StructField("Age", T.IntegerType(), True)
])

df = spark.read.csv("path/to/file.csv", header=True, schema=schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debugging in PySpark
# MAGIC

# COMMAND ----------

# Common Debugging Techniques
# Print Schema: Understand the structure of your DataFrame.
df.printSchema()

# Show Data: Display a few rows to verify transformations.
df.show(5)

# Explain Plan: Understand the execution plan of your query.
df.explain()