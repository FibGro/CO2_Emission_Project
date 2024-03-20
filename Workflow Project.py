# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Define path for each layer 
# MAGIC

# COMMAND ----------

bronze_path = '/mnt/co2emission/bronze'
silver_path = '/mnt/co2emission/silver'
gold_path = '/mnt/co2emission/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Bronze to Silver 
# MAGIC
# MAGIC #### Snowflake Schemas 
# MAGIC
# MAGIC Save tables in the silver layer that illustrated a Snowflake Schema, it contains the following: 
# MAGIC
# MAGIC - Fact table (transactions)
# MAGIC - Several dimension tables (account, counter_party and transactin_type)
# MAGIC - Write the dataframe into tables with DELTA format
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### Supporting Documents : 
# MAGIC
# MAGIC ##### Medalion Architecture
# MAGIC * https://www.databricks.com/glossary/medallion-architecture
# MAGIC * https://docs.databricks.com/lakehouse/medallion.html
# MAGIC
# MAGIC ##### Reading Data
# MAGIC * Input / Output: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
# MAGIC * Data Source Options: https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option
# MAGIC * StructType: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType
# MAGIC * StrcutField: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructField.html#pyspark.sql.types.StructField
# MAGIC
# MAGIC
# MAGIC ##### Writing Data
# MAGIC * https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
# MAGIC * https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option
# MAGIC
# MAGIC
# MAGIC ##### MultiLine and Encoding CSV file
# MAGIC * https://spark.apache.org/docs/3.5.1/sql-data-sources-csv.html
# MAGIC
# MAGIC
# MAGIC ##### Constraints and declare the keys 
# MAGIC We will not declare the keys in our tables because  we are using the Standars version of the databricks., While the defining Primary Key, we need Unity Catalog that only available in Premium. Additionally, based on Microsoft : the keys is only informational and not enforced. 
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/tables/constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Fact Table : transactions
# MAGIC
# MAGIC Save a table called transactions in the silver layer, it should contain the following columns:
# MAGIC
# MAGIC - `account_id`: type `STRING`
# MAGIC - `balance_after_booking`: type `DOUBLE`
# MAGIC - `booking_amount`: type `DOUBLE`
# MAGIC - `booking_datetime`: type `TIMESTAMP`
# MAGIC - `booking_id`: type `STRING`
# MAGIC - `counterparty_address_line1`: type `STRING`
# MAGIC - `counterparty_name`: type `STRING`
# MAGIC - `detailed_transaction_type`: type `STRING`
# MAGIC
# MAGIC The file should be saved in SILVER container as TABLE in DELTA format
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Defining path and Scehma for the part_r_00000_CNA.csv file  
# MAGIC 2. Read the file and ensure to do some transformation: 
# MAGIC       * MultiLine = True due to mulitline record found in address column 
# MAGIC       * encoding = UTF-8 to be able to read other language (Japan)
# MAGIC 3. Perform transformations 
# MAGIC       * Drop uncessessary columns 
# MAGIC       * Rename some columns using .withColumnRenamed
# MAGIC 4. Write the file into the Silver layer and save as TABLE in DELTA format.
# MAGIC
# MAGIC

# COMMAND ----------

# Import the releveant data type
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType, TimestampType, DateType

# Define Path
transactions_path = f"{bronze_path}/part_r_00000_CNA.csv"

# Defina Schema 
transactions_schema = StructType([
    StructField("acct_ccy", StringType(), False),
    StructField("acct_id", StringType(), False),
    StructField("bal_aftr_bookg", DoubleType(), False),
    StructField("bal_aftr_bookg_nmrc", DoubleType(), False),
    StructField("bookg_amt", DoubleType(), False),
    StructField("bookg_amt_nmrc", DoubleType(), False),
    StructField("bookg_cdt_dbt_ind", StringType(), False),
    StructField("bookg_dt_tm_cet", StringType(), False),
    StructField("bookg_dt_tm_gmt", StringType(), False),
    StructField("booking_id", StringType(), False),
    StructField("card_poi_id", StringType(), True),
    StructField("cdtr_schme_id", StringType(), True),
    StructField("ctpty_acct_ccy", StringType(), True),
    StructField("ctpty_acct_id_bban", StringType(), True),
    StructField("ctpty_acct_id_iban", StringType(), True),
    StructField("ctpty_adr_line1", StringType(), False),
    StructField("ctpty_adr_line2", StringType(), True),
    StructField("ctpty_agt_bic", StringType(), True),
    StructField("ctpty_ctry", StringType(), False),
    StructField("ctpty_nm", StringType(), False),
    StructField("dtld_tx_tp", IntegerType(), False),
    StructField("end_to_end_id", StringType(), False),
    StructField("ntry_seq_nb", IntegerType(), False),
    StructField("rmt_inf_ustrd1", StringType(), False),
    StructField("rmt_inf_ustrd2", StringType(), True),
    StructField("tx_acct_svcr_ref", StringType(), False),
    StructField("tx_tp", StringType(), True),
    StructField("year_month", StringType(), False)
])


# Use multiLine to read multiline records
# Use the encoding = "UTF-8" to read various language such as Japanese 
transactions_df = spark.read.csv(
    path=transactions_path, 
    header=True, 
    schema=transactions_schema, 
    multiLine=True, 
    encoding="UTF-8")

# COMMAND ----------


# Select the column and change the column name
from pyspark.sql.functions import col, to_timestamp, to_date

transactions = transactions_df.select(
    col("bal_aftr_bookg").alias("balance_after_booking"),
    col("bookg_amt").alias("booking_amount"),
    to_timestamp("bookg_dt_tm_cet", "yyyy-MM-dd HH:mm:ss").alias("booking_datetime"),
    col("booking_id"),
    col("acct_id").alias("account_id"),
    col("ctpty_adr_line1").alias("counterparty_address_line1"),
    col("ctpty_nm").alias("counterparty_name"),
    col("dtld_tx_tp").alias("detailed_transaction_type")
)


# COMMAND ----------

transactions.display()

# COMMAND ----------

transactions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2.1.1 Write Delta Table and Store in ADLS 
# MAGIC Create databases in Databricks using a location which will allow to register the tables in the hive metastore while writing the data to ADLS.
# MAGIC
# MAGIC * https://community.databricks.com/t5/data-engineering/delta-table-storage-best-practices/td-p/31496
# MAGIC * https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html
# MAGIC
# MAGIC Here is how to create a database with location and save a dataframe as a table : 
# MAGIC 1. Create a database with location based on silver container 
# MAGIC 2. Set default database 
# MAGIC 3. Write as Delta Table -- This will be saved in the location of database. 

# COMMAND ----------

#%sql 
#DROP DATABASE emission_silver CASCADE

# COMMAND ----------

# Create a database in silver layer
spark.sql(f"CREATE DATABASE IF NOT EXISTS silver_emission LOCATION '{silver_path}' ")

# Set database
spark.sql("USE silver_emission")

# Write as delta table
transactions.write \
  .format('delta') \
  .mode('overwrite') \
  .option("overwriteSchema", "true") \
  .saveAsTable("transactions")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_emission.transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Lookup Table 1 : account
# MAGIC
# MAGIC Save a table called account in the silver layer, it should contain the following columns:
# MAGIC - account_currency, type STRING
# MAGIC - account_id, type INTERGER
# MAGIC
# MAGIC
# MAGIC The file should be saved as TABLE in DELTA format.
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Selecting the columns from transactions table
# MAGIC 2. Drop the duplicates, because we want only the distinct account_id. This will result 250 distinct account_id 
# MAGIC 3. Write the file into the Silver layer and save as TABLE in DELTA format
# MAGIC
# MAGIC

# COMMAND ----------

# Select columns from transaction 
# Drop duplicates
account = transactions_df.select(
    col("acct_ccy").alias("account_currency"),
    col("acct_id").alias("account_id"),
).dropDuplicates()

# COMMAND ----------

# writing the account as a parquet file in the silver layer
# set database 
spark.sql("use silver_emission")

# Write as delta table 
account.write \
  .format('delta')\
  .mode('overwrite')\
  .option("overwriteSchema", "true") \
  .saveAsTable("account")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### 2.3 Lookup Table 2 : counter_party
# MAGIC
# MAGIC Save a table called counter_party in the silver layer, it should contain the following columns:
# MAGIC - counterparty_account_currency, type STRING
# MAGIC - counterparty_account_id_iban, type STRING
# MAGIC - counterparty_address_line1, type STRING
# MAGIC - counterparty_country, type STRING
# MAGIC - counterparty_name, type STRING
# MAGIC - counterparty_currency_converter, type DOUBLE
# MAGIC
# MAGIC The file should be saved as TABLE in DELTA format
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Defining path and Schema for the counter_party.csv file and read it. 
# MAGIC 2. Filling up the empty row in counterparty_account_currency 
# MAGIC 3. Write the file into the Silver layer and save as TABLE in DELTA format

# COMMAND ----------

# Select the columns 
# Drop duplicate
counter_party = transactions_df.select(
    col('ctpty_ctry').alias('counterparty_country'), 
    col('ctpty_acct_ccy').alias('counterparty_account_currency'),
    col("ctpty_acct_id_iban").alias("counterparty_account_id_iban"),
    col("ctpty_adr_line1").alias("counterparty_address_line1"),
    col("ctpty_nm").alias("counterparty_name")
).dropDuplicates()


# COMMAND ----------

from pyspark.sql.functions import col, when

# Define the country-currency mapping 
country_currency_map = {
    'NL': 'EUR', 'Denmark': 'EUR', 'Hungary': 'EUR', 'France': 'EUR', 'Spain': 'EUR', 
    'Germany': 'EUR', 'Belgium': 'EUR', 'Austria': 'EUR', 'Italy': 'EUR', 
    'Japan': 'J', 'Great Britain': 'GBP', 'Canada': 'CAD', 'United States': 'USD'
}

# Create DataFrame from the country-currency mapping
country_currency_df = spark.createDataFrame(list(country_currency_map.items()), ['country', 'currency'])

# Perform the conditional mapping 
counter_party_join = counter_party.join(country_currency_df, counter_party['counterparty_country'] == country_currency_df["country"], "left") \
                             .withColumn("counterparty_account_currency", 
                                         when(col("counterparty_account_currency").isNull() & 
                                              col("counterparty_country").isin(list(country_currency_map.keys())), 
                                              country_currency_df['currency'])
                                         .otherwise(col("counterparty_account_currency"))
                                        )

# Drop the country and currency columns
counter_party = counter_party_join.drop('country', 'currency')


counter_party.display()

# COMMAND ----------

# writing the counter_party as a parquet file in the silver layer
# set database 
spark.sql("use silver_emission")

# Write as delta table 
counter_party.write \
  .format('delta')\
  .mode('overwrite')\
  .option("overwriteSchema", "true") \
  .saveAsTable("counter_party")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Table Lookup 3 : converter
# MAGIC
# MAGIC Save a table called "converter" in the silver layer. It contains the following columns:
# MAGIC
# MAGIC - detailed_transaction_type, type INTEGER
# MAGIC - Transaction_Type, type STRING
# MAGIC - Avg_Emission_Intensity, type DOUBLE
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Defining path and Scehma for each files and read it. 
# MAGIC 2. Build a converter look up tables : 
# MAGIC     * Configure the mapping dictionary to serve as a lookup table, establishing a relationship between  
# MAGIC       Detailed transaction type and Categories.
# MAGIC     * Join the mapping with the two tables of Categories and Multipliers.
# MAGIC     * Group by the Detailed transaction type and calculate the average for the included categories.
# MAGIC     * Rename columns
# MAGIC 3. Write the file into the Silver layer and save as TABLE in DELTA format
# MAGIC

# COMMAND ----------

# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# Define path 
categories_path = f"{bronze_path}/categories.csv"

# Define schema
categories_schema = StructType([
                    StructField("Transaction Type", StringType(), False),
                    StructField("Detailed transaction type (dtld_tx_tp)", IntegerType(), False)
                    ]
                    )


# Read multipliers file and save it in multipliers object
# We will set nullable, which is the third argument in StructField to False, this does not permit null values
categories=spark.read.csv(path=categories_path, header=True, schema=categories_schema)

# COMMAND ----------

# Define path 
multipliers_path = f"{bronze_path}/multipliers.csv"

# Define Schema
multipliers_schema = StructType([
                    StructField("No", IntegerType(), False),
                    StructField("Categories", StringType(), False),
                    StructField("Emission Intensity (gCO2e/EUR)", DoubleType(), False)
                    ]
                    )

# Read multipliers file and save it in multipliers object. 
multipliers=spark.read.csv(path=multipliers_path, header=True, schema=multipliers_schema)

# COMMAND ----------

# Import avg 
from pyspark.sql.functions import avg

# Define the mapping between transaction type and categories
mapping = {
    1111: ['Financial Services'],
    2222: ['Financial Services'],
    3333: ['Financial Services'],
    4111: ['Electric Vehicles', 'E-Mobility', 'Local Public Transport', 'Train Travel', 'Taxi Cabs & Limousines'],
    4112: ['Train Travel'],
    4121: ['Taxi Cabs & Limousines'],
    4131: ['Local Public Transport'],
    4511: ['Air Travel'],
    5411: ['Alcoholic Beverages', 'Non-Alcoholic Beverages', 'Groceries - Food', 'Convenience Stores'],
    5420: ['Alcoholic Beverages', 'Non-Alcoholic Beverages', 'Groceries - Food', 'Catering'],
    5422: ['Catering', 'Groceries - Food'],
    5462: ['Bakeries & Cafés'],
    5541: ['Service Stations', 'Operation of Personal Transport Equipment', 'Electric Vehicles'],
    5542: ['Service Stations'],
    5912: ['Convenience Stores', 'Alcoholic Beverages'],
    6542: ['Energy'],
    6630: ['Water Supply and Miscellaneous Services'],
    7512: ['Car Rental']
}


# Convert the mapping into a DataFrame
mapping_data = [(k, v) for k, vs in mapping.items() for v in vs]
mapping_df = spark.createDataFrame(mapping_data, ['Detailed transaction type (dtld_tx_tp)', 'Categories'])

# Join the DataFrames based on the mapping, multipliers and categories 
# Ensure the join mapping_df and categories with right join, to ensure the type is based on the categories table. 
joined_df = mapping_df.join(multipliers, on='Categories') \
                      .join(categories, on='Detailed transaction type (dtld_tx_tp)', how='right')

# Calculate the average emission intensity for each transactions type and save as converter
from pyspark.sql.functions import avg

converter = joined_df \
    .groupBy('Detailed transaction type (dtld_tx_tp)', 'Transaction Type')\
    .agg(avg('Emission Intensity (gCO2e/EUR)').alias('Avg_Emission_Intensity'))

# Rename Column
converter = converter \
    .withColumnRenamed("Detailed transaction type (dtld_tx_tp)", "detailed_transaction_type") \
    .withColumnRenamed("Transaction Type", "Transaction_Type")



# COMMAND ----------

mapping_df.display()

# COMMAND ----------

converter.display()

# COMMAND ----------

# set database
spark.sql("use silver_emission")

# Write as delta table 
converter.write \
  .format('delta')\
  .mode('overwrite')\
  .option("overwriteSchema", "true") \
  .saveAsTable("converter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Table Lookup 4 : currency
# MAGIC
# MAGIC Save a table called "currency" in the silver layer. It contains the following columns:
# MAGIC
# MAGIC - account_currency, type STRING
# MAGIC - currency_converter, type DOUBLE
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Defining path and Scehma for each files and read it. 
# MAGIC 2. Write the file into the Silver layer and save as TABLE in DELTA format

# COMMAND ----------

# Import the releveant data type
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# Create path and schema 
currency_path = f"{bronze_path}/currency.csv"

currency_schema = StructType([
                    StructField("acct_ccy", StringType(), False),
                    StructField("currency_converter", DoubleType(), False)
                    ]
                    )

# Create a dataframe
currency = spark.read.csv(path=currency_path, header=True, schema=currency_schema)

# Change the column name 
currency = currency.withColumnRenamed("acct_ccy", "account_currency")

# COMMAND ----------

# set database
spark.sql("use silver_emission")

# Write as delta table 
currency.write \
  .format('delta')\
  .mode('overwrite')\
  .option("overwriteSchema", "true") \
  .saveAsTable("currency")

# COMMAND ----------

currency.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Table Lookup 5 : datetable
# MAGIC
# MAGIC Save a table called "datetable" in the silver layer. It contains the following columns:
# MAGIC
# MAGIC - booking_datetime, type timestamp
# MAGIC - month, type string
# MAGIC - year, type int
# MAGIC - day, type string
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC 1. Defining path and Scehma for each files and read it. 
# MAGIC 2. Create a new column for month, year and day based on the booking_time_cet
# MAGIC 3. Write the file into the Silver layer and save as TABLE in DELTA format

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, to_date, to_timestamp, date_format, cast


# Convert timestamp column to a timestamp type
datetable = transactions_df.select(
    to_timestamp("bookg_dt_tm_cet", "yyyy-MM-dd HH:mm:ss").alias("booking_datetime"))

# Drop duplicates 
datetable = datetable.dropDuplicates(["booking_datetime"])

# Extracting date, month, year, and day
datetable = datetable.withColumn("date", to_date("booking_datetime")) \
                     .withColumn("month", month("booking_datetime").cast("int")) \
                     .withColumn("year", year("booking_datetime").cast("int")) \
                     .withColumn("day_of_week", date_format("booking_datetime", "EEEE"))

# Show the DataFrame
datetable.display()


# COMMAND ----------

# set database
spark.sql("use silver_emission")

# Write as delta table 
datetable.write \
  .format('delta')\
  .mode('overwrite')\
  .option("overwriteSchema", "true") \
  .saveAsTable("datetable")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Silver to Gold
# MAGIC
# MAGIC * Create a table (analysis_emission) consists all lookup and fact tables and save it in Gold layer. This table can be used for further analysis by data analyst. 
# MAGIC * Perform analysis using aggregation and visualization (charts)
# MAGIC * Save the table of aggregation in gold layer using SQL Syntax (can be also using Python)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Create a table that is a combination of lookup and fact tables, which can be used for further analysis.

# COMMAND ----------


# Read delta tables and save as DataFrame
path = f"{silver_path}/transactions"
transactions = spark.read.format('delta').load(path)

path = f"{silver_path}/account"
account = spark.read.format('delta').load(path)

path = f"{silver_path}/converter"
converter = spark.read.format('delta').load(path)

path = f"{silver_path}/counter_party"
counter_party = spark.read.format('delta').load(path)

path = f"{silver_path}/currency"
currency = spark.read.format('delta').load(path)

path = f"{silver_path}/datetable"
datetable = spark.read.format('delta').load(path)

# Import Function F
from pyspark.sql import functions as F

# Join currency and account
account = account.join(currency, "account_currency", "left") \
                 .select("account_currency", 
                         "account_id", 
                         currency["currency_converter"].alias("account_currency_converter"))
                 
# Join transactions with account, counter_party, and converter
joined_df = transactions.join(account, ["account_id"], "left") \
                       .join(converter, "detailed_transaction_type", "left") \
                       .join(counter_party, ["counterparty_address_line1", "counterparty_name"], "left") \
                       .join(datetable, ["booking_datetime"], "left")


# Add column "Quantity_Emission" 
analysis_emission = joined_df.withColumn("Quantity_Emission_Account", 
                                         F.round((F.col("booking_amount")) * F.col("Avg_Emission_Intensity") * F.col("account_currency_converter"), 2))

analysis_emission.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2 Create a database and save the underlaying data in gold layer

# COMMAND ----------

# Create a database in the gold layer
spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_emission LOCATION '{gold_path}'")


# COMMAND ----------


# Save the tables in the gold layer

# set database 
spark.sql("use gold_emission")

# Write as delta table 
analysis_emission.write \
  .format('delta') \
  .mode('overwrite') \
  .option("overwriteSchema", "true") \
  .saveAsTable("analysis_emission")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check the table information (MANAGED and Location is in gold layer)
# MAGIC DESCRIBE EXTENDED gold_emission.analysis_emission

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3 Let's do some analysis and visualization 
# MAGIC
# MAGIC * Visualization 1  : Bar Chart Transaction Type:  versus Total CO2 Emission
# MAGIC * Visualization 2  : Bar Chart Stack : Total emission each transaction type for every counterparty country 
# MAGIC * Visualization 3  : Pivot Table : Total number of transaction based on account and counterparty account  
# MAGIC * Visualization 4  : Line Chart : Timeline of total CO2 emission 

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE gold_emission;
# MAGIC
# MAGIC -- Show the total emmison based on transaction type and year_month
# MAGIC SELECT Transaction_Type, 
# MAGIC        account_currency,
# MAGIC        counterparty_country,
# MAGIC        counterparty_account_currency,
# MAGIC        date,
# MAGIC        ROUND(SUM(Quantity_Emission_Account)/1000000,2) AS total_emission_million_gram
# MAGIC FROM analysis_emission
# MAGIC GROUP BY Transaction_Type, account_currency, counterparty_country, counterparty_account_currency,date
# MAGIC ORDER BY total_emission_million_gram DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4 Let's try using SQL syntax to create a table and save it in gold layer.. should be fun.. :) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the gold_emission database
# MAGIC USE gold_emission;
# MAGIC
# MAGIC -- Drop the original table (try to overwrite it) 
# MAGIC DROP TABLE IF EXISTS analysis_emission_chart;
# MAGIC
# MAGIC -- Create a table 
# MAGIC CREATE TABLE IF NOT EXISTS analysis_emission_chart (
# MAGIC     Transaction_Type VARCHAR(255),
# MAGIC     account_currency VARCHAR(255),
# MAGIC     counterparty_country VARCHAR(255),
# MAGIC     counterparty_account_currency VARCHAR(255),
# MAGIC     date DATE,
# MAGIC     total_emission_million_gram DECIMAL(18,2)
# MAGIC );
# MAGIC
# MAGIC -- Insert data to the table
# MAGIC INSERT INTO analysis_emission_chart (Transaction_Type, account_currency, counterparty_country, counterparty_account_currency, date, total_emission_million_gram)
# MAGIC SELECT Transaction_Type, 
# MAGIC        account_currency,
# MAGIC        counterparty_country,
# MAGIC        counterparty_account_currency,
# MAGIC        date,
# MAGIC        ROUND(SUM(Quantity_Emission_Account)/1000000,2) AS total_emission_million_gram
# MAGIC FROM analysis_emission
# MAGIC GROUP BY Transaction_Type, account_currency, counterparty_country, counterparty_account_currency, date
# MAGIC ORDER BY total_emission_million_gram DESC;
# MAGIC
# MAGIC
# MAGIC -- Select and display data
# MAGIC SELECT * FROM analysis_emission_chart;
# MAGIC
