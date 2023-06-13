"""Entry point for the ETL application

Sample usage:
docker-compose run etl poetry run python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse \
  --table transactions
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

# Using argparse to take input for main.py
parser = argparse.ArgumentParser(description='Entry point to concept ETL Script')
parser.add_argument('--source', type=str, required=True, help='Input path to sourcefile for extrac')
parser.add_argument('--database', type=str, required=True, help='Input Database name to create in warehouse')
parser.add_argument('--table', type=str, required=True, help='Input Table name to create in --database')
args = parser.parse_args()

# Get environment variables
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
# Establish connection to PostgreSQL server
conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database='postgres',
    user=postgres_user,
    password=postgres_password
)
# Set autocommit to True
conn.autocommit = True
# Create a cursor object
cursor = conn.cursor()

# Create database if it doesn't exist
try:
    cursor.execute(f'CREATE DATABASE {args.database}')
except:
    pass

# Switch to the specified database
cursor.execute(f"SET search_path TO {args.database}")

# Define the schema for table
schema = """
    customer_id INTEGER,
    favourite_product VARCHAR,
    longest_streak INTEGER
"""

# Create the table
cursor.execute(f"CREATE TABLE IF NOT EXISTS public.{args.table} ({schema})")



# ## Get or create spark or spark://spark:7077
spark: SparkSession = SparkSession.builder\
    .appName('ETL Concept Script')\
    .master('spark://spark:7077')\
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.16')\
    .config('spark.driver.extraClassPath', '/usr/share/java/postgresql-42.6.0.jar')\
    .getOrCreate()
    

# Read the source CSV file into a DataFrame
df = spark.read.csv(args.source, sep='|', header=True, inferSchema=True)

# Spark SQL Create or replace temp view
df.createOrReplaceTempView('transaction')
df.show()

# Calculate the favourite_product and longest_streak
#####
# First select customer ID, product sold, and transaction date for each transaction
# Use window functions to calculate the days since the last transaction, 
# the row number within each customer-product group, and the row number within each customer-product-days since last transaction group
# Find the favorite product for each customer, group the transactions by customer and product and sum the quantities sold for each product
# Select only the top sold product.
# Last group by customer and product selects the maximum streak, and orders the results by longest_streak.
######


result = spark.sql("""
SELECT 
    t2.customer_id,
    t2.favourite_product,
    MAX(streak) AS longest_streak
FROM (
    SELECT 
        custId AS customer_id, 
        productSold AS favourite_product, 
        transactionDate, 
        DATEDIFF(transactionDate, lag_date) AS days_since_last,
        ROW_NUMBER() OVER (PARTITION BY custId, productSold ORDER BY transactionDate) AS rn,
        ROW_NUMBER() OVER (PARTITION BY custId, productSold, DATEDIFF(transactionDate, lag_date) ORDER BY transactionDate) AS streak,
        SUM(unitsSold) OVER (PARTITION BY custId, productSold ORDER BY transactionDate ROWS UNBOUNDED PRECEDING) AS total_sold
    FROM (
        SELECT 
            custId, 
            productSold, 
            unitsSold,
            transactionDate,
            LAG(transactionDate, 1) OVER (PARTITION BY custId, productSold ORDER BY transactionDate) AS lag_date
        FROM transaction
    ) t1
) t2
JOIN (
    SELECT 
        t3.customer_id,
        t3.productSold
    FROM (
        SELECT 
            custId AS customer_id, 
            productSold AS productSold, 
            ROW_NUMBER() OVER (PARTITION BY custId ORDER BY SUM(unitsSold) DESC) AS rn
        FROM transaction 
        GROUP BY custId, productSold
    ) t3
    WHERE t3.rn = 1
) t4 ON t2.customer_id = t4.customer_id AND t2.favourite_product = t4.productSold
GROUP BY t2.customer_id, t2.favourite_product
ORDER BY longest_streak DESC
""")

# Filter the result by customer_id=23938
test = result.filter(result.customer_id == 23938)

# Assert that the value of longest_streak is 1
longest_streak_test = test.select('longest_streak').collect()[0][0] == 1

# Assert that the value of favourite_product is 'PURA250'
favourite_product_test = test.select('favourite_product').collect()[0][0] == 'PURA250'

# Insert the test into the test table
test = test.withColumn('favourite_product_test', lit(favourite_product_test))
test = test.withColumn('longest_streak_test', lit(longest_streak_test))

# Set autocommit to True
conn.autocommit = True
# Create a cursor object
cursor = conn.cursor()

# Switch to the specified database
cursor.execute(f"SET search_path TO {args.database}")

# Define the schema for test table
schema_test = """
    customer_id INTEGER,
    favourite_product VARCHAR,
    longest_streak INTEGER,
    favourite_product_test BOOLEAN,
    longest_streak_test BOOLEAN
"""

# Create the test table
cursor.execute(f"CREATE TABLE IF NOT EXISTS public.test ({schema_test})")


# Close the cursor and connection objects
cursor.close()
conn.close()

test.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/" + args.database) \
    .option("dbtable", "test") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .mode("overwrite") \
    .save()

# Print the test result
print("Testing for customer_id=23938")
if longest_streak_test and favourite_product_test:
    print("Test result: Passed")
else:
    print("Test result: Not passed")

# Insert the result into the customers table
result.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/" + args.database) \
    .option("dbtable", args.table) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .mode("overwrite") \
    .save()




# #################################################################
# # Spark DSL example
# df.groupBy('transactionId').sum('unitsSold').show()
# #################################################################

# #################################################################
# # Spark SQL example
# df.createOrReplaceTempView('transaction')
# spark.sql("""
# select transactionId, sum(unitsSold)
# from transaction
# group by transactionId
# """).show()
# #################################################################
