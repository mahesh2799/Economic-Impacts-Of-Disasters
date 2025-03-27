import os
from pyspark.sql import SparkSession

# Set JAVA_HOME and HADOOP_HOME
java_home = r'C:\Program Files\Java\jdk-17.0.2'
hadoop_home = r'C:\hadoop'

# Check if the paths exist
if not os.path.exists(java_home):
    raise FileNotFoundError(f"JAVA_HOME path does not exist: {java_home}")
if not os.path.exists(os.path.join(hadoop_home, 'bin', 'winutils.exe')):
    raise FileNotFoundError(f"winutils.exe not found in HADOOP_HOME: {hadoop_home}")

# Set environment variables
os.environ['JAVA_HOME'] = java_home
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = f"{java_home}\\bin;{hadoop_home}\\bin;" + os.environ['PATH']

# Verify environment variables
print("JAVA_HOME:", os.environ['JAVA_HOME'])
print("HADOOP_HOME:", os.environ['HADOOP_HOME'])
print("PATH:", os.environ['PATH'])

# Path to the MySQL Connector/J JAR file
mysql_connector_jar = r"C:\Users\HP\Downloads\mysql-connector-j-8.4.0\mysql-connector-j-8.4.0.jar"

# Ensure the JAR file exists
if not os.path.exists(mysql_connector_jar):
    raise FileNotFoundError(f"JAR file does not exist: {mysql_connector_jar}")

# Initialize Spark session with the MySQL Connector/J
spark = SparkSession.builder \
    .appName("MariaDBIntegration") \
    .config("spark.jars", mysql_connector_jar) \
    .getOrCreate()

#Set MariaDB jdbc parameters
server = "localhost"
port = 3306
database = "foodpricesdb"
jdbc_url = f"jdbc:mysql://{server}:{port}/{database}"

# Database Credentials
user = "root"
password = "root"
jdbc_driver = "com.mysql.cj.jdbc.Driver"

# Create a data frame by reading data from MariaDB via JDBC
sql = "SELECT * FROM foodprices"
food_prices_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", sql) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbc_driver) \
    .load()

# Check food prices data
print('Food Prices shape (rows, cols):', (food_prices_df.count(), len(food_prices_df.columns)))
food_prices_df.show(5)
