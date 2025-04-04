from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize a Spark session
spark = SparkSession.builder.appName("AvgTemperature").getOrCreate()

# Load the CSV file into a DataFrame
file_path = "avg_temperature.csv"  # Update this path if necessary
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Calculate the average temperature
avg_temp = df.select(avg("Average Temperature (°C)").alias("Yearly Avg Temperature"))

# Show the result
avg_temp.show()

# Stop the Spark session
spark.stop()
