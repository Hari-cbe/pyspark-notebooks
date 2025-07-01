from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, lit, expr
import random

# Start SparkSession
spark = SparkSession.builder \
    .appName("SkewedDataFrame") \
    .getOrCreate()

# Number of rows
num_rows = 100_000

# Create skewed values: 95% of rows will have key 'A', 5% will be distributed across 'B' to 'E'
def generate_skewed_key():
    return random.choices(['A', 'B', 'C', 'D', 'E'], weights=[95, 1.25, 1.25, 1.25, 1.25])[0]

# Generate skewed data
skewed_data = [
    (
        generate_skewed_key(),
        random.randint(1, 100),
        random.random(),
        f"user_{random.randint(1, 1000)}",
        random.choice(['US', 'UK', 'IN', 'DE']),
        random.choice(['mobile', 'desktop']),
        random.randint(1000, 9999),
        random.uniform(10.5, 500.5),
        random.choice(['Y', 'N']),
        random.randint(1, 5)
    )
    for _ in range(num_rows)
]

# Define column names
columns = [
    "skewed_key",  # Skewed column
    "int_col1",
    "float_col2",
    "user_id",
    "country",
    "device",
    "session_id",
    "purchase_amt",
    "subscribed",
    "rating"
]

# Create DataFrame
df = spark.createDataFrame(skewed_data, schema=columns)

# Optional: Check distribution
df.groupBy("skewed_key").count().orderBy("count", ascending=False).show()

# Show schema and sample
df.printSchema()
df.show(5)

#writing DF
df.write.mode("overwrite").csv('gs://lrnskeweddata/skewesData/')
