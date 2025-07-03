from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, spark_partition_id, count, concat, lit
)
import random

spark = SparkSession.builder.getOrCreate()

# string = ["This is a test to create and work with rdd"]

# rd = spark.sparkContext.parallelize(string)

# flatted = rd.flatMap(lambda x: x.split(' ')) \
#     .map(lambda x: (x,1)) \
#         .reduceByKey(lambda x,y: x+y) \
#             .sortBy(lambda x: -x[1]).collect()

# df = spark.createDataFrame(flatted,['word','count'])

# df.printSchema()
# df.show()
spark.conf.set('spark.sql.shuffle.partitions',7)

df_range = spark.range(0,100000).withColumn('id',monotonically_increasing_id())

df_range.printSchema()
# print(f'Count of the {df_range.count()}')
# df_range.show(5)

df_range = df_range.withColumn('partition_id',spark_partition_id())
df_range = df_range.withColumn('salt_id',concat(col('id'),lit('_'),lit(random.randint(0,7))))

df_range.show(10)
# (
#     df_range.groupBy('partition_id').agg(count('id').alias('count_of_ids'))
# ).show(10)


# df.write.mode("overwrite").partitionBy('count').parquet('/home/hari/python-notebooks/output_data/rdd_py/')

spark.stop()