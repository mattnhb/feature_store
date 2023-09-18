from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CoalesceCalculator").getOrCreate()

sc = spark._jsc.sc()

result1 = sc.getExecutorMemoryStatus().keys()
result2 = (
    len([executor.host() for executor in sc.statusTracker().getExecutorInfos()]) - 1
)
print(result1)
print(f"{result2=}")
spark.stop()
