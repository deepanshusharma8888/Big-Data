from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BigDataPipelineSim").getOrCreate()

df = spark.read.csv("people.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("people")

result = spark.sql("SELECT city, COUNT(*) as people_count FROM people GROUP BY city")

result.show()

result.write.csv("output/people_summary.csv", header=True)

spark.stop()
