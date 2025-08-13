from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("WordCountTest").getOrCreate()

    # Sample data
    data = ["hello world", "hello spark", "spark on kubernetes"]

    # Create RDD
    rdd = spark.sparkContext.parallelize(data)

    # Word count
    word_counts = (rdd.flatMap(lambda line: line.split(" "))
                      .map(lambda word: (word, 1))
                      .reduceByKey(lambda a, b: a + b))

    # Convert to DataFrame
    df = word_counts.toDF(["word", "count"])

    # Show result
    df.show()

    # Optional: write to output directory (e.g., mounted PVC or S3)
    df.write.mode("overwrite").format("csv").save("/tmp/spark-wordcount-output")

    spark.stop()

if __name__ == "__main__":
    main()
