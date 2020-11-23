from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F

if __name__ == "__main__":
    print("Real-Time Data Pipeline Started ...")

spark = SparkSession \
    .builder \
    .appName("Batch Processing Started") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Created")

input_path = "C:/Users/admin/Desktop/RawData.csv"
output_path = "F:/OutputPath/OP1"
input_df = spark.read.csv(input_path, header=True)
input_df.cache()
input_df.show()

output_df1 = input_df.filter(F.col("TimeStamp").between('11:59:00', '12:14:00'))

output_df1.write.option("header", "true") \
     .mode("overwrite") \
     .partitionBy("TimeStamp")\
    .text(output_path)
print("Finished Execution")
#output_df2 = input_df.filter(F.col("TimeStamp").between('12:14:00', '12:30:00'))
# output_df = input_df.withColumn("TimeStamp",).cast(TimestampType)
# output_df.write.option("header", "true") \
#     .mode("overwrite") \
#     .partitionBy("TimeStamp") \
#     .csv(output_path)
# print("Finished Execution")
