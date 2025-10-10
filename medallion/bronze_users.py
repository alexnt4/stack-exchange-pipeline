from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# df = (
#    spark.read.format("xml")
#    .option("rootTag", "post")
#    .option("rowTag", "row")
#    .option("inferSchema", "true")
#    .load("../data/raw/Users.xml")
# )


schema = StructType(
    [
        StructField("_AboutMe", StringType(), True),
        StructField("_AccountId", LongType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_DisplayName", StringType(), True),
        StructField("_DownVotes", LongType(), True),
        StructField("_Id", LongType(), True),
        StructField("_LastAccessDate", TimestampType(), True),
        StructField("_Location", StringType(), True),
        StructField("_Reputation", LongType(), True),
        StructField("_UpVotes", LongType(), True),
        StructField("_Views", LongType(), True),
        StructField("_WebsiteUrl", StringType(), True),
    ]
)

df = (
    spark.read.format("xml")
    .option("rootTag", "users")
    .option("rowTag", "row")
    .schema(schema)
    .load("../data/raw/Users.xml")
)

new_column_names = [col[1:] for col in df.columns]
df = df.toDF(*new_column_names)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_users")
