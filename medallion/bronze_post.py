from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# df = (
#    spark.read.format("xml")
#    .option("rootTag", "post")
#    .option("rowTag", "row")
#    .option("inferSchema", "true")
#    .load("../data/raw/Posts.xml")
# )


schema = StructType(
    [
        StructField("_AcceptedAnswerId", LongType(), True),
        StructField("_AnswerCount", LongType(), True),
        StructField("_Body", StringType(), True),
        StructField("_ClosedDate", TimestampType(), True),
        StructField("_CommentCount", LongType(), True),
        StructField("_CommunityOwnedDate", TimestampType(), True),
        StructField("_ContentLicense", StringType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_FavoriteCount", LongType(), True),
        StructField("_Id", LongType(), True),
        StructField("_LastActivityDate", TimestampType(), True),
        StructField("_LastEditDate", TimestampType(), True),
        StructField("_LastEditorDisplayName", StringType(), True),
        StructField("_LastEditorUserId", LongType(), True),
        StructField("_OwnerDisplayName", StringType(), True),
        StructField("_OwnerUserId", LongType(), True),
        StructField("_ParentId", LongType(), True),
        StructField("_PostTypeId", LongType(), True),
        StructField("_Score", LongType(), True),
        StructField("_Tags", StringType(), True),
        StructField("_Title", StringType(), True),
        StructField("_ViewCount", LongType(), True),
    ]
)


df = (
    spark.read.format("xml")
    .option("rootTag", "posts")
    .option("rowTag", "row")
    .schema(schema)
    .load("../data/raw/Posts.xml")
)

new_column_names = [col[1:] for col in df.columns]
df = df.toDF(*new_column_names)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_posts")
