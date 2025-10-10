from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()


stg_posts_df = spark.read.parquet("spark-warehouse/stg_posts")


def posts_top_tags(stg_posts_df: DataFrame) -> DataFrame:
    return (
        stg_posts_df.withColumn("tag_exploded", F.explode("TagsArray"))
        .groupBy("tag_exploded")
        .agg(F.approx_count_distinct("PostId").alias("tags_count"))
        .orderBy(F.col("tags_count").desc())
    )


marts_top_tags_df = posts_top_tags(stg_posts_df)
marts_top_tags_df.write.mode("overwrite").saveAsTable("marts_top_tags")
