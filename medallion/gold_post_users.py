from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()


stg_posts_df = spark.read.parquet("spark-warehouse/stg_posts")
raw_users_df = spark.read.parquet("spark-warehouse/raw_users")


def posts_users_OBT(stg_posts_df: DataFrame, raw_users_df: DataFrame) -> DataFrame:
    return (
        stg_posts_df.alias("posts")
        .withColumnRenamed("CreationDate", "PostCreationDate")
        .join(
            other=raw_users_df.withColumnRenamed(
                "CreationDate", "UserCreationDate"
            ).alias("users"),
            on=F.col("posts.OwnerUserId") == F.col("users.Id"),
            how="left",
        )
    )


marts_posts_user_df = posts_users_OBT(stg_posts_df, raw_users_df)
marts_posts_user_df.write.mode("overwrite").saveAsTable("marts_posts_users")
