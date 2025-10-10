from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, MapType, IntegerType, StringType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import pyspark.sql.functions as F

builder = (
    SparkSession.builder.appName("StackExchangePipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    #                                          ^^^^^^^^^^^^^^^ Nota el .catalog. aquí
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

raw_posts_df = spark.read.parquet("spark-warehouse/raw_posts")


def split_tag_into_array(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "TagsArray", F.filter(F.split(F.col("tags"), r"\|"), lambda x: x != "")
    ).drop("Tags")


def rename_columns(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("Id", "PostId")


def map_post_type(df: DataFrame) -> DataFrame:
    map_data = [
        (1, "Question"),
        (2, "Answer"),
        (3, "Orphaned tag wiki"),
        (4, "Tag wiki excerpt"),
        (5, "Tag wiki"),
        (6, "Moderator nomination"),
        (7, "Wiki placeholder"),
        (8, "Privilege wiki"),
        (9, "Article"),
        (10, "HelpArticle"),
        (12, "Collection"),
        (13, "ModeratorQuestionnaireResponse"),
        (14, "Announcement"),
        (15, "CollectiveDiscussion"),
        (17, "CollectiveCollection"),
    ]

    map_schema = StructType(
        [
            StructField("PostTypeId", IntegerType(), False),
            StructField("PostType", StringType(), False),
        ]
    )

    map_df = spark.createDataFrame(map_data, schema=map_schema)

    return df.join(
        F.broadcast(map_df), df["PostTypeId"] == map_df["PostTypeId"], "left"
    ).drop(map_df["PostTypeId"])


stg_posts_df = (
    raw_posts_df.transform(split_tag_into_array)
    .transform(rename_columns)
    .transform(map_post_type)
)


def incremental_upsert(
    dest_table: str, df: DataFrame, unique_key: str, updated_at: str, full_refresh=False
):
    """
    Performs incremental upsert using updated_at as the cursor value with unique_key
    Doesn't support deletes, very minimal
    """
    if not spark.catalog.tableExists(dest_table) or full_refresh:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(dest_table)
        )
    else:
        last_max = (
            spark.table(dest_table)
            .agg(F.max(updated_at).alias("max_ts"))
            .collect()[0]["max_ts"]
        )

        incr_df = df.filter(F.col(updated_at) > last_max)

        if not incr_df.rdd.isEmpty():
            delta_table = DeltaTable.forName(spark, dest_table)
            (
                delta_table.alias("t")
                .merge(
                    source=incr_df.alias("s"),
                    condition=f"s.{unique_key} = t.{unique_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )


dest_table = "default.stg_posts"
incremental_upsert(dest_table, stg_posts_df, "PostId", "CreationDate")
