import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, when, lower, avg, array_contains
import pandas as pd
from pyspark.sql import SparkSession
import logging


def transform_raw_dataset(beef_recipes_raw_df: DataFrame) -> DataFrame:
    beef_recipes_filtered_df = filter_and_enrich(beef_recipes_raw_df)
    beef_recipes_filtered_grouped_df = beef_recipes_filtered_df.groupBy(
        "difficulty"
    ).agg(avg("totalCookTime").alias("avg_total_cooking_time"))
    beef_recipes_filtered_df.show()

    return beef_recipes_filtered_grouped_df


def filter_and_enrich(
    beef_recipes_raw_df: DataFrame,
    easy_recipe_lower_threshold: int = 30,
    medium_recipe_upper_threshold: int = 60,
) -> DataFrame:
    """Returns the cleansed and filtered recipe as records as DataFrame

    This routine filters the input dataset for recipes that include the ingredient beef.
    Secondly, it enriches the dataset by the determined difficulty
    which is derived by the sum of prep and cook time.
    """

    @F.pandas_udf("long")
    def parse_iso8601_duration(str_duration: pd.Series) -> pd.Series:
        return str_duration.apply(
            lambda duration: (pd.Timedelta(duration).seconds / 60)
        )

    beef_recipes_df = (
        beef_recipes_raw_df.withColumn(
            "ingredients", split(lower(col("ingredients")), "\s+")
        )
        .withColumn("prepTimeInMinutes", parse_iso8601_duration(F.col("prepTime")))
        .withColumn("cookTimeInMinutes", parse_iso8601_duration(F.col("cookTime")))
        .withColumn(
            "totalCookTime", (col("prepTimeInMinutes") + col("cookTimeInMinutes"))
        )
        .withColumn(
            "difficulty",
            (
                when(
                    col("totalCookTime") < easy_recipe_lower_threshold, "easy"
                ).otherwise(
                    when(
                        col("totalCookTime") <= medium_recipe_upper_threshold, "medium"
                    ).otherwise("hard")
                )
            ),
        )
    )

    beef_recipes_filtered_df = beef_recipes_df.filter(
        array_contains(beef_recipes_df.ingredients, "beef")
    )
    return beef_recipes_filtered_df


def main_routine(local=True):
    """
    This is the main routine of the assessment, reading the input files and applying the transformation routines.
    """

    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    try:
        if local:
            spark = (
                SparkSession.builder.master("local[1]")
                .appName("twiechert-data-engineering-test")
                .getOrCreate()
            )
        else:
            spark = SparkSession.builder.config(
                "spark.archives", "pyspark_env.tar.gz#environment"
            ).getOrCreate()

        logging.info("Spark context has been setup")

        beef_recipes_df = spark.read.json("input/*.json")
        beef_recipes_filtered_grouped_df = transform_raw_dataset(beef_recipes_df)
        beef_recipes_filtered_grouped_df.write.mode("overwrite").options(
            header="True", delimiter=","
        ).csv("output")

    except Exception as e:
        logging.error(
            "Exception occurred while running spark application", exc_info=True
        )


main_routine()
