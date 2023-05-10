import pytest
from main import filter_and_enrich
from chispa.dataframe_comparer import assert_df_equality


@pytest.mark.usefixtures("spark_session")
def test_beef_filtering(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ("beef sugar, ", "PT45M", "PT45M"),
            ("Beef salad tomatoe", "PT45M", "PT45M"),
            ("salt flour", "PT45M", "PT45M"),
            ("mustard egg", "PT45M", "PT45M"),
        ],
        ["ingredients", "prepTime", "cookTime"],
    )
    new_df = filter_and_enrich(test_df)
    assert new_df.count() == 2


@pytest.mark.usefixtures("spark_session")
def test_difficulty_classification(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ("beef sugar", "PT20M", "PT9M"),
            ("Beef salad tomatoe", "PT15M", "PT15M"),
            ("salt flour", "PT45M", "PT15M"),
            ("mustard egg", "PT45M", "PT45M"),
        ],
        ["ingredients", "prepTime", "cookTime"],
    )

    expected_data = [
        (["beef", "sugar"], "PT20M", "PT9M", 20, 9, 29, "easy"),
        (
            ["beef", "salad", "tomatoe"],
            "PT15M",
            "PT15M",
            15,
            15,
            30,
            "medium",
        ),
    ]

    actual_df = filter_and_enrich(test_df)
    expected_df = spark_session.createDataFrame(
        expected_data,
        [
            "ingredients",
            "prepTime",
            "cookTime",
            "prepTimeInMinutes",
            "cookTimeInMinutes",
            "totalCookTime",
            "difficulty",
        ],
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
