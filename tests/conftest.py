import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")          # "session" = build once for the whole pytest run, not once per test
def spark():                              # the fixture's NAME is how tests ask for it: def test_x(spark)
    s = (
        SparkSession.builder
        .appName("silver_arrivals_tests")
        .master("local[1]")               # one core: tests are tiny, and fewer cores = faster startup
        .config("spark.sql.session.timeZone", "UTC")   # same rule as every Spark script in this repo
        .config("spark.sql.shuffle.partitions", "1")   # default is 200; on 5 rows that means 200 near-empty tasks per groupBy, very slow
        .getOrCreate()
    )
    yield s                               # hand the session to the tests; everything after yield runs at the end
    s.stop()                              # tear down the kitchen once all tests are done