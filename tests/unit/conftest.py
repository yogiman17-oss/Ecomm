import os
import sys

import pytest

# Make src/pipeline (which holds ecommerce_lib) importable regardless of where pytest is launched.
_PIPELINE_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src", "pipeline"))
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def raw(spark):
    from ecommerce_lib.synthetic import generate_dataframes

    return generate_dataframes(spark)
