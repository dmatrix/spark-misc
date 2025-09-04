from pyspark import pipelines as sdp
from pyspark.sql import DataFrame
import importlib.util
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Get the path to the utility module
util_path = Path(__file__).parent.parent.parent / "utils" / "oil_gen_util.py"

# Load the module using importlib.util
spec = importlib.util.spec_from_file_location("oil_gen_util", util_path)
oil_gen_util = importlib.util.module_from_spec(spec)
sys.modules["oil_gen_util"] = oil_gen_util
spec.loader.exec_module(oil_gen_util)


@sdp.materialized_view
def permian_rig_mv() -> DataFrame:
    """
    Materialized view that generates Permian Basin oil rig sensor events.
    
    Returns:
        DataFrame: DataFrame containing Permian rig sensor events from the last 2 days.
    """
    start_date = datetime.now() - timedelta(days=2)
    return oil_gen_util.create_oil_rig_events_dataframe('permian_rig', start_date, num_events=100)


@sdp.materialized_view
def eagle_ford_rig_mv() -> DataFrame:
    """
    Materialized view that generates Eagle Ford Shale oil rig sensor events.
    
    Returns:
        DataFrame: DataFrame containing Eagle Ford rig sensor events from the last 2 days.
    """
    start_date = datetime.now() - timedelta(days=2)
    return oil_gen_util.create_oil_rig_events_dataframe('eagle_ford_rig', start_date, num_events=10000) 