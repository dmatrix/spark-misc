import os
import sys
sys.path.append('.')
import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession

from sklearn.datasets import make_regression
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import pandas as pd
from pyspark.sql.functions import pandas_udf, col


if __name__ == "__main__":
    spark = None
    # Create a new session with Spark Connect mode={"dbconnect", "connect", "classic"}
    if len(sys.argv) <= 1:
        args = ["dbconnect", "classic", "connect"]
        print(f"Command line must be one of these values: {args}")
        sys.exit(1)  
    mode = sys.argv[1]
    print(f"++++ Using Spark Connect mode: {mode}")
    
    # create Spark Connect type based on type of SparkSession you want
    if mode == "dbconnect":
        cluster_id = os.environ.get("clusterID")
        assert cluster_id
        spark = spark = DatabrckSparkSession().get()
    else:
        spark = SparkConnectSession(remote="local[*]", mode=mode,
                                app_name="PySpark Scikit-learn Example 3").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    NUM_SAMPLES = 250_000
    # Step 2: Generate a large synthetic dataset with 10,000 rows and 10 features
    X, y = make_regression(n_samples=NUM_SAMPLES, n_features=10, noise=0.1, random_state=42)

    # Convert to Pandas DataFrame
    data = pd.DataFrame(X, columns=[f"feature_{i}" for i in range(10)])
    data["target"] = y

    # Step 3: Convert the Pandas DataFrame to a PySpark DataFrame
    spark_df = spark.createDataFrame(data)

    # Step 4: Split the data into train and test sets
    # Convert PySpark DataFrame back to Pandas for scikit-learn compatibility
    pandas_df = spark_df.toPandas()
    X = pandas_df.drop(columns=["target"])
    y = pandas_df["target"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Step 5: Train a Ridge regression model using scikit-learn
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)

    # Step 6: Perform inference
    y_pred = model.predict(X_test)

    # Step 7: Evaluate the model
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse:.2f}")

    # Step 8: Convert predictions back to PySpark DataFrame for Spark compatibility
    predictions_df = pd.DataFrame({
        "Actual": y_test.reset_index(drop=True),
        "Predicted": y_pred
    })

    spark_predictions_df = spark.createDataFrame(predictions_df)

    # Show predictions in PySpark DataFrame
    spark_predictions_df.show(10)

     # Define Pandas UDF to compute accuracy column for each row in the spark_predictions DataFrame
    @pandas_udf("float")
    def compute_accuracy(actual, predicted):
        """
        Computes the accuracy percentage for each row.
        Accuracy = 100 - (|Actual - Predicted| / |Actual|) * 100
        Handles cases where Actual = 0 by returning 0 accuracy.
        """
        accuracy = 100 - ((abs(actual - predicted) / (abs(actual) + 1e-10)) * 100)
        return accuracy
    
    # Step 9: Apply Pandas UDF to compute accuracy for each row
    spark_predicions_accuracy_df = spark_predictions_df.withColumn("Accuracy %", compute_accuracy(col("Actual"), col("Predicted")))
    spark_predicions_accuracy_df.show()

    # Stop the Spark session
    spark.stop()