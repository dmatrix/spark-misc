"""
ChatGPT, CodePilot, and docs used to generate code sample for testing
"""
from pyspark.sql import SparkSession
import pandas as pd
from sklearn.datasets import load_diabetes
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from pyspark.sql.functions import pandas_udf, col

if __name__ == "__main__":
    # Step 1: Initialize Spark Session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Scikit-learn Example 1") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")


    # Step 2: Load the dataset using scikit-learn
    diabetes_data = load_diabetes(as_frame=True)
    data = diabetes_data.data
    target = diabetes_data.target

    # Combine the features and target into a single Pandas DataFrame
    data["target"] = target

    # Step 3: Convert the Pandas DataFrame to PySpark DataFrame
    spark_df = spark.createDataFrame(data)

    # Step 4: Split the data into train and test sets
    # Convert PySpark DataFrame back to Pandas for scikit-learn compatibility
    pandas_df = spark_df.toPandas()
    X = pandas_df.drop(columns=["target"])
    y = pandas_df["target"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Step 5: Train a linear regression model using scikit-learn
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Step 6: Perform inference
    y_pred = model.predict(X_test)

    # Step 7: Evaluate the model
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse:.2f}")

    # Step 8: Convert predictions back to PySpark DataFrame for Spark compatibility
    predictions_df = pd.DataFrame({
        "Actual": y_test,
        "Predicted": y_pred
    })

    spark_predictions_df = spark.createDataFrame(predictions_df)

    # Show predictions in PySpark DataFrame
    spark_predictions_df.show()

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