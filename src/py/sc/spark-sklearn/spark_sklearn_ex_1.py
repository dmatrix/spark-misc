from pyspark.sql import SparkSession
from spark_sklearn import GridSearchCV
from sklearn.datasets import make_regression
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import pandas as pd

if __name__ == "__main__":
# Step 1: Initialize Spark Session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark and spark-sklearn Distributed Training with SparkSession") \
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
    
    #
    # NOTE: THIS WON'T WORK WITH SPARK CONNECT SINCE SPARK-CONTEXT IS
    # NOT AVAILABLE -- jules
    # Access sparkContext from SparkSession
    sc = spark.sparkContext

    # Step 2: Generate a large synthetic dataset with 10,000 rows and 10 features
    X, y = make_regression(n_samples=10000, n_features=10, noise=0.1, random_state=42)

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

    # Step 5: Define a scikit-learn pipeline with scaling and Ridge regression
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("ridge", Ridge())
    ])

    # Step 6: Set up hyperparameter tuning using GridSearchCV
    param_grid = {
        "ridge__alpha": [0.1, 1.0, 10.0]
    }

    # Use spark-sklearn's GridSearchCV to distribute the training using SparkSession's sparkContext
    grid_search = GridSearchCV(sc, pipeline, param_grid, cv=3)
    grid_search.fit(X_train, y_train)

    # Step 7: Perform inference using the best model
    best_model = grid_search.best_estimator_
    y_pred = best_model.predict(X_test)

    # Step 8: Evaluate the model
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse:.2f}")
    print(f"Best Parameters: {grid_search.best_params_}")

    # Step 9: Convert predictions back to PySpark DataFrame for Spark compatibility
    predictions_df = pd.DataFrame({
        "Actual": y_test.reset_index(drop=True),
        "Predicted": y_pred
    })

    spark_predictions_df = spark.createDataFrame(predictions_df)

    # Show predictions in PySpark DataFrame
    spark_predictions_df.show(10)

    # Stop the Spark session
    spark.stop()