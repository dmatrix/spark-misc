import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs, datediff, current_date

class DataValidation:
    """
    A class to validate data in JSON files using PySpark.
    
    Attributes:
        df (DataFrame): The DataFrame to validate.
    Methods:
        _count_missing_values(): Counts the number of rows with missing values.
        _count_negative_mileage(): Counts the number of rows with negative mileage.
        _count_zero_mileage(): Counts the number of rows with zero mileage.
        _count_age_mismatch(): Counts the number of rows with age and date of birth mismatch.
        _count_status_mismatch(): Counts the number of rows with status and mileage mismatch.
        print_validation_report(): Prints a validation report for the DataFrame.
    Usage:
        df = spark.read.json("path/to/json")
        validator = DataValidation(df)
        validator.print_validation_report()
        
    """

    def __init__(self, df: DataFrame) -> None:
        """
        Initializes the DataValidation class with initial DataFrame for validation
        """
    
        self.df = df

    def _count_missing_values(self) -> int:
        """
        Counts the number of rows with missing values in the DataFrame.
        
        Returns:
            int: The count of rows with missing values.
        """
        return self.df.filter(
            col("ssn").isNull() | 
            col("city").isNull() | 
            col("state").isNull() | 
            col("airline_of_choice").isNull()
        ).count()
    
    def _count_negative_mileage(self) -> int:
        """
        Filters the DataFrame to include only rows with negative mileage.
        
        Returns:
            DataFrame: A DataFrame containing rows with negative mileage.
        """
        return self.df.filter(col("mileage_flown") < 0).count()
    
    def _count_zero_mileage(self) -> int:
        """
        Filters the DataFrame to include only rows with zero mileage.
        
        Returns:
            DataFrame: A DataFrame containing rows with zero mileage.
        """
        return self.df.filter(col("mileage_flown") == 0 ).count()
    
    def _count_age_mismatch(self) -> int: 
        """
        Filters the DataFrame to include only rows with age and date of birth mismatch.
        
        Returns:
            DataFrame: A DataFrame containing rows with age and date of birth mismatch.
        """
        return self.df.withColumn(
            "calculated_age", 
            datediff(current_date(), col("date_of_birth")) / 365
        ).filter(
            abs(col("calculated_age")) - abs(col("age")) > 1  # Allow 1 year difference due to day calculation
        ).count()
    
    def _count_status_mismatch(self) -> int:
        """
        Filters the DataFrame to include only rows with status and mileage mismatch.
        """
        return self.df.filter(
            ~(
                (col("mileage_flown") > 100000) & (col("status") == "GOLD") |
                ((col("mileage_flown") >= 50000) & (col("mileage_flown") <= 100000)) & (col("status") == "SILVER") |
                ((col("mileage_flown") >= 25000) & (col("mileage_flown") < 50000)) & (col("status") == "BRONZE") |
                (col("mileage_flown") < 25000) & (col("status") == "NONE")
            )
        ).count()


    def print_validation_report(self) -> None:
        """
        Prints a validation report for the DataFrame.
        """

        # Count records with various issues
        missing_fields = self._count_missing_values()
        
        negative_mileage = self._count_negative_mileage()
        zero_mileage = self._count_zero_mileage()


        age_mismatch = self._count_age_mismatch()
        status_mismatch = self._count_status_mismatch()

        # Print the validation report

        print(f"Data validation summary:")
        print(f"- Total records: {self.df.count()}")
        print(f"- Records with missing fields: {missing_fields}")
        print(f"- Records with negative mileage: {negative_mileage}")
        print(f"- Records with zero mileage: {zero_mileage}")
        print(f"- Records with age/DOB mismatch: {age_mismatch}")
        print(f"- Records with status/mileage mismatch: {status_mismatch}")
    

