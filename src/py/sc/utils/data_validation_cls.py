import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs, datediff, current_date, count

class DataValidation:
    """
    A class to validate data in JSON files using PySpark.
    
    Attributes:
        df (DataFrame): The DataFrame to validate.
    Methods:
        except_all(df: DataFrame) -> int:
            Returns the rows in the DataFrame that are not present in the other DataFrame.
        drop_missing_values() -> int:
            Drops rows with missing values from the DataFrame.
        drop_negative_or_zero_mileage() -> int:
            Filters the DataFrame to include only rows with negative mileage.
        drop_duplicate_records() -> int:
            Filters the DataFrame to include only duplicate records based on unique_id.
        print_validation_report() -> int:
            Prints a validation report for the DataFrame.
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
        self.missing_values = 0
        self.zero_or_negative_mileage = 0
        self.duplicate_records = 0
        self.original_count = df.count()

    def get_df(self) -> DataFrame:
        """
        Returns the DataFrame.
        
        Returns:
            DataFrame: The DataFrame.
        """
        return self.df
    

    def except_all(self, df: DataFrame) -> int:
        """
        Returns the rows in the DataFrame that are not present in the other DataFrame.
        
        Args:
            df (DataFrame): The DataFrame to compare against.
        
        Returns:
            int: the new count of rows in the DataFrame after removing missing values
        """
        
        self.df = self.df.exceptAll(df)
       
        return self.df.count()

    def drop_missing_values(self, cleanup=False) -> int:
        """
        Drops rows with missing values from the DataFrame.
        
        Returns:
            int: The count of rows dropped of missing values.
        """
        self.df = self.df.na.drop()
        self.missing_values = self.original_count - self.df.count()
        return self.missing_values 
       
    
    def drop_negative_or_zero_mileage(self) -> DataFrame:
        """
        Filters the DataFrame to include only rows with negative mileage.
        
        Returns:
            int: The count of rows with negative or zero mileage.
        """
        self.zero_or_negative_mileage = self.df.filter(col("mileage_flown") <= 0).count()
        self.df = self.df.filter(col("mileage_flown") > 0)

        return self.zero_or_negative_mileage
    
   
    def drop_duplicate_records(self) -> DataFrame:
        """
        Filters the DataFrame to include only duplicate records based on unique_id.
        
        Returns:
            int: The count of duplicate records.
        """
        duplicate_df = (self.df.groupBy("unique_id") 
                        .agg(count("*").alias("count")) 
                        .filter(col("count") > 1))
        self.duplicate_records  = duplicate_df.count()
        self.df = self.df.dropDuplicates(subset=["unique_id"])
        return self.duplicate_records


    def print_validation_report(self) -> None:
        """
        Prints a validation report for the DataFrame.

        Returns: 
            None
        """

        # Count records with various issues
        missing_fields = self.drop_missing_values()
        negative_or_zero_mileage = self.drop_negative_or_zero_mileage()
        duplicate_records = self.drop_duplicate_records()

        # Print the validation report
        print(f"Data data validation summary:")
        print (f"- Total records before cleanup: {self.original_count}")
        print(f" - Total records after cleanup: {self.df.count()}")
        print(f" - Records with missing fields: {missing_fields}")
        print(f" - Records with negative or zero mileage: {negative_or_zero_mileage}")
        if duplicate_records > 0:
            print(f" - Records with duplicate unique_id: {duplicate_records}")
        else:
            print(f" - No records with duplicate unique_id")
