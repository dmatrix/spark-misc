from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when, round as spark_round, percent_rank, ntile, cume_dist
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import date

def create_employee_salary_data(spark):
    """Create simple employee salary data for percentile analysis"""
    data = [
        # Engineering Department
        ("Alice Johnson", "Engineering", "Senior Engineer", 120000.0),
        ("Bob Smith", "Engineering", "Software Engineer", 95000.0),
        ("Carol Davis", "Engineering", "Principal Engineer", 140000.0),
        ("David Wilson", "Engineering", "Staff Engineer", 130000.0),
        
        # Sales Department
        ("Grace Lee", "Sales", "Account Executive", 80000.0),
        ("Henry Chen", "Sales", "Sales Manager", 100000.0),
        ("Iris Taylor", "Sales", "Sales Director", 125000.0),
        
        # HR Department
        ("Luis Garcia", "HR", "HR Specialist", 65000.0),
        ("Mary Jones", "HR", "HR Manager", 85000.0),
        ("Nick Clark", "HR", "HR Director", 110000.0),
        
        # Finance Department
        ("Paul Rodriguez", "Finance", "Financial Analyst", 70000.0),
        ("Quinn Thompson", "Finance", "Finance Manager", 95000.0),
        ("Rachel Green", "Finance", "Finance Director", 120000.0),
    ]
    
    schema = StructType([
        StructField("employee_name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("job_title", StringType(), False),
        StructField("annual_salary", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_percentile_functions(df):
    """Show percentile-based window functions"""
    
    print("🎯 PERCENTILE & STATISTICAL FUNCTIONS DEMO")
    print("=" * 60)
    
    # Company-wide window for overall percentiles
    company_window = Window.orderBy("annual_salary")
    
    # Department-wise window for department percentiles
    dept_window = Window.partitionBy("department").orderBy("annual_salary")
    
    # Calculate percentile ranks across company and department:
    # - company_percentile: Rank within entire company (0-100%)
    # - dept_percentile: Rank within department (0-100%)
    # - company_quartile: Company quartile position (1-4)
    df_percentiles = (df.withColumn("company_percentile", spark_round(percent_rank().over(company_window) * 100, 1))
                        .withColumn("dept_percentile", spark_round(percent_rank().over(dept_window) * 100, 1))
                        .withColumn("company_quartile", ntile(4).over(company_window)))
    
    print("📊 Salary Percentiles and Quartiles")
    print("-" * 60)
    df_percentiles.select("employee_name", "department", "annual_salary",
                          "company_percentile", "dept_percentile", "company_quartile") \
                  .orderBy("department", desc("annual_salary")) \
                  .show(truncate=False)
    
    return df_percentiles

def analyze_salary_distribution(df):
    """Analyze salary distribution using NTILE"""
    
    print("\n🎯 SALARY DISTRIBUTION ANALYSIS")
    print("=" * 60)
    
    # Department window for distribution analysis
    dept_window = Window.partitionBy("department").orderBy("annual_salary")
    company_window = Window.orderBy("annual_salary")
    
    # Analyze salary distribution within departments:
    # - dept_quintile: Department quintile (1-5)
    # - dept_percentile: Department percentile rank
    # - salary_tier: Classify employees into tiers
    df_distribution = (df.withColumn("dept_quintile", ntile(5).over(dept_window))
                         .withColumn("dept_percentile", spark_round(percent_rank().over(dept_window) * 100, 1))
                         .withColumn("salary_tier",
                                    when(col("dept_percentile") >= 80, "🥇 Top 20%")
                                    .otherwise("📊 Below Top 20%")))
    
    print("📊 Department Salary Distribution (Quintiles & Tiers)")
    print("-" * 60)
    df_distribution.select("department", "employee_name", "annual_salary",
                           "dept_quintile", "salary_tier") \
                   .orderBy("department", desc("annual_salary")) \
                   .show(truncate=False)
    
    return df_distribution

def identify_salary_bands(df):
    """Create salary bands using percentile functions"""
    
    print("\n🎯 SALARY BAND CREATION")
    print("=" * 60)
    
    # Company-wide percentile window
    company_window = Window.orderBy("annual_salary")
    
    # Create salary bands using percentile functions:
    # - company_quintile: Divide into 5 equal groups (1-5)
    # - percentile_rank: Exact percentile position
    # - salary_band: Assign band based on quintile
    df_bands = (df.withColumn("company_quintile", ntile(5).over(company_window))
                  .withColumn("percentile_rank", spark_round(percent_rank().over(company_window) * 100, 1))
                  .withColumn("salary_band",
                             when(col("company_quintile") >= 4, "Senior Level")
                             .otherwise("Junior/Mid Level")))
    
    print("📊 Company-Wide Salary Bands")
    print("-" * 60)
    df_bands.select("employee_name", "department", "annual_salary",
                    "percentile_rank", "salary_band") \
            .orderBy(desc("annual_salary")) \
            .show(truncate=False)
    
    # Show band summary
    print("\n📈 Salary Band Summary")
    print("-" * 40)
    from pyspark.sql.functions import count, avg as spark_avg, min as spark_min, max as spark_max
    
    band_summary = df_bands.groupBy("salary_band") \
                           .agg(count("*").alias("employee_count"),
                                spark_avg("annual_salary").alias("avg_band_salary"),
                                spark_min("annual_salary").alias("min_salary"),
                                spark_max("annual_salary").alias("max_salary")) \
                           .orderBy(desc("avg_band_salary"))
    
    band_summary.show(truncate=False)
    
    return df_bands

def real_world_example(df):
    """Show practical application: Compensation Analysis"""
    
    print("\n💼 REAL-WORLD APPLICATION: Compensation Analysis & Benchmarking")
    print("=" * 60)
    
    # Department and company windows
    dept_window = Window.partitionBy("department").orderBy("annual_salary")
    company_window = Window.orderBy("annual_salary")
    
    # Assess compensation status using percentiles:
    # - company_percentile: Company-wide percentile rank
    # - dept_quartile: Department quartile position
    # - compensation_status: Overall compensation assessment
    df_compensation = (df.withColumn("company_percentile", percent_rank().over(company_window))
                         .withColumn("dept_quartile", ntile(4).over(dept_window))
                         .withColumn("compensation_status",
                                    when(col("company_percentile") >= 0.75, "✅ WELL COMPENSATED")
                                    .otherwise("📊 AVERAGE COMPENSATED")))
    
    print("📊 Compensation Analysis")
    print("-" * 60)
    df_compensation.select("employee_name", "department", "annual_salary",
                           "dept_quartile", "compensation_status") \
                   .orderBy("department", desc("annual_salary")) \
                   .show(truncate=False)
    
    # Show departmental compensation summary
    print("\n📈 Departmental Compensation Summary")
    print("-" * 50)
    from pyspark.sql.functions import count, avg as spark_avg
    
    dept_summary = df_compensation.groupBy("department", "compensation_status") \
                                 .agg(count("*").alias("employee_count")) \
                                 .orderBy("department", "compensation_status")
    
    dept_summary.show(truncate=False)
    
    # Show summary by department
    print("\n📈 Department Summary")
    print("-" * 30)
    dept_summary.show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PercentileAnalysisDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create sample data
    df = create_employee_salary_data(spark)
    
    print("📋 Original Employee Salary Data")
    print("-" * 40)
    df.orderBy("department", desc("annual_salary")).show(truncate=False)
    
    # Demonstrate percentile functions
    df_percentiles = demonstrate_percentile_functions(df)
    
    # Analyze salary distribution
    df_distribution = analyze_salary_distribution(df)
    
    # Create salary bands
    df_bands = identify_salary_bands(df)
    
    # Real-world application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• PERCENT_RANK() calculates percentile position (0.0 to 1.0)")
    print("• NTILE(n) divides data into n equal groups/buckets")
    print("• CUME_DIST() shows cumulative distribution")
    print("• Perfect for salary analysis, performance evaluation, and benchmarking")
    print("• Essential for creating fair compensation structures")
    print("• Combine with business logic for actionable insights")
    
    spark.stop() 