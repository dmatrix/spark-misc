# Mastering PySpark Window Functions: 6 Real-World Use Cases

Window functions are one of the most powerful yet underutilized features in PySpark. Unlike regular aggregations that collapse your data into summary rows, window functions perform calculations across related rows while preserving the original dataset structure. This makes them perfect for analytics tasks like ranking, running totals, trend analysis, and customer journey mapping.

In this comprehensive guide, we'll explore six essential window function patterns through practical, real-world examples. Each demo is designed to be simple, digestible, and ready to use.

> ⚡ **All demos use Spark Connect** with modern architecture for Spark 4.0+ environments.

## 🎯 What Are Window Functions?

Think of window functions as a way to "look around" at neighboring rows while processing each record. They operate over a "window" of rows defined by:

- **Partition**: Groups of related rows (like sales by region)
- **Order**: How rows are sorted within each partition
- **Frame**: Which specific rows to include in calculations

```python
from pyspark.sql.window import Window

# Basic window specification
window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
```

Now let's dive into the six most valuable use cases.

## 🏆 1. Ranking Operations: Finding Top Performers

**Business Problem**: You need to rank salespeople within each region to determine performance bonuses and identify top performers.

Ranking functions help you answer questions like:
- Who are the top 3 performers in each region?
- How should we handle ties in performance rankings?
- Which ranking method best fits our bonus structure?

### The Three Ranking Champions

PySpark offers three distinct ranking functions, each handling ties differently:

```python
# Add three types of ranking functions:
# - row_number: Always unique ranks (1,2,3,4...)
# - rank: Skips ranks after ties (1,1,3,4...)  
# - dense_rank: No gaps in ranks (1,1,2,3...)
df_ranked = (df.withColumn("row_number", row_number().over(window_spec))
               .withColumn("rank", rank().over(window_spec))
               .withColumn("dense_rank", dense_rank().over(window_spec)))
```

### Real-World Application: Performance Bonuses

```python
# Create performance tiers and bonuses based on regional ranking:
# - Get regional rank for each salesperson
# - Assign performance tier (Top Performer vs Standard)
# - Calculate bonus percentage based on rank
df_with_tiers = (df.withColumn("regional_rank", row_number().over(window_spec))
                   .withColumn("performance_tier",
                              when(col("regional_rank") == 1, "🥇 Top Performer")
                              .otherwise("📊 Standard"))
                   .withColumn("bonus_percentage",
                              when(col("regional_rank") == 1, 15.0)
                              .otherwise(5.0)))
```

**When to Use**: Performance rankings, leaderboards, top-N analysis, bonus calculations.

**Demo File**: `ranking_operations_demo.py`

---

## 📊 2. Running Aggregations: Tracking Cumulative Progress

**Business Problem**: You need to track running totals, cumulative averages, and year-to-date performance metrics as new data arrives.

Running aggregations transform your data into a continuous story, showing how metrics evolve over time without losing the detail of individual transactions.

### Building Running Totals

```python
# Calculate cumulative aggregations for each salesperson:
# - running_total: Sum of all sales from start to current row
# - running_count: Count of transactions processed so far  
# - running_avg: Running average of sales amounts
df_running = (df.withColumn("running_total", spark_sum("sales_amount").over(window_spec))
                .withColumn("running_count", count("*").over(window_spec))
                .withColumn("running_avg", spark_avg("sales_amount").over(window_spec)))
```

### Advanced: Window Frames for Flexible Calculations

```python
# Compare different window frame types:
# - unbounded_window: From start of partition to current row
# - last_3_window: Only last 3 rows including current row
df_frames = (df.withColumn("cumulative_total", spark_sum("sales_amount").over(unbounded_window))
               .withColumn("last_3_total", spark_sum("sales_amount").over(last_3_window))
               .withColumn("cumulative_avg", spark_avg("sales_amount").over(unbounded_window)))
```

**When to Use**: Financial running totals, YTD calculations, progressive KPI tracking, contribution analysis.

**Demo File**: `aggregation_window_demo.py`

---

## 🔄 3. Lead/Lag Operations: Time Series Analysis Made Simple

**Business Problem**: You need to compare current values with previous or future periods to identify trends, calculate growth rates, and generate trading signals.

Lead and lag functions are your time-travel toolkit, letting you access yesterday's stock price or next quarter's forecast without complex self-joins.

### Accessing Adjacent Time Periods

```python
# Access adjacent row values using LAG and LEAD:
# - previous_price: Get yesterday's closing price (LAG)
# - next_price: Get tomorrow's closing price (LEAD)
df_lead_lag = (df.withColumn("previous_price", lag("closing_price", 1).over(window_spec))
                 .withColumn("next_price", lead("closing_price", 1).over(window_spec)))
```

### Building Trend Analysis

```python
# Calculate daily price changes using LAG function:
# - previous_price: Get previous day's closing price
# - daily_change: Calculate price difference from previous day
# - trend_direction: Determine if price went up or down
df_changes = (df.withColumn("previous_price", lag("closing_price", 1).over(window_spec))
                .withColumn("daily_change", col("closing_price") - col("previous_price"))
                .withColumn("trend_direction",
                           when(col("daily_change") > 0, "📈 UP").otherwise("📉 DOWN")))
```

### Generating Trading Signals

```python
# Generate trading signals based on price changes:
# - price_yesterday: Get previous day's closing price
# - daily_change: Calculate price difference from previous day
# - trading_signal: Generate buy/sell/hold signal based on price movement
df_signals = (df.withColumn("price_yesterday", lag("closing_price", 1).over(window_spec))
                .withColumn("daily_change", col("closing_price") - col("price_yesterday"))
                .withColumn("trading_signal",
                           when(col("daily_change") > 5, "🟢 BUY")
                           .when(col("daily_change") < -5, "🔴 SELL")
                           .otherwise("🟡 HOLD")))
```

**When to Use**: Time series analysis, trend detection, growth rate calculations, comparative analysis, algorithmic trading signals.

**Demo File**: `lead_lag_demo.py`

---

## 📈 4. Moving Averages: Smoothing the Noise

**Business Problem**: Raw daily metrics are too volatile. You need moving averages to identify genuine trends, smooth out seasonal variations, and make data-driven decisions.

Moving averages are the cornerstone of trend analysis, helping you see the forest through the trees by filtering out short-term noise.

### Creating Moving Averages

```python
# Calculate different moving averages:
# - ma_3_day: 3-day moving average of sales
# - ma_5_day: 5-day moving average of sales
df_moving_avg = (df.withColumn("ma_3_day", spark_round(spark_avg("daily_sales").over(moving_3_day), 2))
                   .withColumn("ma_5_day", spark_round(spark_avg("daily_sales").over(moving_5_day), 2)))
```

### Trend Direction Analysis

```python
# Analyze sales trends using moving averages:
# - ma_5_day: Current 5-day moving average
# - prev_ma_5_day: Previous 5-day moving average
# - trend_direction: Compare current vs previous to show trend
df_trends = (df.withColumn("ma_5_day", spark_avg("daily_sales").over(moving_5_day))
               .withColumn("prev_ma_5_day", lag("ma_5_day", 1).over(window_spec))
               .withColumn("trend_direction",
                          when(col("ma_5_day") > col("prev_ma_5_day"), "📈 IMPROVING")
                          .otherwise("📉 DECLINING")))
```

### Performance Monitoring System

```python
# Monitor performance against targets:
# - ma_5_day: 5-day moving average for trend analysis
# - target_achievement: Check if daily target was met
# - performance_alert: Alert based on moving average vs target
df_monitoring = (df.withColumn("ma_5_day", spark_avg("daily_sales").over(moving_5_day))
                   .withColumn("target_achievement",
                              when(col("daily_sales") >= daily_target, "✅ TARGET MET")
                              .otherwise("❌ BELOW TARGET"))
                   .withColumn("performance_alert",
                              when(col("ma_5_day") < daily_target, "⚠️ MONITOR CLOSELY")
                              .otherwise("✅ PERFORMING WELL")))
```

**When to Use**: Trend analysis, performance monitoring, seasonal adjustment, noise reduction, forecasting preparation.

**Demo File**: `moving_averages_demo.py`

---

## 📊 5. Percentile Analysis: Understanding Distribution and Position

**Business Problem**: You need to understand where employees stand in salary distributions, create equitable compensation bands, and identify outliers for review.

Percentile functions transform absolute values into relative positions, answering questions like "Is this salary in the top 10%?" or "Which quartile does this performance fall into?"

### Calculating Percentile Ranks

```python
# Calculate percentile ranks across company and department:
# - company_percentile: Rank within entire company (0-100%)
# - dept_percentile: Rank within department (0-100%)
# - company_quartile: Company quartile position (1-4)
df_percentiles = (df.withColumn("company_percentile", spark_round(percent_rank().over(company_window) * 100, 1))
                    .withColumn("dept_percentile", spark_round(percent_rank().over(dept_window) * 100, 1))
                    .withColumn("company_quartile", ntile(4).over(company_window)))
```

### Creating Salary Bands

```python
# Create salary bands using percentile functions:
# - company_quintile: Divide into 5 equal groups (1-5)
# - percentile_rank: Exact percentile position
# - salary_band: Assign band based on quintile
df_bands = (df.withColumn("company_quintile", ntile(5).over(company_window))
              .withColumn("percentile_rank", spark_round(percent_rank().over(company_window) * 100, 1))
              .withColumn("salary_band",
                         when(col("company_quintile") >= 4, "Senior Level")
                         .otherwise("Junior/Mid Level")))
```

### Compensation Analysis

```python
# Assess compensation status using percentiles:
# - company_percentile: Company-wide percentile rank
# - dept_quartile: Department quartile position
# - compensation_status: Overall compensation assessment
df_compensation = (df.withColumn("company_percentile", percent_rank().over(company_window))
                     .withColumn("dept_quartile", ntile(4).over(dept_window))
                     .withColumn("compensation_status",
                                when(col("company_percentile") >= 0.75, "✅ WELL COMPENSATED")
                                .otherwise("📊 AVERAGE COMPENSATED")))
```

**When to Use**: Salary analysis, performance distributions, risk assessment, market positioning, equitable compensation design.

**Demo File**: `percentile_analysis_demo.py`

---

## 🎯 6. First/Last Value Analysis: Customer Journey Insights

**Business Problem**: You need to understand customer acquisition channels, track lifetime value, and perform marketing attribution analysis across the entire customer journey.

First and last value functions help you capture the bookends of customer interactions, essential for understanding acquisition patterns and measuring marketing effectiveness.

### Customer Journey Mapping

```python
# Analyze customer journey using first and last values:
# - first_touchpoint: Customer's initial interaction
# - last_touchpoint: Customer's final interaction
# - first_channel: How customer was acquired
df_first_last = (df.withColumn("first_touchpoint", first("touchpoint").over(window_spec))
                   .withColumn("last_touchpoint", last("touchpoint").over(full_window))
                   .withColumn("first_channel", first("channel").over(window_spec)))
```

### Customer Lifetime Value Tracking

```python
# Track customer lifetime value:
# - first_purchase_date: Date of customer's first purchase
# - total_clv: Total revenue from customer
# - customer_segment: Classify customer by value
df_clv = (df.withColumn("first_purchase_date",
                       first(when(col("revenue") > 0, col("event_date"))).over(window_spec))
            .withColumn("total_clv", spark_sum("revenue").over(full_window))
            .withColumn("customer_segment",
                       when(col("total_clv") >= 300, "💎 HIGH VALUE")
                       .otherwise("📊 STANDARD")))
```

### Marketing Attribution Analysis

```python
# Attribution analysis using first and last touchpoints:
# - first_touch_channel: First marketing touchpoint
# - last_touch_channel: Last marketing touchpoint
# - total_revenue: Total revenue from customer
df_attribution = (df.withColumn("first_touch_channel", first("channel").over(window_spec))
                    .withColumn("last_touch_channel", last("channel").over(full_window))
                    .withColumn("total_revenue", spark_sum("revenue").over(full_window)))
```

**When to Use**: Customer journey analysis, marketing attribution, acquisition tracking, lifetime value calculation, retention analysis.

**Demo File**: `first_last_value_demo.py`

---

## 🚀 Getting Started

All demos are built using **Spark Connect**. Each demo file is standalone and ready to run:

```bash
# Run individual demos directly
python ranking_operations_demo.py
python aggregation_window_demo.py
python lead_lag_demo.py
python moving_averages_demo.py
python percentile_analysis_demo.py
python first_last_value_demo.py

# Or use the convenient runner script
python run_demo.py ranking
python run_demo.py aggregation
python run_demo.py lead_lag
python run_demo.py moving_averages
python run_demo.py percentile
python run_demo.py first_last_value

# Run ALL demos sequentially
python run_demo.py all

# Run all demos with clean output (no stderr noise when piping)
python run_demo.py all --quiet

# List all available demos
python run_demo.py --list
```

### ⚡ Spark Connect Configuration

All demos use **Spark Connect** with the following configuration:

```python
spark = SparkSession.builder \
    .appName("WindowFunctionDemo") \
    .config("spark.api.mode", "connect") \
    .remote("local[*]") \
    .getOrCreate()
```

**Benefits of Spark Connect:**
- Better separation between client and server processes
- Modern architecture for Spark 4.0+
- Simplified configuration and setup

### Prerequisites

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Spark Connect is included in PySpark 4.0+
# No additional setup required for local development
```

## 💡 Key Takeaways

1. **Window functions preserve data structure** while adding analytical insights
2. **Partitioning and ordering** are crucial for meaningful results
3. **Different ranking functions** handle ties in distinct ways
4. **Frame specifications** control which rows participate in calculations
5. **Combining multiple window functions** creates powerful analytical pipelines

## 🎯 Choosing the Right Window Function

| Use Case | Function Type | Best For |
|----------|---------------|----------|
| **Rankings & Top-N** | `ROW_NUMBER`, `RANK`, `DENSE_RANK` | Performance analysis, leaderboards |
| **Running Totals** | `SUM`, `COUNT`, `AVG` with frames | Financial tracking, cumulative metrics |
| **Time Comparisons** | `LAG`, `LEAD` | Trend analysis, growth calculations |
| **Trend Smoothing** | `AVG` with moving frames | Moving averages, noise reduction |
| **Distribution Analysis** | `PERCENT_RANK`, `NTILE` | Salary bands, performance tiers |
| **Journey Analysis** | `FIRST_VALUE`, `LAST_VALUE` | Customer acquisition, attribution |

Window functions transform complex analytical questions into elegant, readable code. Master these six patterns, and you'll have the tools to tackle most real-world data analytics challenges with PySpark.

---

*Ready to dive deeper? Each demo file contains detailed explanations, sample data, and production-ready code examples. Happy analyzing! 🎉* 