"""
Ultra-Simple Utilities for Learning transformWithState

Just the essential functions needed to understand the basics.
No complex configurations or options - just pure learning.

Author: Jules S. Damji & Cursor AI
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max as spark_max, count, expr
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.streaming import StreamingQuery


def create_spark() -> SparkSession:
    """
    Create a Spark session optimized for transformWithState learning.
    
    This setup provides full support for transformWithState including:
    - Advanced state store providers (RocksDB)
    - Reliable checkpointing with local filesystem
    - JSON-based state management
    - Clean streaming infrastructure for learning
    
    Args:
        None
        
    Returns:
        SparkSession: A Spark session configured for transformWithState learning
    """
    print("🔧 Creating Spark session for transformWithState...")
    
    # Get or create Spark session with RocksDB configuration
    spark = SparkSession.builder \
        .appName("LearnTransformWithState") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.sql.streaming.stateStore.rocksdb.formatVersion", "5") \
        .config("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows", "true") \
        .config("spark.sql.streaming.stateStore.rocksdb.maxOpenFiles", "1000") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", "2") \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "300s") \
        .getOrCreate()
    
    # Set log level for cleaner output
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark ready with full transformWithState support!")
    print("   🗄️  RocksDB state store enabled")
    print("   📁 Local checkpointing available")
    print("   🚀 Clean streaming infrastructure for learning")
    return spark


def create_flight_data(spark: SparkSession) -> DataFrame:
    """
    Create simple flight data stream.
    
    Args:
        spark: The Spark session to use for creating the stream
        
    Returns:
        DataFrame: A streaming DataFrame with flight state updates
    """
    print("📊 Creating flight data stream...")
    
    # Use rate source to generate data
    rate_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    
    # Transform to simple flight data
    flight_stream = rate_stream.selectExpr(
        # Three flights cycling - realistic flight numbers
        """CASE (value % 3)
            WHEN 0 THEN 'Delta1247'
            WHEN 1 THEN 'United892'
            ELSE 'Southwest5031'
        END as flight""",
        
        # Simple state progression: boarding -> flying -> landed
        """CASE (value % 3)
            WHEN 0 THEN 'boarding'
            WHEN 1 THEN 'flying'
            ELSE 'landed'
        END as state""",
        
        "timestamp"
    )
    
    print("✅ Flight data ready!")
    return flight_stream


def run_learning_demo(spark: SparkSession) -> None:
    """
    Run the transformWithState demo for learning stateful processing.
    
    This uses the actual transformWithState API with full support for:
    - RocksDB state store for reliable state management
    - FS checkpointing for fault tolerance
    
    Args:
        spark: The Spark session to use for the demo
        
    Returns:
        None
    """
    print("\n" + "🎓" + "="*50)
    print("LEARNING DEMO: transformWithState")
    print("="*50)
    
    # Import our simple processor
    from learn_tws import FlightProcessor
    
    # Create flight data
    flight_data = create_flight_data(spark)
    
    # Define output schema
    output_schema = StructType([
        StructField("flight", StringType(), True),
        StructField("current_state", StringType(), True),
        StructField("update_count", StringType(), True)
    ])
    
    # Apply transformWithState - this is the key learning point!
    # Spark fully supports this with RocksDB state store
    flight_states = flight_data \
        .groupBy("flight") \
        .transformWithStateInPandas(
            statefulProcessor=FlightProcessor(),
            outputStructType=output_schema,
            outputMode="Update",
            timeMode="ProcessingTime"
        )
    
    # Use FS for reliable checkpointing 
    import uuid
    checkpoint_dir = f"/tmp/learn_checkpoint_{uuid.uuid4().hex[:8]}"
    
    print(f"📁 Using checkpoint location: {checkpoint_dir}")
    
    # Start the stream with optimized settings
    query = flight_states \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 15) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("\n🚀 Demo running! Watch transformWithState in action...")
    print("📝 Key things to notice:")
    print("   - 🗄️  RocksDB manages state reliably for each flight")  
    print("   - ✅ State transitions are validated in real-time")
    print("   - 📈 Update counts increase over time")
    print("   - 💾 State persists across batches with checkpointing")
    print("   - 🚀 This is real transformWithState in action!")
    print(f"   - 📁 Checkpoint: {checkpoint_dir}")
    print("\n⏹️  Press Ctrl+C to stop when you've learned enough!")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping demo...")
        query.stop()
        print("✅ Demo complete! You've mastered transformWithState basics.")
        print(f"📁 Checkpoint preserved at: {checkpoint_dir}")
    finally:
        # Ensure clean shutdown
        if query.isActive:
            query.stop()
            print("🔄 Stream stopped gracefully")


def explain_basics() -> None:
    """
    Explain the core concepts simply.
    
    Args:
        None
        
    Returns:
        None
    """
    print("\n" + "📚" + "="*60)
    print("TRANSFORM WITH STATE LEARNING")
    print("="*60)
    print("""
🎯 THE BIG IDEA:
   Keep information about each thing (like flights) between batches

🔑 KEY CONCEPTS:

1. GROUPING BY KEY
   .groupBy("flight")  ← Each flight gets separate processing

2. STATE STORAGE  
   Each flight remembers its current state (boarding/flying/landed)

3. BATCH PROCESSING
   Every few seconds, process new updates for each flight

4. STATE PERSISTENCE
   Flight state survives between batches - that's the magic!

🛫 OUR EXAMPLE:
   - Track flights: Delta1247, United892, Southwest5031
   - States: boarding → flying → landed
   - Each flight remembers where it is

🧠 MENTAL MODEL:
   Think of it like having a notebook for each flight.
   Every batch, you:
   1. Look up the flight's current page in the notebook
   2. Read what state it was in
   3. Update it based on new information  
   4. Write the new state back to the notebook
   5. The notebook persists for the next batch!

🏗️ LEARNING SETUP ADVANTAGES:
   - 🗄️  RocksDB state store (reliable and fast)
   - 📁 Local checkpointing (fault tolerance)
   - 🚀 Simple configuration (easy to understand)
   - 💾 JSON serialization (avoids complexity)
   - 🔧 Clean infrastructure (focus on concepts)

⚙️ THE API:
   - transformWithState gives you full control
   - StatefulProcessor handles the state logic
   - You decide what to store and how to update it
   - RocksDB makes it reliable for learning!
""")
    print("="*60)
    print("🚀 READY TO SEE IT IN ACTION!")
    print("="*60)
