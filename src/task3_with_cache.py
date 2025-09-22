"""
Task 3: Using Cache for Optimization
Expected Jobs: 9
This script demonstrates how caching intermediate results 
reduces the number of jobs by avoiding re-computation.
"""

from pyspark.sql import SparkSession
import os
import sys

def main():
    # Get data path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, '..', 'data', 'nuek-vuh3.csv')
    
    if not os.path.exists(data_path):
        data_path = "/mnt/d/Projects/Data Engineering/woolf-de-hw-04/data/nuek-vuh3.csv"
    
    print("=" * 60)
    print("Task 3: With Cache")
    print("=" * 60)
    print(f"Data path: {data_path}")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .appName("Task3_WithCache") \
        .getOrCreate()
    
    print("Spark session created successfully")
    print(f"Spark UI available at: http://localhost:4040")
    
    try:
        # Load dataset
        nuek_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(data_path)
        
        print(f"Dataset loaded: {nuek_df.count()} rows")
        
        nuek_repart = nuek_df.repartition(2)
        
        # Cache the intermediate result
        nuek_processed_cached = nuek_repart \
            .where("final_priority < 3") \
            .select("unit_id", "final_priority") \
            .groupBy("unit_id") \
            .count() \
            .cache()  # KEY DIFFERENCE
        
        # First collect - data gets cached
        result1 = nuek_processed_cached.collect()
        print(f"\nFirst collect result: {len(result1)} rows (data cached)")
        
        # Use cached data for additional transformation
        nuek_processed = nuek_processed_cached.where("count > 2")
        
        # Second collect - uses cached data
        result2 = nuek_processed.collect()
        print(f"Second collect result: {len(result2)} rows (from cache)")
        
        print("\n" + "=" * 60)
        print("CHECK SPARKUI: You should see 9 Jobs")
        print("Take a screenshot of the Jobs page now!")
        print("=" * 60)
        
        input("\nPress Enter to continue...")
        
        # Clean up cache
        nuek_processed_cached.unpersist()
        print("Cache cleared")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session closed")

if __name__ == "__main__":
    main()