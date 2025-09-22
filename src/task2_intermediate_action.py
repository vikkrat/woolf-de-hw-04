"""
Task 2: Intermediate Action Without Cache
Expected Jobs: 10
This script demonstrates the impact of intermediate actions 
without caching, causing re-computation.
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
    print("Task 2: Intermediate Action")
    print("=" * 60)
    print(f"Data path: {data_path}")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .appName("Task2_IntermediateAction") \
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
        
        nuek_processed = nuek_repart \
            .where("final_priority < 3") \
            .select("unit_id", "final_priority") \
            .groupBy("unit_id") \
            .count()
        
        # First collect - intermediate action
        result1 = nuek_processed.collect()
        print(f"\nFirst collect result: {len(result1)} rows")
        
        # Additional transformation after action
        nuek_processed = nuek_processed.where("count > 2")
        
        # Second collect - causes re-computation
        result2 = nuek_processed.collect()
        print(f"Second collect result: {len(result2)} rows")
        
        print("\n" + "=" * 60)
        print("CHECK SPARKUI: You should see 10 Jobs")
        print("Take a screenshot of the Jobs page now!")
        print("=" * 60)
        
        input("\nPress Enter to exit and close Spark session...")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session closed")

if __name__ == "__main__":
    main()