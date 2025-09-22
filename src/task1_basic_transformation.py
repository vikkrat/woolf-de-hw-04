"""
Task 1: Basic Transformation with Additional Filter
Expected Jobs: 7
This script demonstrates lazy evaluation where all transformations 
are executed together when collect() is called.
"""

from pyspark.sql import SparkSession
import os
import sys

def main():
    # Get the absolute path to the data file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, '..', 'data', 'nuek-vuh3.csv')
    
    # For WSL, convert Windows path if needed
    if not os.path.exists(data_path):
        # Try Windows path
        data_path = "/mnt/d/Projects/Data Engineering/woolf-de-hw-04/data/nuek-vuh3.csv"
    
    print("=" * 60)
    print("Task 1: Basic Transformation")
    print("=" * 60)
    print(f"Data path: {data_path}")
    
    # Create Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .appName("Task1_BasicTransformation") \
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
        
        # Repartition
        nuek_repart = nuek_df.repartition(2)
        
        # Process data with transformations
        nuek_processed = nuek_repart \
            .where("final_priority < 3") \
            .select("unit_id", "final_priority") \
            .groupBy("unit_id") \
            .count()
        
        # Additional transformation (still lazy)
        nuek_processed = nuek_processed.where("count > 2")
        
        # Action - triggers execution
        result = nuek_processed.collect()
        
        print(f"\nResult count: {len(result)}")
        print("\n" + "=" * 60)
        print("CHECK SPARKUI: You should see 7 Jobs")
        print("Take a screenshot of the Jobs page now!")
        print("Save as: screenshots/task1_7_jobs.png")
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