from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random

class SkewedDatasetGenerator:
    def __init__(self, spark_session=None):
        """Initialize the dataset generator with PySpark session."""
        if spark_session is None:
            self.spark = SparkSession.builder \
                .appName("SkewedDatasetGenerator") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        else:
            self.spark = spark_session
    
    def generate_skewed_normal(self, mean, std, skew, size):
        """Generate skewed normal distribution data."""
        np.random.seed(42)
        # Generate normal distribution
        normal_data = np.random.normal(mean, std, size)
        
        # Apply skewness transformation
        if skew > 0:
            # Right skew - make positive values more extreme
            normal_data = np.where(normal_data > mean, 
                                 mean + (normal_data - mean) * (1 + skew),
                                 normal_data)
        elif skew < 0:
            # Left skew - make negative values more extreme  
            normal_data = np.where(normal_data < mean,
                                 mean + (normal_data - mean) * (1 + abs(skew)),
                                 normal_data)
        
        return normal_data
    
    def generate_power_law(self, alpha, x_min, size):
        """Generate power law distribution (heavy-tailed)."""
        np.random.seed(43)
        # Pareto distribution
        data = np.random.pareto(alpha, size) + x_min
        # Cap extreme values
        return np.clip(data, x_min, x_min * 1000)
    
    def generate_log_normal(self, mu, sigma, size):
        """Generate log-normal distribution."""
        np.random.seed(44)
        return np.random.lognormal(mu, sigma, size)
    
    def generate_exponential_skewed(self, rate, size):
        """Generate exponentially skewed data."""
        np.random.seed(45)
        return np.random.exponential(1/rate, size)
    
    def create_real_time_dataset(self, num_rows=100000, num_partitions=8):
        """
        Create a real-time dataset with skewed data distributions.
        
        Args:
            num_rows: Number of rows to generate (default: 100,000)
            num_partitions: Number of partitions for the DataFrame
        
        Returns:
            PySpark DataFrame with skewed data
        """
        
        print(f"Generating {num_rows:,} rows of skewed data...")
        
        # Generate base data using numpy for efficiency
        np.random.seed(42)
        
        # Column 1: User ID (Sequential)
        user_ids = list(range(1, num_rows + 1))
        
        # Column 2: Age (Right-skewed normal - younger population bias)
        ages = self.generate_skewed_normal(mean=32, std=12, skew=1.8, size=num_rows)
        ages = np.clip(ages, 18, 85).astype(int)
        
        # Column 3: Income (Log-normal - wealth inequality)
        incomes = self.generate_log_normal(mu=10.5, sigma=0.9, size=num_rows)
        incomes = np.clip(incomes, 20000, 1000000).astype(int)
        
        # Column 4: Credit Score (Left-skewed - most people have decent credit)
        credit_scores = self.generate_skewed_normal(mean=720, std=80, skew=-1.2, size=num_rows)
        credit_scores = np.clip(credit_scores, 300, 850).astype(int)
        
        # Column 5: Session Duration (Power law - most sessions are short)
        session_durations = self.generate_power_law(alpha=2.5, x_min=30, size=num_rows)
        session_durations = session_durations.astype(int)
        
        # Column 6: Purchase Amount (Log-normal with many small purchases)
        purchase_amounts = self.generate_log_normal(mu=3.5, sigma=1.5, size=num_rows)
        purchase_amounts = np.clip(purchase_amounts, 5, 5000).round(2)
        
        # Column 7: Page Views (Power law distribution)
        page_views = self.generate_power_law(alpha=2.0, x_min=1, size=num_rows)
        page_views = page_views.astype(int)
        
        # Column 8: Response Time (Log-normal - server performance)
        response_times = self.generate_log_normal(mu=4.5, sigma=0.8, size=num_rows)
        response_times = np.clip(response_times, 50, 10000).astype(int)
        
        # Column 9: Device Usage Hours (Exponential decay)
        device_hours = self.generate_exponential_skewed(rate=0.3, size=num_rows)
        device_hours = np.clip(device_hours, 0.1, 24).round(1)
        
        # Column 10: Error Count (Heavy right skew - most users have few errors)
        error_counts = self.generate_power_law(alpha=3.0, x_min=0, size=num_rows)
        error_counts = error_counts.astype(int)
        
        # Create timestamp column (real-time simulation)
        start_time = datetime.now() - timedelta(days=30)
        timestamps = [start_time + timedelta(seconds=i*2.592) for i in range(num_rows)]  # ~30 days spread
        
        # Create categorical columns with skewed distributions
        regions = ['North', 'South', 'East', 'West', 'Central']
        region_weights = [0.4, 0.25, 0.15, 0.15, 0.05]  # Skewed towards North
        user_regions = np.random.choice(regions, size=num_rows, p=region_weights)
        
        device_types = ['Mobile', 'Desktop', 'Tablet']
        device_weights = [0.7, 0.25, 0.05]  # Mobile dominant
        devices = np.random.choice(device_types, size=num_rows, p=device_weights)
        
        # Create pandas DataFrame first for easier handling
        pandas_df = pd.DataFrame({
            'user_id': user_ids,
            'age': ages,
            'income': incomes,
            'credit_score': credit_scores,
            'session_duration_sec': session_durations,
            'purchase_amount': purchase_amounts,
            'page_views': page_views,
            'response_time_ms': response_times,
            'device_usage_hours': device_hours,
            'error_count': error_counts,
            'timestamp': timestamps,
            'region': user_regions,
            'device_type': devices
        })
        
        # Convert to PySpark DataFrame
        spark_df = self.spark.createDataFrame(pandas_df)
        
        # Repartition for better performance
        spark_df = spark_df.repartition(num_partitions)
        
        # Add some computed columns to make it more realistic
        spark_df = spark_df.withColumn("user_segment", 
                                     when(col("income") > 80000, "Premium")
                                     .when(col("income") > 50000, "Standard")
                                     .otherwise("Basic")) \
                          .withColumn("high_engagement", 
                                     (col("page_views") > 10) & (col("session_duration_sec") > 300)) \
                          .withColumn("weekend_user", 
                                     date_format(col("timestamp"), "EEEE").isin(["Saturday", "Sunday"]))
        
        # Cache for better performance if used multiple times
        spark_df.cache()
        
        print(f"Dataset created successfully with {spark_df.count():,} rows and {len(spark_df.columns)} columns")
        
        return spark_df
    
    def show_data_skewness_analysis(self, df):
        """Analyze and display the skewness in the generated data."""
        print("\n" + "="*60)
        print("DATA SKEWNESS ANALYSIS")
        print("="*60)
        
        # Numerical columns analysis
        numerical_cols = ['age', 'income', 'credit_score', 'session_duration_sec', 
                         'purchase_amount', 'page_views', 'response_time_ms', 
                         'device_usage_hours', 'error_count']
        
        for col_name in numerical_cols:
            stats = df.select(
                min(col(col_name)).alias('min'),
                max(col(col_name)).alias('max'),
                mean(col(col_name)).alias('mean'),
                stddev(col(col_name)).alias('std'),
                expr(f"percentile_approx({col_name}, 0.5)").alias('median'),
                expr(f"percentile_approx({col_name}, 0.95)").alias('p95'),
                expr(f"percentile_approx({col_name}, 0.99)").alias('p99')
            ).collect()[0]
            
            print(f"\n{col_name.upper()}:")
            print(f"  Range: {stats['min']:,.0f} - {stats['max']:,.0f}")
            print(f"  Mean: {stats['mean']:,.2f} | Median: {stats['median']:,.0f}")
            print(f"  Std Dev: {stats['std']:,.2f}")
            print(f"  95th percentile: {stats['p95']:,.0f}")
            print(f"  99th percentile: {stats['p99']:,.0f}")
            
            # Indicate skewness direction
            if stats['mean'] > stats['median'] * 1.1:
                print(f"  → RIGHT SKEWED (long tail of high values)")
            elif stats['mean'] < stats['median'] * 0.9:
                print(f"  → LEFT SKEWED (long tail of low values)")
            else:
                print(f"  → RELATIVELY SYMMETRIC")
        
        # Categorical distributions
        print(f"\nCATEGORICAL DISTRIBUTIONS:")
        
        print(f"\nRegion Distribution:")
        df.groupBy("region").count().orderBy(desc("count")).show()
        
        print(f"Device Type Distribution:")
        df.groupBy("device_type").count().orderBy(desc("count")).show()
        
        print(f"User Segment Distribution:")
        df.groupBy("user_segment").count().orderBy(desc("count")).show()
    
    def save_dataset(self, df, output_path, format="parquet"):
        """Save the dataset to specified path and format."""
        print(f"Saving dataset to {output_path} in {format} format...")
        
        if format.lower() == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format.lower() == "csv":
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        elif format.lower() == "json":
            df.write.mode("overwrite").json(output_path)
        else:
            raise ValueError("Supported formats: parquet, csv, json")
        
        print(f"Dataset saved successfully!")
    
    def create_streaming_simulation(self, df, output_path, batch_size=1000):
        """Simulate real-time streaming by writing data in batches."""
        print(f"Starting real-time streaming simulation...")
        print(f"Batch size: {batch_size} records")
        
        total_rows = df.count()
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        # Add row numbers for batching
        window = Window.orderBy("timestamp")
        df_with_row_num = df.withColumn("row_num", row_number().over(window))
        
        for batch_num in range(1, num_batches + 1):
            batch_df = df_with_row_num.filter(
                (col("row_num") > (batch_num - 1) * batch_size) & 
                (col("row_num") <= batch_num * batch_size)
            ).drop("row_num")
            
            batch_path = f"{output_path}/batch_{batch_num:04d}"
            batch_df.write.mode("overwrite").parquet(batch_path)
            
            print(f"Batch {batch_num}/{num_batches} written ({batch_df.count()} records)")
            
            # Simulate real-time delay
            import time
            time.sleep(0.1)
        
        print("Streaming simulation completed!")

# Usage Example
def main():
    """Example usage of the SkewedDatasetGenerator."""
    
    # Initialize generator
    generator = SkewedDatasetGenerator()
    
    # Generate dataset
    skewed_df = generator.create_real_time_dataset(num_rows=100000, num_partitions=8)
    
    # Show basic info
    print("\nDataset Schema:")
    skewed_df.printSchema()
    
    print("\nFirst 20 rows:")
    skewed_df.show(20, truncate=False)
    
    # Analyze skewness
    generator.show_data_skewness_analysis(skewed_df)
    
    # Save dataset
    # generator.save_dataset(skewed_df, "/path/to/output/skewed_dataset", format="parquet")
    
    # Simulate streaming (optional)
    # generator.create_streaming_simulation(skewed_df, "/path/to/streaming/output", batch_size=5000)
    
    # Stop Spark session
    generator.spark.stop()

if __name__ == "__main__":
    main()