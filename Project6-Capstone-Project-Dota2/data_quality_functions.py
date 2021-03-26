import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def get_record_counts(destination_table_paths: list,
                      spark: SparkSession,
                      date_processed: str):
    """
    This function will loop through each of the destination paths and get a row count from each, where the data processed is the same as the
    current etl run.
    This is to make sure that data was loaded for the current etl run.
    It will report on each table that has 0 rows.
    """
    
    for table_path in destination_table_paths:
        try:
            count_df = spark.read.parquet(table_path).where(f"date_processed == '{date_processed}'").count()
        except Exception as e:
            print(e)
            raise
        
        if count_df == 0:
            table_name = table_path.split("/")[-1]
            print(table_name + " contains " + str(count_df) + " rows for this current ETL run.")
            
            
def get_orphaned_records(source_destination_path:str,
                        target_destination_path: str,
                        source_column_name: str,
                        target_column_name: str,
                        spark: SparkSession):
    
    """
    Here we check that there are no orphaned records. We pass in a source and destination path, and we check if 
    there are any records from the source that do not exist in the destination. This is to ensure data integrity, 
    since we cannot enforce referential integrity.
    We use this one function to check all source and destinations.
    """
    
    source_data = source_destination_path.split("/")[-1]
    target_data = target_destination_path.split("/")[-1]
    
    try:
        source_ids = spark.read.parquet(source_destination_path).select(F.col(source_column_name)).distinct()
    except Exception as e:
        print(e)
    
    try:
        target_ids = spark.read.parquet(target_destination_path).select(F.col(target_column_name)).distinct()
    except Exception as e:
        print(e)
    
    orphaned_rows = (source_ids.alias("src").join(target_ids.alias("tar"),
                                          on= F.col(f"src.{source_column_name}")==F.col(f"tar.{target_column_name}"),
                                          how="left_anti"
                                         )
                                    .select(f"src.{source_column_name}")
                                    .count()
                     )
    
    if (not orphaned_rows or orphaned_rows is None or orphaned_rows == 0):
        print(f"Great, There are no orpahned rows in {source_data} that are not in {target_data}")
        return "Success"
    else:
        print(str(orphaned_rows) + f" id(s) found in {source_data} that do not exist in {target_data}.") 
        return "Failed"