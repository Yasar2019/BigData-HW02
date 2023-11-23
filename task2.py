from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

# Create a Spark session
spark = SparkSession.builder \
    .appName("AggregateScoresDataFrame") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

# Define platforms and topics
platforms = ["Facebook_", "GooglePlus_", "LinkedIn_"]
topics = ["Economy", "Microsoft", "Economy", "Microsoft"]  

# Loop over platforms
for platform in platforms:
    # Initialize an empty list to store DataFrames for each topic
    result_dfs_2nd_column = []
    result_dfs_3rd_column = []
    # Loop over topics
    for topic in topics:
        # Construct the input file path
        input_file = f"Data/{platform}{topic}.csv"

        # Load the CSV file into a DataFrame, specifying the delimiter
        df = spark.read.csv(input_file, header=True, inferSchema=True, sep=",")

        # Define column indices
        id_index = 0

        # Determine the column range dynamically based on your specific use case
        num_columns1 = 3  # Adjust this based on your data
        num_columns2 = 72  # Adjust this based on your data

        # Use when function to handle -1 values as 0 for each column
        avg_columns1 = [
            sum([when(col(df.columns[i]) == -1, 0).otherwise(col(df.columns[i])) for i in range(1, num_columns1 + 1)])
            / num_columns1
        ]
        
        avg_columns2 = [
            sum([when(col(df.columns[i]) == -1, 0).otherwise(col(df.columns[i])) for i in range(1, num_columns2 + 1)])
            / num_columns2
        ]

        # Create DataFrames with IDLink and the calculated averages for each column
        result_df1 = df.select(*avg_columns1)
        result_df2 = df.select(*avg_columns2)

        # Append the result DataFrames to the lists
        result_dfs_2nd_column.append(result_df1)
        result_dfs_3rd_column.append(result_df2)

    # Concatenate the DataFrames for different topics
    final_result_df_2nd_column = result_dfs_2nd_column[0]
    for i in range(1, len(result_dfs_2nd_column)):
        final_result_df_2nd_column = final_result_df_2nd_column.union(result_dfs_2nd_column[i])

    final_result_df_3rd_column = result_dfs_3rd_column[0]
    for i in range(1, len(result_dfs_3rd_column)):
        final_result_df_3rd_column = final_result_df_3rd_column.union(result_dfs_3rd_column[i])

    # Display the result for each column
    # final_result_df_2nd_column.show(truncate=False)
    # final_result_df_3rd_column.show(truncate=False)

    # Save the result as individual CSV files with platform information in the filename (without header)
    output_path_2nd_column = f"output/{platform}_hour.csv"
    output_path_3rd_column = f"output/{platform}_day.csv"

    final_result_df_2nd_column.toPandas().to_csv(output_path_2nd_column, index=False, header=False)
    final_result_df_3rd_column.toPandas().to_csv(output_path_3rd_column, index=False, header=False)

# Stop the Spark session
spark.stop()
