from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace
from operator import add
from pyspark.sql import functions as F
import os

print(os.environ['HADOOP_HOME'])

if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("NewsDataStats") \
        .master("spark://96.9.210.170:7077") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.hadoop.home.dir", "C:/Users/Asus/Downloads/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3/bin") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()
    sc = spark.sparkContext

    # Read the data
    data = spark.read.option("header", "true").option("quote", "\"").option(
        "escape", "\"").option("multiline", "true").csv('Data/News_Final.csv')

    # Define a function to remove punctuation and ellipses
    def clean_text(column):
        # Remove possessive apostrophes and any characters following it
        column = regexp_replace(column, "'s|’s|-", " ")
        # Remove ellipses
        column = regexp_replace(column, "\\.\\.\\.", "")
        # Remove remaining punctuation except apostrophes within words (e.g., O'Malley, ObamaBiden)
        column = regexp_replace(column, "(?<!\\w)'|'(?!(\\w|'))|[^\\w\\s]", "")
        return column
    
    # Clean the 'Title' and 'Headline' columns
    data = data.withColumn('Title', clean_text(col('Title')))
    data = data.withColumn('Headline', clean_text(col('Headline')))
        
    # Define a function to count words in a title
    def word_count_per_title(title):
        if title is not None:
            return [(word.lower(), 1) for word in title.split()]
        else:
            return []

    # TASK #1
    # Extract titles and count words
    titles = data.rdd.map(lambda row: row['Title'])
    total_title_word_counts = titles.flatMap(word_count_per_title).reduceByKey(add).sortBy(lambda x: x[1], ascending=False)

    # Extract headlines and count words
    headlines = data.rdd.map(lambda row: row['Headline'])
    total_headline_word_counts = headlines.flatMap(word_count_per_title).reduceByKey(add).sortBy(lambda x: x[1], ascending=False)

    # Convert total word counts to DataFrame and save to CSV
    total_title_word_counts_df = total_title_word_counts.toDF(["word", "count"]).toPandas()
    total_title_word_counts_df.to_csv("output/total_title_word_counts.csv", header=True)

    total_headline_word_counts_df = total_headline_word_counts.toDF(["word", "count"]).toPandas()
    total_headline_word_counts_df.to_csv("output/total_headline_word_counts.csv", header=True)
    
    date_word_rdd_title = data.rdd.filter(lambda row: row['Title'] is not None).flatMap(
    lambda row: [((row['PublishDate'], word.lower()), 1) for word in row['Title'].split()])
    date_word_rdd_headline = data.rdd.filter(lambda row: row['Headline'] is not None).flatMap(
    lambda row: [((row['PublishDate'], word.lower()), 1) for word in row['Headline'].split()])

    # Reduce by key to get the count
    word_count_rdd_title = date_word_rdd_title.reduceByKey(add)
    word_count_rdd_headline = date_word_rdd_headline.reduceByKey(add)

    # Map to prepare for grouping by date
    date_word_count_rdd_title = word_count_rdd_title.map(lambda x: (x[0][0], (x[0][1], x[1])))
    date_word_count_rdd_headline = word_count_rdd_headline.map(lambda x: (x[0][0], (x[0][1], x[1])))

    # Group by date and sort by count in descending order within each date
    sorted_word_count_rdd_title = date_word_count_rdd_title.groupByKey().mapValues(
        lambda words: sorted(words, key=lambda x: x[1], reverse=True))
    sorted_word_count_rdd_headline = date_word_count_rdd_headline.groupByKey().mapValues(
        lambda words: sorted(words, key=lambda x: x[1], reverse=True))

    # Convert to DataFrame
    word_count_df_title = spark.createDataFrame(sorted_word_count_rdd_title.flatMap(
        lambda x: [(x[0], word, count) for word, count in x[1]]), ["date", "word", "count"])
    word_count_df_headline = spark.createDataFrame(sorted_word_count_rdd_headline.flatMap(
        lambda x: [(x[0], word, count) for word, count in x[1]]), ["date", "word", "count"])

    # Convert to Pandas DataFrame
    word_count_pd_title = word_count_df_title.toPandas()
    word_count_pd_headline = word_count_df_headline.toPandas()

    # Save to CSV
    word_count_pd_title.to_csv("output/per_date_word_counts_title.csv", index=False)
    word_count_pd_headline.to_csv("output/per_date_word_counts_headline.csv", index=False)
    
    # Filter and flatMap to create ((topic, word), 1) pairs
    topic_word_rdd_title = data.rdd.filter(lambda row: row['Title'] is not None).flatMap(
        lambda row: [((row['Topic'], word.lower()), 1) for word in row['Title'].split()])
    topic_word_rdd_headline = data.rdd.filter(lambda row: row['Headline'] is not None).flatMap(
        lambda row: [((row['Topic'], word.lower()), 1) for word in row['Headline'].split()])

    # Reduce by key to get the count
    word_count_rdd_title = topic_word_rdd_title.reduceByKey(add)
    word_count_rdd_headline = topic_word_rdd_headline.reduceByKey(add)

    # Map to prepare for grouping by topic
    topic_word_count_rdd_title = word_count_rdd_title.map(lambda x: (x[0][0], (x[0][1], x[1])))
    topic_word_count_rdd_headline = word_count_rdd_headline.map(lambda x: (x[0][0], (x[0][1], x[1])))

    # Group by topic and sort by count in descending order within each topic
    sorted_word_count_rdd_title = topic_word_count_rdd_title.groupByKey().mapValues(
        lambda words: sorted(words, key=lambda x: x[1], reverse=True))
    sorted_word_count_rdd_headline = topic_word_count_rdd_headline.groupByKey().mapValues(
        lambda words: sorted(words, key=lambda x: x[1], reverse=True))

    # Convert to DataFrame
    word_count_df_title = spark.createDataFrame(sorted_word_count_rdd_title.flatMap(
        lambda x: [(x[0], word, count) for word, count in x[1]]), ["topic", "word", "count"])
    word_count_df_headline = spark.createDataFrame(sorted_word_count_rdd_headline.flatMap(
        lambda x: [(x[0], word, count) for word, count in x[1]]), ["topic", "word", "count"])

    # Convert to Pandas DataFrame
    word_count_pd_title = word_count_df_title.toPandas()
    word_count_pd_headline = word_count_df_headline.toPandas()

    # Save to CSV
    word_count_pd_title.to_csv("output/per_topic_word_counts_title.csv", index=False)
    word_count_pd_headline.to_csv("output/per_topic_word_counts_headline.csv", index=False)