from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace, collect_list, struct, expr, lower, udf
from operator import add
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations
from pyspark.sql import functions as F
from itertools import combinations
import numpy as np
import pandas as pd

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
    
# ***************************************TASK 1*************************************************************************

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
    
# ***************************************TASK 3*************************************************************************

    # Map to (topic, (sentiment_score, 1)) pairs for both SentimentTitle and SentimentHeadline
    topic_sentiment_rdd_title = data.rdd.map(lambda row: (row['Topic'], (float(row['SentimentTitle']), 1)))
    topic_sentiment_rdd_headline = data.rdd.map(lambda row: (row['Topic'], (float(row['SentimentHeadline']), 1)))

    # Reduce by key to sum sentiment scores and count occurrences
    sum_count_rdd_title = topic_sentiment_rdd_title.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    sum_count_rdd_headline = topic_sentiment_rdd_headline.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Combine results from title and headline
    combined_rdd = sum_count_rdd_title.join(sum_count_rdd_headline)

    # Calculate total sum and average
    final_rdd = combined_rdd.map(lambda x: (x[0], (x[1][0][0] + x[1][1][0], (x[1][0][0] + x[1][1][0]) / (x[1][0][1] + x[1][1][1]))))

    # Convert to DataFrame
    sentiment_df = spark.createDataFrame(final_rdd.map(lambda x: (x[0], x[1][0], x[1][1])),
                                        ["Topic", "Total_Sum_Sentiment", "Avg_Sentiment"])

    # Convert to Pandas DataFrame
    sentiment_pd = sentiment_df.toPandas()

    # Save to CSV
    sentiment_pd.to_csv("output/topic_sentiment_summary.csv", index=False)


    # ***************************************TASK 4*************************************************************************

    # Load the word counts data
    title_word_counts_df = spark.read.csv("output/per_topic_word_counts_title.csv", header=True, inferSchema=True)
    headline_word_counts_df = spark.read.csv("output/per_topic_word_counts_headline.csv", header=True, inferSchema=True)

    # Function to extract top 100 words per topic
    def extract_top_words(df):
        return df.groupBy("topic").agg(
            collect_list(struct(col("count"), col("word"))).alias("words")
        ).withColumn(
            "top_words", expr("slice(array_sort(words, (x, y) -> int(y.count - x.count)), 1, 100)")
        ).select("topic", explode("top_words.word").alias("word"))
        
    # Extract top 100 words for titles and headlines
    top_words_titles_df = extract_top_words(title_word_counts_df)
    top_words_headlines_df = extract_top_words(headline_word_counts_df)

    # Function to calculate co-occurrence matrix
    def calculate_co_occurrence_matrix(news_data_df, top_words_df, field):
        topics = top_words_df.select("topic").distinct().collect()
        for topic in topics:
            topic_name = topic['topic']
            words = top_words_df.filter(col('topic') == topic_name).select('word').rdd.flatMap(lambda x: x).collect()
            word_pairs = list(combinations(words, 2))

            
            # Function to generate word pairs
            def generate_pairs(content):
                if content is None:
                    return []
                content_words = set(content.split(" "))
                pairs = [(w1, w2) for w1 in content_words for w2 in content_words if w1 in words and w2 in words and w1 != w2]
                return list(set(pairs))  # Remove duplicates

            # Register UDF
            pair_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

            # Apply UDF to generate word pairs for each news item
            filtered_news = news_data_df.filter(col("Topic") == topic_name)
            pairs_df = filtered_news.withColumn("WordPairs", explode(pair_udf(lower(col(field)))))

            # Count occurrences of each word pair
            pairs_count = pairs_df.groupBy("WordPairs").count()

            # Convert to Pandas DataFrame for matrix construction
            pairs_count_pandas = pairs_count.toPandas()
            matrix = pd.DataFrame(0, index=words, columns=words)
            for index, row in pairs_count_pandas.iterrows():
                word1, word2 = row["WordPairs"]
                count = row["count"]
                matrix.loc[word1, word2] = count
                matrix.loc[word2, word1] = count

            # Save matrix to CSV
            matrix.to_csv(f"output/co_occurrence_matrix_{topic_name}_{field}.csv")

    # Calculate co-occurrence matrices for titles and headlines
    calculate_co_occurrence_matrix(data, top_words_titles_df, "Title")
    calculate_co_occurrence_matrix(data, top_words_headlines_df, "Headline")

    # Stop Spark session
    spark.stop()