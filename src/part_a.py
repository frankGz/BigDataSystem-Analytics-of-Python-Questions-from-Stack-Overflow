'''
This script is to compute descriptive statistics of the python stack overflow data set.
For details about the data set please refer to
https://www.kaggle.com/stackoverflow/pythonquestions/home

This script will compute the following statistics
1. the top 10 most frequent tags asked by users.
2. the top 10 users who anwsered questions
3. for each user, what tags they anwsered the most
4. for each tag, who gives the most number of anwsers


@author hao li
@email hao.li.9311@gmail.com

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import matplotlib.pyplot as plt
	
## build spark session
spark = SparkSession \
    .builder \
    .appName("tag_processing") \
    .getOrCreate()


## read dataset Answers.csv
anwsers_df = spark.read.format("com.databricks.spark.csv") \
				.option("header", "True") \
				.option("sep", "|") \
				.option("multiLine","True") \
				.option("quote", "\"") \
				.option("escape", "\"") \
				.load("/app/Documents/EECS4415_Project/data/Answers.csv")

## project table to selected columns
anwsers_df = anwsers_df.select(["OwnerUserId", "ParentId"])


## read dataset Tags.csv
df = spark.read.format("com.databricks.spark.csv") \
				.option("header", "true") \
				.load("/app/Documents/EECS4415_Project/data/Tags.csv")

## join to table in order to link user id with tags
df = df.join(anwsers_df, df.Id==anwsers_df.ParentId, 'inner')


## compute the top 10 tags
df_groupby_tags = df.groupBy('Tag').count()
df_groupby_tags = df_groupby_tags.sort(desc('count'))
df_groupby_tags.limit(10).toPandas().plot.bar(x='Tag', y='count', figsize=(8, 8))
plt.savefig('top10tags.png')

## compute the top 10 users
df_groupby_ids = df.groupBy('OwnerUserId').count().filter("OwnerUserId != ''")
df_groupby_ids = df_groupby_ids.sort(desc('count'))
df_groupby_ids.limit(10).toPandas().plot.bar(x='OwnerUserId', y='count', figsize=(8,8))
plt.savefig('top10userId.png')

## compute word count groupby tag and oid
groupby_oid_tag_df = df.groupBy(['OwnerUserId', 'Tag']).count().persist()
# groupby_oid_tag_df = groupby_oid_tag_df.groupBy(['OwnerUserId', 'Tag']).max('count')
groupby_oid_tag_df = groupby_oid_tag_df.select(col('OwnerUserId'), col('Tag'), col('count').alias('otcount'))




## compute who anwsered which tag the most
maxby_tag_df = groupby_oid_tag_df.groupBy('Tag').agg(max('otcount').alias('otcount'))
maxby_tag_df = maxby_tag_df.join(groupby_oid_tag_df, ['otcount', 'Tag'], 'inner')
maxby_tag_df = maxby_tag_df.orderBy(['otcount', 'Tag'], ascending=[0, 0])
maxby_tag_df.write.format('csv').save('/app/Documents/EECS4415_Project/data/maxby_tag.csv')


## compute top tags anwsered by users
maxby_id_df = groupby_oid_tag_df.groupBy('OwnerUserId').agg(max('otcount').alias('otcount'))
maxby_id_df = maxby_id_df.join(groupby_oid_tag_df, ['otcount', 'OwnerUserId'], 'inner')
maxby_id_df = maxby_id_df.orderBy(["otcount", 'OwnerUserId'], ascending=[0, 0])
maxby_id_df.write.format('csv').save('/app/Documents/EECS4415_Project/data/maxby_id.csv')
