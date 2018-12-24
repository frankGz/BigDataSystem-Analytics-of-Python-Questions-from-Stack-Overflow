import re, nltk, string
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.linalg import *
from pyspark.sql.types import * 
from pyspark.sql.functions import *

nltk.download('stopwords')
stopwords = nltk.corpus.stopwords.words('english')

def clean(df):
    tokenizer = Tokenizer(inputCol="body", outputCol="vector")
    remover = StopWordsRemover(inputCol="vector", outputCol="body")
    df = df.withColumn('body', regexp_replace(col('body'),'<code>.*?</code>',' '))
    df = df.withColumn('body', regexp_replace(col('body'),'<.*?>',' '))
    df = df.withColumn('body', regexp_replace(col('body'),'&.*?;',' '))
    df = df.withColumn('body', regexp_replace(col('body'), "[{0}]".format(re.escape(string.punctuation)), ' '))
    df = df.withColumn('body', regexp_replace(col('body'), '[^a-zA-Z]', ' '))
    df = tokenizer.transform(df).drop('body')
    df = remover.transform(df).drop('vector')
    return df

## build spark session
spark = SparkSession \
    .builder \
    .appName("processing") \
    .getOrCreate()

## read dataset Answers.csv
anwsers_df = spark.read.format("com.databricks.spark.csv") \
				.option("header", "True") \
				.option("sep", "|") \
				.option("multiLine","True") \
				.option("quote", "\"") \
				.option("escape", "\"") \
				.load("/app/Documents/EECS4415_Project/data/Answers.csv")

anwsers_df = clean(anwsers_df)
cv = CountVectorizer(inputCol="body", outputCol="features", minDF=2.0)
model = cv.fit(anwsers_df)
transformed_anwsers_df = model.transform(anwsers_df)
transformed_anwsers_df = transformed_anwsers_df.select(['OwnerUserId', 'features'])
transformed_anwsers_df.rdd.saveAsTextFile('/app/Documents/EECS4415_Project/data/id_features')

with open('vocabulary.txt', 'w+') as v:
    for w in model.vocabulary:
        v.write(w+'\n')
