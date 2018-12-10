spark = SparkSession.builder.getOrCreate()

df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/words.csv")
df.createOrReplaceTempView("wordList")

tweetsdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/tweets.csv")
tweetsdf.createOrReplaceTempView("names")

# Returns top 10 keywords in all collected tweets.
key = spark.sql("select words, count(words) from wordList group by words order by count(words) desc")
key.show()

# Returns top 10 hashtags.
tags = spark.sql("select words, count(words) from wordList where locate('#', words)>0 group by words order by count(words) desc")
tags.show()

#Returns keywords count.
keywords = spark.sql("select words, count(words) from wordList where words='flu' OR words='zika' OR words='diarrhea' OR words='ebola' OR words='headache' OR words='measles' group by words order by count(words) desc")
ketwords.show()

#Returns the 10 screen names that were most active.
names = spark.sql("select name, count(name) from names group by name order by count(name) desc")
names.show()