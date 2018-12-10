import numpy as np
import pandas as pd
import itertools

data = pd.read_csv("/home/geraldo_vera/tweets.csv", dtype=object, error_bad_lines=False)

posts = data['post']
newPosts = []

for post in posts:
    # print(type(post))
    # break
    wordArr = ""
    words = str(post).split(" ")
    for word in words:
        if(not isinstance(word, float)):
            wordArr = wordArr + word + " "
    #newList = list(itertools.chain(*wordArr))
    newPosts.append(wordArr)

#flattened_list = [y for x in wordArr for y in x]

df = pd.DataFrame(np.array(newPosts))
df.columns = ['words']
#print (df)

df.to_csv("wordsML.csv")

# spark = SparkSession.builder.getOrCreate()
# df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/words.csv")
# df.createOrReplaceTempView("wordList")
# key = spark.sql("select words, count(words) from wordList group by words order by count(words) desc")
# tags = spark.sql("select words, count(words) from wordList where locate('#', words)>0 group by words order by count(words) desc").show()
# keywords = spark.sql("select words, count(words) from wordList where words='flu' OR words='zika' OR words='diarrhea' OR words='ebola' OR words='headache' OR words='measles' group by words order by count(words) desc").show()

# tweetsdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/tweets.csv")
# tweetsdf.createOrReplaceTempView("names")
# names = spark.sql("select name, count(name) from names group by name order by count(name) desc")
