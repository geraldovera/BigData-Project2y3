import numpy as np
import pandas as pd
import itertools

data = pd.read_csv("/home/geraldo_vera/tweets.csv", dtype=object, error_bad_lines=False)

posts = data['post']

stopWords = ["rt", "i", "a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "like", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "o", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]

wordArr = []
for post in posts:
    # print(type(post))
    # break
    words = str(post).split(" ")
    for word in words:
        if(word.lower() not in stopWords):
            wordArr.append(word.lower())

#flattened_list = [y for x in wordArr for y in x]

df = pd.DataFrame(np.array(wordArr))
df.columns = ['words']
#print (df)

df.to_csv("words.csv")

# spark = SparkSession.builder.getOrCreate()
# df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/words.csv")
# df.createOrReplaceTempView("wordList")
# key = spark.sql("select words, count(words) from wordList group by words order by count(words) desc")
# tags = spark.sql("select words, count(words) from wordList where locate('#', words)>0 group by words order by count(words) desc").show()
# keywords = spark.sql("select words, count(words) from wordList where words='flu' OR words='zika' OR words='diarrhea' OR words='ebola' OR words='headache' OR words='measles' group by words order by count(words) desc").show()

# tweetsdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/tweets.csv")
# tweetsdf.createOrReplaceTempView("names")
# names = spark.sql("select name, count(name) from names group by name order by count(name) desc")

#
#
#
