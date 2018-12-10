import numpy as np
import pandas as pd
import itertools

data = pd.read_csv("/home/geraldo_vera/tweets.csv", dtype=object, error_bad_lines=False)

posts = data['post']
newPosts = []

for post in posts:
    wordArr = ""
    words = str(post).split(" ")
    for word in words:
        if(not isinstance(word, float)):
            wordArr = wordArr + word + " "
    newPosts.append(wordArr)


df = pd.DataFrame(np.array(newPosts))
df.columns = ['words']

df.to_csv("wordsML.csv")

