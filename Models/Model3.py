import tensorflow as tf
from tensorflow import keras
import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelBinarizer, LabelEncoder
from sklearn.metrics import confusion_matrix

from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout
from keras.preprocessing import text, sequence
from keras import utils
from keras import optimizers

print("You have TensorFlow version", tf.__version__)

data = pd.read_csv("/home/geraldo_vera/cleantextlabels7.csv")
testData = pd.read_csv("/home/geraldo_vera/wordsML.csv", dtype=object, error_bad_lines=False)
#testData = testData[testData.post.apply(lambda x: isinstance(x, float))]

print(data.head())

#print(data['tags'].value_counts())

train_size = int(len(data) * .8)
print ("Train size: %d" % train_size)
print ("Test size: %d" % (len(data) - train_size))

train_posts = data['post'][:train_size]
train_tags = data['tags'][:train_size]

test_posts = data['post'][train_size:]
test_tags = data['tags'][train_size:]

project_posts_size = len(testData)
project_posts = testData['words'][:project_posts_size]

# print(" ")
#
# counter = 0
#
# for item in project_posts.iteritems():
#     print(type(item[1]))
#     counter = counter + 1
#     if(counter == 10):
#         break
#
# print(" ")

max_words = 2000
tokenize = text.Tokenizer(num_words=max_words, char_level=False)

tokenize.fit_on_texts(train_posts) # only fit on train
x_train = tokenize.texts_to_matrix(train_posts)
x_test = tokenize.texts_to_matrix(test_posts)
x_project = tokenize.texts_to_matrix(project_posts)

# Use sklearn utility to convert label strings to numbered index
encoder = LabelEncoder()
encoder.fit(train_tags)
y_train = encoder.transform(train_tags)
y_test = encoder.transform(test_tags)

# Converts the labels to a one-hot representation
num_classes = np.max(y_train) + 1
y_train = utils.to_categorical(y_train, num_classes)
y_test = utils.to_categorical(y_test, num_classes)


# Inspect the dimenstions of our training and test data (this is helpful to debug)
print('x_train shape:', x_train.shape)
print('x_test shape:', x_test.shape)
print('y_train shape:', y_train.shape)
print('y_test shape:', y_test.shape)

# This model trains very quickly and 2 epochs are already more than enough
# Training for more epochs will likely lead to overfitting on this dataset
# You can try tweaking these hyperparamaters when using this model with your own data
batch_size = 64
epochs = 5


# Build the model
model = Sequential()
model.add(Dense(512, input_shape=(max_words,)))
model.add(Activation('relu'))
model.add(Dense(256, input_shape=(max_words,)))
model.add(Activation('relu'))
model.add(Dropout(0.5))
model.add(Dense(num_classes))
model.add(Activation('softmax'))

model.compile(loss='categorical_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])


# model.fit trains the model
# The validation_split param tells Keras what % of our training data should be used in the validation set
# You can see the validation loss decreasing slowly when you run this
# Because val_loss is no longer decreasing we stop training to prevent overfitting
history = model.fit(x_train, y_train,
                    batch_size=batch_size,
                    epochs=epochs,
                    verbose=1,
                    validation_split=0.1)

# Evaluate the accuracy of our trained model
score = model.evaluate(x_test, y_test,
                       batch_size=batch_size, verbose=1)
print('Test score:', score[0])
print('Test accuracy:', score[1])

# Here's how to generate a prediction on individual examples
text_labels = encoder.classes_

for i in range(50):
    prediction = model.predict(np.array([x_test[i]]))
    predicted_label = text_labels[np.argmax(prediction)]
    print(test_posts.iloc[i][:50], "...")
    print('Actual label:' + str(test_tags.iloc[i]))
    print("Predicted label: " + str(predicted_label) + "\n")

# postsArr = []
#
# for i in range(project_posts_size):
#     prediction = model.predict(np.array([x_project[i]]))
#     predicted_label = text_labels[np.argmax(prediction)]
#     post = project_posts.iloc[i]
#     row = (str(post), str(predicted_label))
#     postsArr.append(row)
#
# df = pd.DataFrame(np.array(postsArr))
# df.columns = ['Post', 'PredictedLabel']
# #print (df)
#
# df.to_csv("classifiedData3.csv")

#tweetsdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/tweets.csv")
