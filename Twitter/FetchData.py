import tweepy
import json
import twitter_credentials



class StreamListener(tweepy.StreamListener):

    #def __init__(self, fetched_tweets_filename):
    #     self.fetched_tweets_filename = fetched_tweets_filename

    def on_status(self, status):
        if status.retweeted:
            return

        text = status.text
        name = status.user.screen_name
        id_str = status.id_str
        created = status.created_at

        tweets = open("tweets.csv", "a+")
        print("\"" + str(created) + "\",\"" + str(text) + "\",\"" + str(name) + "\",\"" + id_str + "\"\n")
        tweets.write("\"" + str(created) + "\",\"" + str(text) + "\",\"" + str(name) + "\",\"" + id_str + "\"\n")

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False
if __name__ == '__main__':

    auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    #fetched_tweets_filename = open("tweets.csv", "a+")

    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=["flu", "zika", "diarrhea", "ebola", "headache", "measles"])
