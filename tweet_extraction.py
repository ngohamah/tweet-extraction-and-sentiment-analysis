'''
    Ngoh Rodney Amah

    I received help from Elijah Boateng to complete some parts of my code.
'''

import tweepy
import time
import pandas as pd
from textblob import TextBlob

access_token = "1162860923259883521-gzhKfDRsM7K9BCVsY20gpSf2T4FN24"
access_token_secret = "81C6jXzuRW9lNC2xmIDQyx0ry329Z8Mv0R9xeRLd1Vnud"
api_key = "1XTCdc3geOxmexHTD6cFhAxzU"
api_secret = "o1zBvQ619k6bESI8GwsY7y5hQiew4lvELpH9ShA1w3zW9XpCe9"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAGEvhgEAAAAA7vA5p5CnSs9tpwdUP0Vx6nM9tDg%3DuV0ITZOMP8j0T4cQ9rE7UrrAPMNa01ppdt4GLfyIFU4Quqkxl2"

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["Africa", "Ghana"]


# producer = KafkaProducer(bootstrap_servers='localhost:9092')


class MyStream(tweepy.StreamingClient):

    organized_data = {"Tweet ID": [], "Tweet Text": [], "Tweet link": [], "Sentiment": []}
    count = 0

    def on_connect(self):
        print("Connected")

    def on_tweet(self, tweet):
        if tweet.referenced_tweets is None:
            data = tweet.data  # data is a dictionary

            tweet_id = data['id']
            tweet_text = data['text']
            tweet_link = "https://twitter.com/twitter/status/" + tweet_id

            '''
                The sentiment property returns a namedtuple of the form Sentiment(polarity, 
                subjectivity). The polarity score is a float within the range [-1.0, 1.0].
                1 - means it's positive, -1 - means its negative, 0 - means its neutral... 
                The subjectivity is a float within the range [0.0, 1.0] where 0.0 is very 
                objective and 1.0 is very subjective.

            '''

            sentiment = TextBlob(tweet_text).sentiment

            if sentiment.polarity >= 0.5:
                sentiment = "Positive"
            elif -0.1 <= sentiment.polarity < 0.5:
                sentiment = "Neutral"
            else:
                sentiment = "Negative"

            self.organized_data["Tweet ID"].append(tweet_id)
            self.organized_data["Tweet Text"].append(tweet_text)
            self.organized_data["Tweet link"].append(tweet_link)
            self.organized_data["Sentiment"].append(sentiment)

            print(self.organized_data, '\n\n\n')

            time.sleep(0.5)
            if self.count == 100:
                tweet_data = pd.DataFrame(self.organized_data)
                tweet_data.to_csv('tweet_sentiments.csv')
                quit()
            else:
                self.count += 1
                pass

stream = MyStream(bearer_token=bearer_token)

for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields=["referenced_tweets"])
