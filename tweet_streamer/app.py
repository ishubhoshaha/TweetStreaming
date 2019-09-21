import json
import os
import boto3
import pprint
import time
from datetime import datetime
from chalice import Chalice
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler 
from tweepy import Stream

app = Chalice(app_name='tweet_streamer')


# Consumer API keys
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""


AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
AWS_REGION_NAME = "us-east-1"

SEQUENCE_TOKENS = {}
log_group_name_for_origin = "/aws/lambda/tweet_streamer-dev"
tweet_origin_log_stream = "TweetOrigin"

class Utlity():

    def get_authentication_token(self):
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        return auth


    def get_timestamp_utc(self) -> int:
        return int(datetime.utcnow().strftime('%s')) * 1000

    def get_sequence_token(self, cloudwatch, log_group: str, log_stream: str):
        token = SEQUENCE_TOKENS.get(log_stream)
        if not token:
            result = cloudwatch.describe_log_streams(
                logGroupName=log_group,
                logStreamNamePrefix=log_stream,
                limit=1
            )
            token = result['logStreams'][0].get('uploadSequenceToken')

        SEQUENCE_TOKENS[log_stream] = token
        return token

    def upload_orgin(self, cloudwatch, log_group, log_stream, data):
        try:
            params = {
                'logGroupName': log_group,
                'logStreamName': log_stream,
                'logEvents': [{
                    'timestamp': self.get_timestamp_utc(),
                    'message': json.dumps(data)
                }],
            }
            token = self.get_sequence_token(cloudwatch, log_group, log_stream)
            
            if token:
                params['sequenceToken'] = token
            response = cloudwatch.put_log_events(**params)
            print(data)
            SEQUENCE_TOKENS[log_stream] = response['nextSequenceToken']
        except Exception as E:
            print(E)



class Listener(StreamListener):
    def __init__(self):
        self.utility = Utlity()
        self.cloudwatch = boto3.client('logs', aws_access_key_id = AWS_ACCESS_KEY, 
                                aws_secret_access_key = AWS_SECRET_KEY, 
                                region_name = AWS_REGION_NAME)
        self.sqs = boto3.client('sqs', aws_access_key_id = AWS_ACCESS_KEY, 
                                aws_secret_access_key = AWS_SECRET_KEY, 
                                region_name = AWS_REGION_NAME)
    
    def on_connect(self):
        print("Start Streaming")

    def on_data(self, data):
        processed_data = json.loads(data)

        msg_attr = {
                'id': {
                    'DataType': 'String',
                    'StringValue': processed_data['id_str']
                },
                'created_at': {
                    'DataType': 'String',
                    'StringValue': processed_data['created_at']
                },
                'user_id': {
                    'DataType': 'String',
                    'StringValue': processed_data['user']['id_str']
                },
                'tweet': {
                    'DataType': 'String',
                    'StringValue': str(processed_data['text']) if processed_data['text'] else "empty"
                }
            }
        print(processed_data)
        response = self.sqs.send_message(
            QueueUrl= 'https://sqs.us-east-1.amazonaws.com/460344103511/TweetStream' ,
            DelaySeconds=0,
            MessageAttributes= msg_attr,
            MessageBody=(
                'Tweet information '
                '000'
            )
        )
        self.utility.upload_orgin(self.cloudwatch, log_group_name_for_origin, tweet_origin_log_stream, processed_data['user']['location'])


    def on_error(self, status):
        print (status)
    
    def on_disconnect(self, notice):
        print(notice)
        print("Stop Streaming")


class Streamer():
    def __init__(self):
        self.stream = Stream(Utlity().get_authentication_token(), Listener())
    def start_tweet_streaming(self, filter_list):
        self.stream.filter(track=filter_list)
    def stop_tweet_streaming(self):
        self.stream.disconnect()

@app.route('/{operation}')
def index(operation):
    twitter_streamer = Streamer()
    if operation == 'start':
        filter_list = []
        keyword = app.current_request.query_params.get('filter')
        filter_list.append(keyword)
        twitter_streamer.start_tweet_streaming(filter_list, is_async=True)
        return {"msg": "Streaming Started"}
    else:
        twitter_streamer.stop_tweet_streaming()
        return {"msg": "Streaming Stopped"}


@app.route('/get-tweet')
def get_stored_tweet():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TweetStream')
    tweets = table.scan()
    pprint.pprint(tweets["Items"])
    return {"tweet": tweets["Items"]}