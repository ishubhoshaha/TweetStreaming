## Setting up project enviornmnet (Assuming virtualenv is already installed)

`virtualenv -p python3 venv`

`source venv/bin/activate`

`pip install -r requirements.txt`

## run project locally

`chalice local`

## deploy on aws lambda
`chalice deploy`


## URL Map
##### Starting Streaming from Twitter
`<host>/start?filter=<filter_keyword>`

##### Stop streaming
`<host>/stop`

##### Get Tweet
`<host>/get-tweet`
