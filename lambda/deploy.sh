#!/bin/bash


#!/bin/bash

rm -rf ./libs
pip3 install -r requirements.txt -t ./libs

rm *.zip
zip top_tracks.zip -r *

aws s3 rm s3://kihong-spotify-lambda/spotify.zip
aws s3 cp ./spotify.zip s3://kihong-spotify-lambda/spotify.zip
aws lambda update-function-code --function-name spotify-lambda --s3-bucket kihong-spotify-lambda --s3-key spotify.zip
