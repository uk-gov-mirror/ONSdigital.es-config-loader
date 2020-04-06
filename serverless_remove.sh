#!/usr/bin/env bash

cd config-loader-repository
serverless plugin install --name serverless-pseudo-parameters
serverless plugin install --name serverless-latest-layer-version
echo Destroying serverless bundle...
serverless remove --verbose;
