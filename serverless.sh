#!/usr/bin/env bash

# Serverless deployment

cd config-loader-deploy-repository
serverless plugin install --name serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;