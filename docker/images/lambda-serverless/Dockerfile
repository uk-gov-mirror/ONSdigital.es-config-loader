FROM node:10.16.3
ARG huid=9999
# Create user called "docker" with selected UID
RUN useradd --shell /bin/bash -u $huid -o -c "" -m dockeruser

RUN npm install -g --unsafe-perm=true serverless@1.54.0 serverless-step-functions
 
USER dockeruser
WORKDIR /usr/src/app