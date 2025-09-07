FROM node:20-alpine

# app dir
WORKDIR /app

# no deps needed besides amqplib
RUN npm init -y && npm i amqplib

# copy scripts
COPY consumer.js publisher.js /app/
