FROM node:16

WORKDIR /

COPY package*.json ./

RUN npm install

COPY . .

CMD ./wait-for-it.sh -s -t 60 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 60 $KAFKA_SERVER -- node producer.js
