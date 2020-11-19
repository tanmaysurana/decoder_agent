FROM node:12-alpine

WORKDIR /usr/src/app

COPY package.json package-lock.json ./

RUN apk add --no-cache make gcc g++ python && \
    npm install --production --silent && \
    npm cache clean --force && \
    apk del make gcc g++ python

RUN mkdir input output

COPY src/ ./src/

CMD ["node", "src/agent.js"]
