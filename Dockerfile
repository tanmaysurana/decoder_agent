FROM node:14-alpine

WORKDIR /usr/src/app

COPY package.json package-lock.json ./

RUN apk add --no-cache make gcc g++ python3 && \
    npm install --production --silent && \
    npm install pm2 -g && \
    npm cache clean --force && \
    apk del make gcc g++ python3

RUN mkdir input output

COPY src/ ./src/

CMD ["pm2-runtime", "src/agent.js"]