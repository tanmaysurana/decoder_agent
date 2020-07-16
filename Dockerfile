FROM node:12

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install

COPY agent.js .

CMD ["node", "agent.js"]
