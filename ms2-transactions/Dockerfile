FROM node:20-alpine
RUN apk add --no-cache python3 make g++
WORKDIR /app
RUN mkdir -p /app/data
COPY package*.json ./
RUN npm install --build-from-source
COPY . .
EXPOSE 50052
CMD ["node", "index.js"]