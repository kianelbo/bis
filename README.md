## Start mongodb:
```sh
docker run --name my-mongo -d -p 27017:27017 mongo
```
## Run the crawler
```sh
python3 main.py [2023-02-06-12]
```
## Run the API server
```sh
python3 server.py
```
