# Crypto exchange order browser
Lookup [binance](https://www.binance.com/en/landing/data)/[okx](https://www.okx.com/pl/historical-data) 
historical order prices freely in high resolution (current limitation is missing free chart library handling 
millions of rows, so it is not as accurate as it could be but still good (for my purpose at least)) 
without having to pay $9999999.

## Features
- free [(so we can use highcharts library)](https://www.highcharts.com/forum/viewtopic.php?t=45225)
- unzipping, parsing and streaming response to your browser on the fly - fast, low memory usage etc. 
from my benchmarks - download speed is a bottleneck here
- awfully coded frontend
- optional website authentication. can be configured using environment variables: `AUTH_USER` and `AUTH_PASSWORD`.

## Preview
![Exchange order browser website preview](preview.png)

## Available installation options:

### A) Without bloat (docker):
```
$ git clone https://github.com/makindotcc/exchange-order-browser
$ cd exchange-order-browser
$ cargo run --release
```
- visit [http://127.0.0.1:2137/](http://127.0.0.1:2137/)

### B) With docker using prebuilt docker container:
Prebuilt docker container from [dockerhub](https://hub.docker.com/repository/docker/wmknc/exchange-order-browser/general)
```
$ docker pull wmknc/exchange-order-browser
$ docker run --detach wmknc/exchange-order-browser
```
- visit [http://127.0.0.1:2137/](http://127.0.0.1:2137/)

### C) Build docker container yourself:
```
$ git clone https://github.com/makindotcc/exchange-order-browser
$ cd exchange-order-browser 
$ docker build --tag exchange-order-browser .
$ docker run --detach exchange-order-browser
```
- visit [http://127.0.0.1:2137/](http://127.0.0.1:2137/)
