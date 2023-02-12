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
- protected against bots scanning internet using hardcoded login:password (``essa:21essa15``) into source code

## Installation

### Without bloat (docker):
```
$ git clone https://github.com/makindotcc/exchange-order-browser
$ cd exchange-order-browser
$ cargo run --release
```
- visit [http://essa:21essa15@127.0.0.1:2137/](http://essa:21essa15@127.0.0.1:2137/)

### With deprecated bloat (docker):
```
$ git clone https://github.com/makindotcc/exchange-order-browser
$ cd exchange-order-browser 
```
- start container using provided Dockerfile somehow todo: migrate to Nix
- visit [http://essa:21essa15@127.0.0.1:2137/](http://essa:21essa15@127.0.0.1:2137/)

![Exchange order browser website preview](preview.png)
