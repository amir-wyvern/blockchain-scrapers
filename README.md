# Network Scraper
network scraper is a program to scan the network and check transactions in dexes.

> It is recommended to use the program written in Go language

> Note : use your own influx token in the file .env

## Golang program

first of all, create a mod file
```sh
go mod init github.com/my/repo 
```
Then install requirements packages
```sh
go get ./go
```
for run :
```sh
go ./go/offlineScraper.go  [options]
```
### oprions
- start : start block number
- end : end block number
- influx : influx url
- worker : worker number
- help : help you (^ _ ^)

Example :
```sh
go ./go/offlineScraper.go  -start 21060000 -end 21070000 -worker 100
```

## Tech

NetworkScraper uses a number of open source projects to work properly:

- Golang
- Python
- InfluxDb



## License

MIT

**Free Software, Hell Yeah!**
