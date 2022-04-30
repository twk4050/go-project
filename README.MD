# how to run

1. create telegram bot through BotFather
2. create .env with these info

```
    CHAT_ID=<TELEGRAM_CHAT_ID>
    TOKEN_API=<TELEGRAM_BOT_API_TOKEN>
```

3. in root folder, `go run main.go`

# What n Why

- notification of crypto coin prices
- learning golang / sqlite3 database / net/http library / cron job

# How

1. Creates "foo.db"
2. (sqlite3driver) Connects to "foo.db"
3. (net/http) GET request to ftx futures endpoint
4. (sql-insert) insert rows into db
5. (sql-query) display some data
6. repeat step3-5 using **cron job**

# remarks

- cron job library not the same as [standard cron](https://en.wikipedia.org/wiki/Cron)

```
c := cron.New(cron.WithParser(cron.NewParser(
    cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
    )))
```

- "0-59/15 0-59/1 \* \* \* \*" equivalent to every 15second

# useful

- [mattn sqlite create custom func/aggr](https://github.com/mattn/go-sqlite3/blob/master/_example/custom_func/main.go)
- [mattn go-sqlite3](https://pkg.go.dev/github.com/mattn/go-sqlite3?utm_source=godoc)
