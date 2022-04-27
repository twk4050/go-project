package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/robfig/cron/v3"

	"project/coins/tgbotwrapper"
)

/*
	some concept to understand
	- Database > Table > Rows
	- always prepare(SQL) -> execute
	- databytes unmarshal into corresponding struct type with `json` struct tags
	- sqlite accepts all datatype during insertion (able to insert string into REAL) (sqlite auto convert if able)
	- different volume meaning
	-- ftx: volumeUsd24h -> (sql) ftx.volume
	-- binance: volume vs QuoteAssetVolume , quoteAssetVolume -> (sql) binance.volume (response type string auto converted to REAL in sql)
	https://binance-docs.github.io/apidocs/futures/en/#change-log
	- insert 153 symbol * n candlesticks into table 'binance'
	binance docs Kline interval, m h d w M, 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
*/

type FTX_Response struct {
	Success     string        `json:"success"`
	FuturesList []FTX_Futures `json:"result"`
}

type FTX_Futures struct {
	Name            string  `json:"name"`
	Perpetual       bool    `json:"perpetual"`
	Last            float64 `json:"last"`
	Index           float64 `json:"index"`
	Mark            float64 `json:"mark"`
	Change1h        float64 `json:"change1h"`
	Change24h       float64 `json:"change24h"`
	ChangeBod       float64 `json:"changeBod"`
	VolumeUsd24h    float64 `json:"volumeUsd24h"`
	Volume          float64 `json:"volume"`
	OpenInterest    float64 `json:"openInterest"`
	OpenInterestUsd float64 `json:"openInterestUsd"`
}

type Binance_Futures_KLine_Response []Candlesticks
type Candlesticks [12]interface{} // OpenTime, Open, High, Low, Close, Volume, CloseTime, QuoteAssetVolume, NumberOfTrades = c[0] ... c[7]

const dbName = "foo.db"
const TABLENAME_FTX = "ftx"
const TABLENAME_BINANCE = "binance"
const LIST_OF_BINANCE_USDT_FUTURES_FILENAME = "binanceFutures.txt"

const MILLION = 1_000_000

const FTX_ENDPOINT = "https://ftx.com/api/futures"



func main() {
	/* loading env */
	err := godotenv.Load()
	checkErr(err)
	TOKEN_API := os.Getenv("TOKEN_API")
	MY_CHAT_ID_INT64, _ := strconv.ParseInt(os.Getenv("MY_CHAT_ID"), 0, 64)
	tgbotwrapper.SendMessage(TOKEN_API, MY_CHAT_ID_INT64, "starting program from computer!!!")

	fmt.Print("starting program ")
	printCurrentTime()
	initRequiredFiles()

	/* start a connection, note the ...-extension-functions */
	db, err := sql.Open("sqlite3", dbName)
	checkErr(err)
	defer db.Close()

	/* cron job */
	c := cron.New(cron.WithParser(cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)))

	/* number of symbols = n, k = number of candles to retrieve, X = interval 3m,15m,1h,1d ... */
	/* binance, createTable and insert n * k rows of X interval */
	// interval := "15m"
	// func() {
	// 	printCurrentTime()
	// 	fmt.Println("initializing data from binance")
	// 	dropTableInDB(db, TABLENAME_BINANCE)
	// 	createTableBinanceInDB(db)
	// 	initializeDataInBinance(db, interval, 99) //*** skips last candle becos not closed yet // 153 symbols * 20 candles took 20sec
	// 	displayTop24HVolumeInBinance(db)          // not accurate becos missing out 1 candle

	// }()

	// /* every X interval, insert new X-interval candle */
	// c.AddFunc("1 0-59/15 * * * *", func() {
	// 	printCurrentTime()
	// 	initializeDataInBinance(db, interval, 1) //
	// 	displayTop24HVolumeInBinance(db)
	// })

	/* ftx, every X interval, create table and insert value */
	c.AddFunc("0-59/15 0-59/1 * * * *",
		func() {
			printCurrentTime()
			dropTableInDB(db, TABLENAME_FTX)
			createTableFtxInDB(db)
			initializeDataInFtx(db)
			displayTop10VolumeInFtx(db)
			fmt.Println("----- biggest gainz 24h -----")
			displayChangeInFtx(db, 24, 5, "DESC")
			fmt.Println("----- biggest gainz 1h -----")
			displayChangeInFtx(db, 1, 5, "DESC")
			fmt.Println("----- biggest loss 24h -----")
			displayChangeInFtx(db, 24, 5, "ASC")
			fmt.Println("----- biggest loss 1h -----")
			displayChangeInFtx(db, 1, 5, "ASC")

			// var sb strings.Builder
			// top10Vol := displayTop10VolumeInFtx(db)
			// top5Gainer24H := displayChangeInFtx(db, 24, 5, "DESC")
			// sb.WriteString(top10Vol)
			// sb.WriteString(top5Gainer24H)
			// textMessage := sb.String()
			// tgbotwrapper.SendMessage(TOKEN_API, MY_CHAT_ID_INT64, textMessage)
		})

	c.Start()

	select {}
}

// return if within_1sd / within_2sd
func testQueryWithExtension(db *sql.DB) {
	testSQL := `
	SELECT MAX(a.openTime) as latest_c, a.name, a.volume as latest_c_vol, b.sd_1
	FROM binance a
	   INNER JOIN (SELECT id, name, sum(volume) as sum_vol, count(volume) as count, 
					avg(volume) as avg_vol, 
					stdev(volume) as stdev_vol, 
					(avg(volume) + 1*stdev(volume)) as sd_1, 
					(avg(volume) + 2*stdev(volume)) as sd_2
					FROM binance
					WHERE (id, name, openTime) NOT IN (
						SELECT id, name, max(openTime) FROM binance
						GROUP BY name)
					GROUP BY name) as b on a.name = b.name
	GROUP BY a.name
	HAVING a.volume > b.sd_1
	ORDER BY a.volume DESC
	LIMIT 10;`

	rows, err := db.Query(testSQL)
	checkErr(err)
	fmt.Printf("%10s %20s %20s \n", "name", "latestcandlevolume", "68%-CI-1*sd+mean")
	for rows.Next() {
		var openTime int
		var name string
		var latest_c_volume float64
		var sd1 float64
		rows.Scan(&openTime, &name, &latest_c_volume, &sd1)
		fmt.Printf("%10s %20f %20f\n", name, latest_c_volume, sd1)
	}
}

/* Binance helper functions below */
func displayTop24HVolumeInBinance(db *sql.DB) {

	testSQL := `
	SELECT name, sum(volume) as sum_volume from binance
	WHERE datetime(round(openTime/1000), 'unixepoch') > datetime('now', '-1 day')
	GROUP BY name
	ORDER BY sum(volume) DESC
	LIMIT 10;
	`

	rows, err := db.Query(testSQL)
	checkErr(err)
	fmt.Println("----- Binance Top 20 Volume -----")
	fmt.Printf("%12s %12s \n", "name", "sum_volume_in_M")

	for rows.Next() {
		var name string
		var sum_volume float64
		rows.Scan(&name, &sum_volume)
		fmt.Printf("%12s %12.4f \n", name, sum_volume/MILLION)
	}
}

func insertRowsIntoBinance(db *sql.DB, name string, c Candlesticks) {
	openTime, open, high, low, close, closeTime, quoteAssetVolume := c[0], c[1], c[2], c[3], c[4], c[6], c[7]

	insertSQL := `INSERT INTO %s
	(name, openTime, open, high, low, close, volume, closeTime)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	;`
	sql := fmt.Sprintf(insertSQL, TABLENAME_BINANCE)

	statement, err := db.Prepare(sql)
	checkErr(err)

	statement.Exec(name, openTime, open, high, low, close, quoteAssetVolume, closeTime)
}

func initializeDataInBinance(db *sql.DB, interval string, limit int) {
	limit = limit + 1 // this function will always discard last candle, having this makes more sense in main()

	f, err := os.Open(LIST_OF_BINANCE_USDT_FUTURES_FILENAME)
	checkErr(err)
	scanner := bufio.NewScanner(f)
	defer f.Close()

	endpoint := `https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d`
	for scanner.Scan() {
		symbol := scanner.Text()
		formatted_endpoint := fmt.Sprintf(endpoint, symbol, interval, limit)
		dataBytes, err := httpGetUrlRequestAndIORead(formatted_endpoint)
		checkErr(err)

		var kline_response Binance_Futures_KLine_Response
		json.Unmarshal(dataBytes, &kline_response)

		// c = candlesticks
		lastIndex := len(kline_response) - 1
		for i, c := range kline_response {
			// last c not closed yet not a complete candle
			if i == lastIndex {
				continue
			}
			insertRowsIntoBinance(db, symbol, c)
		}
	}
}

func dropTableInDB(db *sql.DB, tableName string) {
	dropTableSQL := `DROP TABLE IF EXISTS %s;`
	sql := fmt.Sprintf(dropTableSQL, tableName)
	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec()
}

func createTableBinanceInDB(db *sql.DB) {
	createTableSQL := `CREATE TABLE IF NOT EXISTS %s(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		openTime int,
		open REAL,
		high REAL,
		low REAL,
		close REAL,
		volume REAL,
		closeTime int
	);`

	sql := fmt.Sprintf(createTableSQL, TABLENAME_BINANCE)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec()
}

func getAllBinanceUSDTPairs() {
	endpoint := "https://fapi.binance.com/fapi/v1/exchangeInfo"
	f, err := os.Create(LIST_OF_BINANCE_USDT_FUTURES_FILENAME)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	dataBytes, err := httpGetUrlRequestAndIORead(endpoint)
	if err != nil {
		panic(err)
	}

	var response struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
		} `json:"symbols"`
	}

	json.Unmarshal(dataBytes, &response)

	fmt.Println("Total Number of Binance USDT-futures pairs:", len(response.Symbols))
	for _, s := range response.Symbols {
		if strings.HasSuffix(s.Symbol, "USDT") {
			f.WriteString(s.Symbol + "\n")
		}
	}
}

func initRequiredFiles() {
	// create db
	os.Remove(dbName) // removes current database
	fmt.Printf("creating %s if does not exist...\n", dbName)
	createFileIfDoesNotExist(dbName)

	// creates a .txt file with all binance USDT futures
	getAllBinanceUSDTPairs()
}

/* FTX helper functions below */
// 2nd param - "order" either "DESC" | "ASC"
func displayChangeInFtx(db *sql.DB, change int, rowsToDisplay int, order string) string {
	if change != 1 && change != 24 {
		panic("change needs to be either 1 or 24 for sql statement")
	}
	if order != "DESC" && order != "ASC" {
		panic("order needs to be either DESC | ASC for sql statement")
	}

	selectTopGainerSQL := `SELECT name, last, change1h, change24h
	from %s
	ORDER BY change%dh %s
	LIMIT %d;
	`

	sql := fmt.Sprintf(selectTopGainerSQL, TABLENAME_FTX, change, order, rowsToDisplay)

	var sb strings.Builder
	columnTitle := fmt.Sprintf("%12s %10s %14s%% %14s%% \n", "name", "last", "1hChange", "24hChange")
	sb.WriteString(columnTitle)

	rows, err := db.Query(sql)
	checkErr(err)
	fmt.Print(columnTitle)
	for rows.Next() {
		var name string
		var last float64
		var change1h float64
		var change24h float64
		rows.Scan(&name, &last, &change1h, &change24h)
		rowsData := fmt.Sprintf("%12s %10.4f %14.4f%% %14.4f%% \n", name, last, change1h*100, change24h*100)
		
		sb.WriteString(rowsData)
		fmt.Printf("%12s %10.4f %14.4f%% %14.4f%% \n", name, last, change1h*100, change24h*100)
	}

	return sb.String()
}

func displayTop10VolumeInFtx(db *sql.DB) string {
	// unformatted
	selectTop10VolumeSQL := `SELECT name, volume
		FROM %s
		ORDER BY volume DESC
		LIMIT 10;
	`

	// formatted
	sql := fmt.Sprintf(selectTop10VolumeSQL, TABLENAME_FTX)

	var sb strings.Builder
	header := "----- FTX Top 10 Volume -----\n"
	columnTitle := fmt.Sprintf("%12s %12s \n", "name", "volume in M")
	sb.WriteString(header)
	sb.WriteString(columnTitle)

	fmt.Print(header)
	fmt.Print(columnTitle)
	rows, err := db.Query(sql)
	checkErr(err)
	for rows.Next() {
		var name string
		var volumeUsd24h float64
		rows.Scan(&name, &volumeUsd24h)
		rowsData := fmt.Sprintf("%12s %12.2fM \n", name, volumeUsd24h/MILLION)

		sb.WriteString(rowsData)
		fmt.Print(rowsData)
	}

	return sb.String()
}

func displayAllRowsInFtx(db *sql.DB) {
	selectAllSQL := `SELECT * 
	FROM %s;
	`
	sql := fmt.Sprintf(selectAllSQL, TABLENAME_FTX)

	rows, err := db.Query(sql)
	checkErr(err)
	for rows.Next() {
		var id int
		var name string
		var last float64
		var change1h float64
		var change24h float64
		var volumeUsd24h float64
		var oiUsd float64

		// rows.Scan must provide equal args length as column in database
		rows.Scan(&id, &name, &last, &change1h, &change24h, &volumeUsd24h, &oiUsd)
		fmt.Println(id, name, last, change1h, change24h, volumeUsd24h, oiUsd)
	}
}

func insertRowsIntoFtx(db *sql.DB, c FTX_Futures) {
	// double check
	if !c.Perpetual {
		return
	}
	insertSQL := `INSERT INTO %s
	(name, last, change1h, change24h, volume, oi)
	VALUES (?, ?, ?, ?, ?, ?)
	;`
	sql := fmt.Sprintf(insertSQL, TABLENAME_FTX)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec(c.Name, c.Last, c.Change1h, c.Change24h, c.VolumeUsd24h, c.OpenInterestUsd)
}

func initializeDataInFtx(db *sql.DB) {
	dataBytes, err := httpGetUrlRequestAndIORead(FTX_ENDPOINT)
	checkErr(err)

	var FTX_Response FTX_Response
	json.Unmarshal(dataBytes, &FTX_Response)

	/* insert rows in ftx */
	for _, coin := range FTX_Response.FuturesList {
		if coin.Perpetual {
			insertRowsIntoFtx(db, coin)
		}
	}
}

func createTableFtxInDB(db *sql.DB) {
	createTableSQL := `CREATE TABLE IF NOT EXISTS %s(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		last REAL,
		change1h REAL,
		change24h REAL,
		volume REAL,
		oi REAL
	);`

	sql := fmt.Sprintf(createTableSQL, TABLENAME_FTX)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec()
}

// common helper functions below
func fileExistInCWD(fname string) bool {
	directoryList, err := os.ReadDir("./")
	checkErr(err)
	for _, dir := range directoryList {
		if dir.Name() == fname {
			return true
		}
	}
	return false
}

func createFileIfDoesNotExist(fname string) {
	doesFileExist := fileExistInCWD(fname)
	if !doesFileExist {
		os.Create(fname)
	}
}

// TODO: look for alternative, 'log' library?
func printCurrentTime() {
	h, m, s := time.Now().Clock()
	fmt.Printf("%d:%d:%d \n", h, m, s)
}

func httpGetUrlRequestAndIORead(url string) ([]byte, error) {
	response, err := http.Get(url)
	checkErr(err)

	responseData, err := ioutil.ReadAll(response.Body)
	checkErr(err)

	return responseData, nil
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

/* read from bottom to top */
