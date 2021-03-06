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
	"github.com/robfig/cron/v3"

	// sqlite "github.com/mattn/go-sqlite3"

	_ "project/coins/customsql"
	"project/coins/tgbotwrapper"
)

/*
	some concept to understand
	- insert 153 symbol * n candlesticks into table 'binance'
	- binance docs Kline interval, m h d w M, 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
	- initializing 155 symbol * 250 candles takes 1m20sec
	- every 3minute, add new candle, currently takes 13seconds
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
	CHAT_ID_INT64, _ := strconv.ParseInt(os.Getenv("CHAT_ID"), 0, 64)

	/* start a connection, sqlite3_custom from ./customsql */
	db, err := sql.Open("sqlite3_custom", dbName)
	checkErr(err)
	defer db.Close()

	/* cron job */
	c := cron.New(cron.WithParser(cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)))

	func() {
		fmt.Print("starting program ")
		tgbotwrapper.SendMessage(TOKEN_API, CHAT_ID_INT64, "starting program from computer!!!", false)
		printCurrentTime()
		initRequiredFiles()

		/* binance, createTable and insert n * k rows of X interval // n=trading-pairs, k=number of candles to retrieve, X=interval 3m,15m,1h,1d*/
		interval := "3m"
		fmt.Println("initializing data from binance")
		dropTableInDB(db, TABLENAME_BINANCE)
		createTableBinanceInDB(db)
		initializeDataInBinance(db, interval, 498) //*** skips last candle becos not closed yet // 153 symbols * 20 candles took 20sec

		/* every X interval, insert new X-interval candle */
		// not sure if time on local machine is ahead of binance servers, can only pull CLOSED candle 5second later
		c.AddFunc("5 0-59/3 * * * *", func() {
			initializeDataInBinance(db, interval, 1) //

			coinsWithUnusualVolume := testQuerys(db)
			
			var sb strings.Builder
			sb.WriteString(coinsWithUnusualVolume)

			textMessage := sb.String()
			tgbotwrapper.SendMessage(TOKEN_API, CHAT_ID_INT64, textMessage, true)
		})

		c.AddFunc("59 59 * * * *", func ()  {
			s := displayTop24HVolumeInBinance(db)

			textMessage := s
			tgbotwrapper.SendMessage(TOKEN_API, CHAT_ID_INT64, textMessage, true)
		})
	}()

	/* ftx, every X interval, create table and insert value */
	// c.AddFunc("59 59 * * * *",
	// 	func() {
	// 		printCurrentTime()
	// 		dropTableInDB(db, TABLENAME_FTX)
	// 		createTableFtxInDB(db)
	// 		initializeDataInFtx(db)

	// 		top10Vol := displayTop10VolumeInFtx(db)
	// 		// displayChangeInFtx(db, 24, 5, "DESC")  // top5 24H gainer
	// 		// displayChangeInFtx(db, 24, 5, "ASC")
	// 		top5Gainer1H := displayChangeInFtx(db, 1, 5, "DESC") // top5 1H gainer
	// 		top5Loser1H := displayChangeInFtx(db, 1, 5, "ASC")

	// 		var sb strings.Builder
	// 		sb.WriteString(top10Vol)
	// 		sb.WriteString(top5Gainer1H)
	// 		sb.WriteString(top5Loser1H)

	// 		textMessage := sb.String()
	// 		tgbotwrapper.SendMessage(TOKEN_API, CHAT_ID_INT64, textMessage, true)
	// 	})

	c.Start()

	select {}
}

// not usable yet
func testQuerys(db *sql.DB) string {
	testSQL := `SELECT a.name, a.volume, b.avg_vol
	FROM binance as a
	INNER JOIN (SELECT name, AVG(volume) as avg_vol, stddev(volume) as stddev_vol
	   FROM binance
	   WHERE
		  (openTime) NOT IN (
			 SELECT MAX(openTime) FROM binance
			 GROUP BY name
		  )
		  AND 
		  	datetime(round(closeTime/1000), 'unixepoch') > datetime('now', '-150 minute')
	   GROUP BY name
	) as b ON a.name=b.name
	WHERE
	   (a.openTime) IN (
		  SELECT max(openTime) FROM binance
		  GROUP BY name
	   )
	   AND
		  1=1
	   AND
		  a.volume > ( b.avg_vol + 3*b.stddev_vol )
	;`

	unusualVolumeHeader := "Binance Unusual Volume 3*sd\n"
	columnTitle := fmt.Sprintf("%10s %15s %15s \n", "name", "3min_vol(M)", "avg50_vol(M)") // , "sd in M")

	var sb strings.Builder
	sb.WriteString(unusualVolumeHeader)
	sb.WriteString(columnTitle)

	rows, err := db.Query(testSQL)
	checkErr(err)
	defer rows.Close() 
	for rows.Next() {
		var name string
		var volume float64
		var avg50_vol float64

		rows.Scan(&name, &volume, &avg50_vol)
		rowsData := fmt.Sprintf("%10s %14.4f %14.4f \n", name, volume/MILLION, avg50_vol/MILLION)

		sb.WriteString(rowsData)
	}

	fmt.Print(sb.String())
	return sb.String()
}

/* Binance helper functions below */
func displayTop24HVolumeInBinance(db *sql.DB) string {

	testSQL := `
	SELECT a.name, sum(a.volume) as sum_volume, b.close
    FROM binance as a
    INNER JOIN (
        SELECT name, close, max(closeTime) as closeTime from binance
        GROUP BY name
    ) as b ON a.name=b.name
	WHERE datetime(round(a.closeTime/1000), 'unixepoch') > datetime('now', '-1 day')
	GROUP BY a.name
	ORDER BY sum(a.volume) DESC
	LIMIT 10;
	`

	topVolumeHeader := "Binance Top 10 Volume\n"
	topVolumeColumnTitle := fmt.Sprintf("%10s %12s %12s \n", "name", "volume in M", "close")

	var sb strings.Builder
	sb.WriteString(topVolumeHeader)
	sb.WriteString(topVolumeColumnTitle)

	rows, err := db.Query(testSQL)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var name string
		var sum_volume float64
		var close float64
		rows.Scan(&name, &sum_volume, &close)
		rowsData := fmt.Sprintf("%10s %12.4f %12.4f \n", name, sum_volume/MILLION, close)

		sb.WriteString(rowsData)
	}

	fmt.Print(sb.String())
	return sb.String()
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
			// last c is not a complete candle, thus skipped
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

	var gainsOrLoss string = "Gains" // s = "Gains" | "Loss"
	if order == "ASC" {
		gainsOrLoss = "Loss"
	}
	unformattedTopChangeHeader := "FTX Top %d %s 24H\n"
	topChangeHeader := fmt.Sprintf(unformattedTopChangeHeader, rowsToDisplay, gainsOrLoss)
	topChangeColumnTitle := fmt.Sprintf("%12s %10s %14s%% %14s%% \n", "name", "last", "1hChange", "24hChange")

	var sb strings.Builder
	sb.WriteString(topChangeHeader)
	sb.WriteString(topChangeColumnTitle)

	rows, err := db.Query(sql)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var name string
		var last float64
		var change1h float64
		var change24h float64
		rows.Scan(&name, &last, &change1h, &change24h)
		rowsData := fmt.Sprintf("%12s %10.4f %14.4f%% %14.4f%% \n", name, last, change1h*100, change24h*100)

		sb.WriteString(rowsData)
	}

	fmt.Print(sb.String())
	return sb.String()
}

func displayTop10VolumeInFtx(db *sql.DB) string {
	// unformatted
	selectTop10VolumeSQL := `SELECT name, last, volume
		FROM %s
		ORDER BY volume DESC
		LIMIT 10;
	`

	// formatted
	sql := fmt.Sprintf(selectTop10VolumeSQL, TABLENAME_FTX)

	topVolumeHeader := "FTX Top 10 Volume\n"
	topVolumeColumnTitle := fmt.Sprintf("%12s %12s %12s \n", "name", "last", "volume in M")

	var sb strings.Builder
	sb.WriteString(topVolumeHeader)
	sb.WriteString(topVolumeColumnTitle)

	rows, err := db.Query(sql)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var name string
		var last float64
		var volumeUsd24h float64
		rows.Scan(&name, &last, &volumeUsd24h)
		rowsData := fmt.Sprintf("%12s %12.4f %12.2fM \n", name, last, volumeUsd24h/MILLION)

		sb.WriteString(rowsData)
	}

	fmt.Print(sb.String())

	return sb.String()
}

func displayAllRowsInFtx(db *sql.DB) {
	selectAllSQL := `SELECT * 
	FROM %s;
	`
	sql := fmt.Sprintf(selectAllSQL, TABLENAME_FTX)

	rows, err := db.Query(sql)
	checkErr(err)
	defer rows.Close()
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
