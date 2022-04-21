package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

/*
	some concept to understand
	- Database > Table > Rows
	- always prepare(SQL) -> execute
	- databytes unmarshal into corresponding struct type with `json` struct tags
	- sql accepts all datatype during insertion (able to insert string into REAL) (sqlite auto convert if able)
	- different volume meaning
	-- ftx: volumeUsd24h -> (sql) ftx.volume
	-- binance: volume vs QuoteAssetVolume , quoteAssetVolume -> (sql) binance.volume (response type string auto converted to REAL in sql)
*/

const ftx_endpoint = "https://ftx.com/api/futures"
const tableName_ftx = "ftx"
const tableName_binance = "binance"
const MILLION = 1_000_000

const binanceFuturesFile = "binance_USDTFutures.txt"

const dbName = "foo.db"

type entrypoint struct {
	lib  string
	proc string
}

var libNames = []entrypoint{
	{"libgo-sqlite3-extension-functions.so", "sqlite3_extension_init"},
	{"libgo-sqlite3-extension-functions.dylib", "sqlite3_extension_init"},
	{"libsqlitefunctions.dll", "sqlite3_extension_init"}, // renamed to custom dll file
}

// go will invoke init() before main()
// rename init() -> other name to invoke/comment function easily from main()
func initDB() {
	/*
		- to get data from FTX and Binance and store to database -
		1) create "foo.db" if does not exist
		2) connection to db
		3) create 2 tables ftx and binance
		4) insert data
	*/
	fmt.Println("initializing database with data...")

	// create db
	os.Remove(dbName) // removes current database
	fmt.Printf("creating %s if does not exist...\n", dbName)
	createFileIfDoesNotExist(dbName)

	// connect to db
	db, err := sql.Open("sqlite3", dbName)
	checkErr(err)
	defer db.Close()

	/* ftx */
	// create table 'ftx'
	fmt.Printf("creating table %s... \n", tableName_ftx)
	createTableFtxInDB(db)

	// insert perpetual futures into table 'ftx'
	fmt.Println("inserting data into ftx...")
	initializeDataInFtx(db)

	/* binance */
	getAllBinanceUSDTPairs() // creates a .txt file with all binance USDT futures

	/* create table 'binance' */
	fmt.Printf("creating table %s... \n", tableName_binance)
	createTableBinanceInDB(db)

	/* insert 153 symbol * n candlesticks into table 'binance'
	binance docs Kline interval, m h d w M, 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M */
	fmt.Println("inserting data into binance...")
	initializeDataInBinance(db)
}

// display data
func main() {
	sql.Register("sqlite3-extension-functions",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				for _, v := range libNames {
					if err := conn.LoadExtension(v.lib, v.proc); err == nil {
						return nil
					}
				}
				return errors.New("libgo-sqlite3-extension-functions not found")
			},
		})
	/* initialize database with data */
	// initDB()

	/* start a connection */
	db, err := sql.Open("sqlite3-extension-functions", dbName)
	checkErr(err)
	defer db.Close()

	/* display all rows / top10 coins */
	// fmt.Println("----- top 10 volume FTX -----")
	// displayTop10VolumeInFtx(db)

	// fmt.Println("----- biggest gainz 1h -----")
	// displayTop1hChangeInFtx(db, 10, "DESC")

	// fmt.Println("----- biggest loss 1h -----")
	// displayTop1hChangeInFtx(db, 10, "ASC")

	/* binance random tests */
	testQueryWithExtension(db)

}

func testQueryWithExtension(db *sql.DB) {
	// SELECT MAX(a.openTime) as latest_candle, a.name, a.volume, b.sum_volume
	// FROM binance a
	// INNER JOIN (SELECT id,sum(volume) as sum_volume,name FROM binance WHERE (id,openTime) NOT IN (SELECT id, max(openTime) FROM binance GROUP BY name) GROUP BY name) as b on a.name = b.name
	// GROUP BY a.name
	// ORDER BY a.volume DESC
	// LIMIT 5;

	// HAVING latest_candle=a.openTime AND a.volume > b.sum_volume;
	reverseSQL := `SELECT reverse("hello world");`
	rows, err := db.Query(reverseSQL)
	checkErr(err)

	for rows.Next() {
		var msg string
		rows.Scan(&msg)
		fmt.Println(msg)
	}
}

func testQuery(db *sql.DB) {
	testSQL := `SELECT MAX(a.openTime) as latest_candle, a.name, a.volume, b.sum_volume
	FROM binance a
	INNER JOIN (SELECT id,sum(volume) as sum_volume,name FROM binance WHERE (id,openTime) NOT IN (SELECT id, max(openTime) FROM binance GROUP BY name) GROUP BY name) as b on a.name = b.name
	GROUP BY a.name
	ORDER BY a.volume DESC 
	LIMIT 5;`

	rows, err := db.Query(testSQL)
	checkErr(err)

	for rows.Next() {
		var openTime int
		var name string
		var volume float64
		var sum_volume float64
		rows.Scan(&openTime, &name, &volume, &sum_volume)
		fmt.Printf("%s %f %f\n", name, volume, sum_volume)
	}
}

func displayAllRowsInBinance(db *sql.DB) {
	selectAllSQL := `SELECT *
	FROM %s;`

	sql := fmt.Sprintf(selectAllSQL, tableName_binance)
	rows, err := db.Query(sql)
	checkErr(err)

	for rows.Next() {

		// rows.Scan() // TODO

	}

}

func insertRowsIntoBinance(db *sql.DB, name string, c Candlesticks) {
	openTime, open, high, low, close, closeTime, quoteAssetVolume := c[0], c[1], c[2], c[3], c[4], c[6], c[7]

	insertSQL := `INSERT INTO %s
	(name, openTime, open, high, low, close, volume, closeTime)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	;`
	sql := fmt.Sprintf(insertSQL, tableName_binance)

	statement, err := db.Prepare(sql)
	checkErr(err)

	statement.Exec(name, openTime, open, high, low, close, quoteAssetVolume, closeTime)
}

type Candlesticks [12]interface{}

// doesnt work "GET" request returns [ [123, "123"], [123, "123"] ] instead of k-v mappings
// OpenTime, Open, High, Low, Close, Volume, CloseTime, QuoteAssetVolume, NumberOfTrades = c[0] ... c[7]

type KLine_Response []Candlesticks

func initializeDataInBinance(db *sql.DB) {
	f, err := os.Open(binanceFuturesFile)
	checkErr(err)
	scanner := bufio.NewScanner(f)
	defer f.Close()

	const binance_baseurl = `https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=15m&limit=3`
	for scanner.Scan() {
		symbol := scanner.Text()
		formatted_endpoint := fmt.Sprintf(binance_baseurl, symbol)
		dataBytes, err := httpGetUrlRequestAndIORead(formatted_endpoint)
		checkErr(err)

		var kline_response KLine_Response
		json.Unmarshal(dataBytes, &kline_response)

		// c = candlesticks , c[0] = OpenTime
		for _, c := range kline_response {
			insertRowsIntoBinance(db, symbol, c)
		}
	}
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

	sql := fmt.Sprintf(createTableSQL, tableName_binance)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec()
}

func getAllBinanceUSDTPairs() {
	endpoint := "https://fapi.binance.com/fapi/v1/exchangeInfo"
	fname := "binance_USDTFutures.txt"
	f, err := os.Create(fname)
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

// 2nd param - "order" either "DESC" | "ASC"
func displayTop1hChangeInFtx(db *sql.DB, rowsToDisplay int, order string) {
	if order != "DESC" && order != "ASC" {
		panic("order needs to be either DESC | ASC for sql statement")
	}
	selectTop1hGainer := `SELECT name, last, change1h
	from %s
	ORDER BY change1h %s
	LIMIT %d;
	`

	sql := fmt.Sprintf(selectTop1hGainer, tableName_ftx, order, rowsToDisplay)

	rows, err := db.Query(sql)
	checkErr(err)
	for rows.Next() {
		var name string
		var last float64
		var change float64
		rows.Scan(&name, &last, &change)
		fmt.Printf("%12s %10.4f %12f%% \n", name, last, change*100)
	}
}

func displayTop10VolumeInFtx(db *sql.DB) {
	// unformatted
	selectTop10VolumeSQL := `SELECT name, volume
		FROM %s
		ORDER BY volume DESC
		LIMIT 10;
	`

	// formatted
	sql := fmt.Sprintf(selectTop10VolumeSQL, tableName_ftx)

	rows, err := db.Query(sql)
	checkErr(err)

	for rows.Next() {
		var name string
		var volumeUsd24h float64
		rows.Scan(&name, &volumeUsd24h)
		fmt.Printf("%12s %12.2fM \n", name, volumeUsd24h/MILLION)
	}
}

func displayAllRowsInFtx(db *sql.DB) {
	selectAllSQL := `SELECT * 
	FROM %s;
	`
	sql := fmt.Sprintf(selectAllSQL, tableName_ftx)

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
	sql := fmt.Sprintf(insertSQL, tableName_ftx)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec(c.Name, c.Last, c.Change1h, c.Change24h, c.VolumeUsd24h, c.OpenInterestUsd)
}

func initializeDataInFtx(db *sql.DB) {
	dataBytes, err := httpGetUrlRequestAndIORead(ftx_endpoint)
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

	sql := fmt.Sprintf(createTableSQL, tableName_ftx)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec()
}

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

type FTX_Response struct {
	Success     string        `json:"success"`
	FuturesList []FTX_Futures `json:"result"`
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

/* for helper functions read from bottom to top */
