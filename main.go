package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

/*
	some concept to understand
	- Database > Table > Rows
	- always prepare(SQL) -> execute
	- databytes unmarshal into corresponding struct type with `json` struct tags
*/

const ftx_endpoint = "https://ftx.com/api/futures"
const tableName_ftx = "ftx"
const tableName_binance = "binance"
const MILLION = 1_000_000

func main() {
	/* create db if not exist */
	dbName := "foo.db"
	os.Remove(dbName) // removes current database
	createFileIfDoesNotExist(dbName)

	/* start a connection */
	db, err := sql.Open("sqlite3", dbName)
	checkErr(err)
	defer db.Close()

	/* create table name 'ftx' in foo.db */
	createTableFtxInDB(db)

	/* http GET request */
	fmt.Println("getting data from ftx endpoint ...")
	dataBytes, err := httpGetUrlRequestAndIORead(ftx_endpoint)
	checkErr(err)

	var FTX_Response FTX_Response
	json.Unmarshal(dataBytes, &FTX_Response)

	FTX_futuresList := FTX_Response.FuturesList

	/* insert rows in coins */
	fmt.Println("inserting rows into coins ...")
	for _, coin := range FTX_futuresList {
		if coin.Perpetual {
			insertRowsIntoFtx(db, coin.Name, coin.Last, coin.Change1h, coin.Change24h, coin.VolumeUsd24h, coin.OpenInterestUsd)
		}
	}

	/* display all rows / top10 coins */
	// displayAllRowsInFtx(db)
	fmt.Println("----- top 10 volume FTX -----")
	displayTop10VolumeInFtx(db)

	fmt.Println("----- biggest gainz 1h -----")
	displayTop1hChangeInFtx(db, 10, "DESC")

	fmt.Println("----- biggest loss 1h -----")
	displayTop1hChangeInFtx(db, 10, "ASC")

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

func insertRowsIntoFtx(db *sql.DB, name string, last, change1h, change24h, volume, oi float64) {
	insertSQL := `INSERT INTO %s
	(name, last, change1h, change24h, volume, oi)
	VALUES (?, ?, ?, ?, ?, ?)
	;`

	sql := fmt.Sprintf(insertSQL, tableName_ftx)

	statement, err := db.Prepare(sql)
	checkErr(err)
	statement.Exec(name, last, change1h, change24h, volume, oi)
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

func createFileIfDoesNotExist(fname string) {
	doesFileExist := fileExistInCWD(fname)
	if !doesFileExist {
		fmt.Println("file does not exist, creating", fname)
		os.Create(fname)
	}
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

type FTX_Response struct {
	Success     string    `json:"success"`
	FuturesList []Futures `json:"result"`
}

type Futures struct {
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
