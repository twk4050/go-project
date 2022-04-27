package sqlite3extension

import (
	"database/sql"
	"errors"

	"github.com/mattn/go-sqlite3"
)

type entrypoint struct {
	lib  string
	proc string
}

var libNames = []entrypoint{
	{"libsqlitefunctions.so", "sqlite3_extension_init"},
	{"libsqlitefunctions.dylib", "sqlite3_extension_init"},
	{"libsqlitefunctions.dll", "sqlite3_extension_init"},
}

func init() {
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
}