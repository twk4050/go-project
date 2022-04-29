package customsql

import (
	"database/sql"
	"math"
	"math/rand"

	sqlite "github.com/mattn/go-sqlite3"
)

// register functions in sql
func getrand() int64 {
	return rand.Int63()
}
// Computes x^y
func pow(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}

// Computes the standard deviation of a GROUPed BY set of values
type stddev struct {
	xs []float64
	// Running average calculation
	sum float64
	n   int64
}
func newStddev() *stddev { return &stddev{} }
func (s *stddev) Step(x float64) {
	s.xs = append(s.xs, x)
	s.sum += x
	s.n++
}
func (s *stddev) Done() float64 {
	mean := float64(s.sum) / float64(s.n)
	var sqDiff []float64
	for _, x := range s.xs {
		sqDiff = append(sqDiff, math.Pow(float64(x)-mean, 2))
	}
	var dev float64
	for _, x := range sqDiff {
		dev += x
	}
	dev /= float64(len(sqDiff))
	return math.Sqrt(dev)
}

func init(){
	sql.Register("sqlite3_custom", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.RegisterFunc("rand", getrand, false); err != nil {
				return err
			}
			if err := conn.RegisterFunc("pow", pow, false); err != nil {
				return err
			}
			if err := conn.RegisterAggregator("stddev", newStddev, true); err != nil {
				return err
			}
			return nil
		},
	})
}
