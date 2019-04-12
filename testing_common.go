package timetogo

import (
	"fmt"
	"io"
	"os"

	"github.com/dsoprea/go-logging"
)

var (
	TestTimeSeriesData  = []byte("some time series data")
	TestTimeSeriesData2 = []byte("X some time series data 2 X")
)

func DumpBytes(description string, rs io.ReadSeeker, position int64, count int, requireAll bool) {
	originalPosition, err := rs.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	_, err = rs.Seek(position, os.SEEK_SET)
	log.PanicIf(err)

	collected := make([]byte, count)
	ptr := collected

	j := count
	for j > 0 {
		n, err := rs.Read(ptr)
		if err == io.EOF {
			break
		}

		ptr = ptr[n:]
		j -= n
	}

	_, err = rs.Seek(originalPosition, os.SEEK_SET)
	log.PanicIf(err)

	if requireAll == true && len(collected) < count {
		log.Panicf("not enough bytes available")
	}

	fmt.Printf("DUMP(%s):", description)
	for i := 0; i < count; i++ {
		fmt.Printf(" %02x", collected[i])
	}

	fmt.Printf("\n")
}
