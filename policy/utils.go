package policy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Write 32 bytes hash addresses consectively to the bytes.Writer given
// First, length of the array will be written, followed by a \n
// Then the byte array is written
func addrListToReader(addrs []gdplogd.Hash, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(len(addrs)))
	buf.WriteString("\n")
	for _, addr := range addrs {
		buf.Write(addr[:])
	}
}

// Read 32 bytes hash addresses consectively from Reader
// First, length of the array will be read, followed by a \n
// Then the byte array is read and returned
func addrListFromReader(buf io.Reader) ([]gdplogd.Hash, error) {
	reader := bufio.NewReader(buf)

	length_, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	length_ = length_[:len(length_)-1]

	length, err := strconv.Atoi(string(length_))
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, fmt.Errorf("Negative length message received")
	}

	result := make([]gdplogd.Hash, length)

	for i := 0; i < length; i++ {
		_, err = io.ReadFull(reader, result[i][:])
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Process begins and ends section, includes "begins\n", "ends\n"
func processBeginsEnds(body io.Reader) ([]gdplogd.Hash, []gdplogd.Hash, error) {
	reader := bufio.NewReader(body)
	line, err := reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("begins\n")) != 0 {
		log.Printf("%v", err)
		log.Printf("%v", string(line))
		return nil, nil, fmt.Errorf("Error processing message: begins")
	}

	peerBegins, err := addrListFromReader(reader)
	if err != nil {
		return nil, nil, err
	}

	line, err = reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("ends\n")) != 0 {
		return nil, nil, fmt.Errorf("Error processing message: ends")
	}

	peerEnds, err := addrListFromReader(reader)
	if err != nil {
		return nil, nil, err
	}

	return peerBegins, peerEnds, nil
}

// Convert slice to map
func addrSliceToMap(s []gdplogd.Hash) map[gdplogd.Hash]int {
	result := make(map[gdplogd.Hash]int)
	for _, item := range s {
		result[item] = 1
	}
	return result
}
