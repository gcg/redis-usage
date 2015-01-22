package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"
)

var (
	redisHost = flag.String("host", ":6379", "redis server host")
)

func main() {
	flag.Parse()
	fmt.Println("Connecting to redis at ", *redisHost)

	c, err := redis.Dial("tcp", *redisHost)
	defer c.Close()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Getting all the keys from redis...")

	keys, err := redis.Values(c.Do("KEYS", "*"))
	if err != nil {
		fmt.Println(err)
	}

	type Key struct {
		r int
		m int64
	}

	stats := make(map[string]*Key)

	keyCount := len(keys)
	pi := 0

	for _, key := range keys {
		pi += 1
		if pi%100 == 0 {
			fmt.Printf("\rProcessing %d/%d keys, stay tuned...", pi, keyCount)
		}
		if pi == keyCount {
			fmt.Println("")
		}
		var keyValue, ok = key.([]byte)
		if ok {
			parts := strings.Split(string(keyValue), ":")
			keyName := parts[0]

			_, ok := stats[keyName]

			if ok == false {
				e := &Key{
					r: 0,
					m: 0,
				}
				stats[keyName] = e
			}

			i := stats[keyName].r
			i += 1

			stats[keyName].r = i

			d, _ := redis.String(c.Do("DEBUG", "OBJECT", string(keyValue)))

			r, _ := regexp.Compile("serializedlength:(.*?) ")
			text := r.FindString(d)

			memoryBytesRaw := strings.Split(text, ":")
			if len(memoryBytesRaw) == 2 {
				mem := strings.Trim(memoryBytesRaw[1], " ")
				memNumber, err := strconv.ParseInt(mem, 0, 64)
				if err == nil {
					m := stats[keyName].m
					m += memNumber
					stats[keyName].m = m
				}
			}

		}
	}

	fmt.Println("All the keys processed")

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)

	fmt.Fprintln(w, "key name", "\t", "repetition", "\t", "used memory", "\t")

	var totalMemoryUsed int64

	for keyName, values := range stats {
		fmt.Fprintln(w, keyName, "\t", values.r, "\t", values.m, "\t")
		totalMemoryUsed += values.m
	}
	fmt.Fprintln(w)
	w.Flush()

	fmt.Println("Total memory used: ", totalMemoryUsed)

	fmt.Println("EOF")

}
