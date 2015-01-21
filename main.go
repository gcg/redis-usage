package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"regexp"
	"strconv"
	"strings"
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

	for _, key := range keys {
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

			d, err := redis.String(c.Do("DEBUG", "OBJECT", string(keyValue)))
			if err != nil {
				fmt.Println(err)
			}

			r, _ := regexp.Compile("serializedlength:(.*?) ")
			text := r.FindString(d)

			mem := strings.Trim(strings.Split(text, ":")[1], " ")
			memNumber, err := strconv.ParseInt(mem, 0, 64)

			if err == nil {
				m := stats[keyName].m
				m += memNumber
				stats[keyName].m = m
			}

		}
	}

	fmt.Println("All the keys processed")

	fmt.Println("key name", ",", "repetition", ",", "used memory")
	for keyName, values := range stats {
		fmt.Println(keyName, ",", values.r, ",", values.m/1000000)
	}

	fmt.Println("EOF")

}
