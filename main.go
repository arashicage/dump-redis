package main

import (
	"common/ini"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)

// go run main.go 172.30.11.230:6379 04* 01,02 10000
// ./dump-redis 172.30.11.230:6379 04* 01,02 10000
// ./dump-redis 172.30.11.230:6379 04* all 10000
// ./dump-redis 172.30.11.230:6379 99* all -1 过滤长度不是15的
// -1 将获取全部

func main() {

	conf := "dump.conf"

	cfg := ini.DumpAll(conf)

	passwd := cfg["DEFAULT:"+"passwd"]
	scan := cfg["DEFAULT:"+"scan"]
	lenH, _ := strconv.Atoi(cfg["DEFAULT:"+"len"])

	for i := 1; i < len(os.Args); i++ {
		fmt.Printf("%12d %s\n", i, os.Args[i])
	}

	fmt.Printf("%12s %s\n", "passwd", passwd)
	fmt.Printf("%12s %s\n", "scan", scan)
	fmt.Printf("%12s %d\n", "len", lenH)

	fmt.Printf("========================================\n")

	url := os.Args[1]
	pattern := os.Args[2]

	count, err := strconv.Atoi(os.Args[4])
	if err != nil {
		logger.Printf("4th parameter invalid. %s\n", err)
		return
	}

	rs, err := redis.Dial("tcp", url)
	if err != nil {
		logger.Printf("create connection to redis fail. url: %s err:%s\n", url, err)
		return
	}

	if passwd != "" {
		if _, err := rs.Do("AUTH", passwd); err != nil {
			logger.Println("AUTH fail.")
			rs.Close()
			return
		}
	}

	cur_init := "0" //cur 初始值
	cur_curt := "0" //cur 当前值
	cur_next := "x" //cur 下一值
	i := 0
	for {

		repl, err := redis.Values(rs.Do(scan, cur_curt, "MATCH", pattern, "COUNT", "1000"))
		if err != nil {
			logger.Printf("command scan fail. command: %s err:%s\n", "scan"+cur_curt+"match"+pattern, err)
			return
		}

		for _, val := range repl {

			switch val.(type) {
			case []uint8:

				cur_next, _ = redis.String(val, nil)

			case []interface{}:

				keys, err := redis.Strings(val, nil)
				if err != nil {
					logger.Printf("get keys from scan fail. %s quit process. \n", err)
					cur_next = cur_init
					break
				}
				for _, key := range keys {
					i++
					v, err := redis.Values(rs.Do("HGETALL", key))
					if err != nil {
						logger.Printf("hgetall fail. %s quit process. \n", err)
						cur_next = cur_init
						break
					}

					m, err := redis.StringMap(v, err)
					if err != nil {
						logger.Printf("map hash fail. %s quit process. \n", err)
						cur_next = cur_init
						break
					}

					if os.Args[3] == "all" {
						if len(m) != lenH {
							fmt.Printf("------------- %-12d -------------\n", i)
							fmt.Printf("%s \n", key)
							for k, v := range m {
								fmt.Printf("%s\t%s \n", k, v)
							}
						}

					} else {
						fmt.Printf("%12d ", i)
						fmt.Printf("%s ", key)
						fields := strings.Split(os.Args[3], ",")
						for _, field := range fields {
							if m[field] != "" {
								fmt.Printf("%s ", m[field])
							} else {
								fmt.Printf("%s ", "<nil>")
							}

						}
					}

					fmt.Println()

					if i == count { // 到了要取数量，退出本次循环，设置整体循环结束条件
						cur_next = cur_init
						break
					}

				}

			}
		}

		if cur_next == cur_init {
			break
		} else {
			cur_curt = cur_next
		}

	}

}
