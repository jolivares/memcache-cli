package main

import (
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jolivares/memcache-cmd/Godeps/_workspace/src/github.com/codegangsta/cli"
	"github.com/jolivares/memcache-cmd/Godeps/_workspace/src/github.com/ziutek/telnet"
)

const timeout = 10 * time.Second

func main() {
	app := cli.NewApp()
	app.Name = "memcahe-cmd"

	hostFlag := cli.StringFlag{
		Name:  "host",
		Value: "127.0.0.1:11211",
		Usage: "target host",
	}

	prefixFlag := cli.StringFlag{
		Name:  "prefix",
		Value: "",
		Usage: "keys prefix",
	}

	app.Commands = []cli.Command{
		{
			Name:    "list-all",
			Aliases: []string{"l"},
			Usage:   "list all keys",
			Flags: []cli.Flag{
				hostFlag,
				prefixFlag,
			},
			Action: func(c *cli.Context) {
				listKeys(c.String("host"), c.String("prefix"))
			},
		},
	}

	app.Run(os.Args)
}

func listKeys(addr string, prefix string) {
	println("Listing keys for " + addr)
	t, err := telnet.Dial("tcp", addr)
	checkErr(err)
	t.SetUnixWriteMode(true)

	slabs := getSlabs(t)
	keys := findKeys(t, slabs, prefix)
	for i := range keys {
		log.Printf("key: %s", keys[i])
	}
}

func findKeys(t *telnet.Conn, slabs map[string]int64, prefix string) []string {
	var keys []string

	r, err := regexp.Compile(`ITEM (.+?) \[(\d+) b; (\d+) s\]`)
	checkErr(err)

	for k, v := range slabs {
		sendln(t, "stats cachedump "+k+" "+strconv.FormatInt(v, 10))
		line, e := t.ReadString('\n')
		checkErr(e)
		for !strings.HasPrefix(line, "END") {
			//			log.Printf("Got item line: %s", line)
			if r.MatchString(line) == true {
				m := r.FindStringSubmatch(line)
				key := m[1]
				if len(prefix) == 0 || (len(prefix) > 0 && strings.HasPrefix(key, prefix)) {
					keys = append(keys, key)
				}
			}

			line, e = t.ReadString('\n')
			checkErr(e)
		}
	}
	return keys
}

func getSlabs(t *telnet.Conn) map[string]int64 {
	var line string
	slabs := make(map[string]int64)

	r, err := regexp.Compile(`STAT items:(\d+):number (\d+)`) // slab id & slab items
	checkErr(err)

	sendln(t, "stats items")
	line, e := t.ReadString('\n')
	checkErr(e)
	for !strings.HasPrefix(line, "END") {
		//		log.Printf("Got slab line: %s", line)
		if r.MatchString(line) == true {
			m := r.FindStringSubmatch(line)
			i, e1 := strconv.ParseInt(m[2], 10, 64)
			checkErr(e1)
			slabs[m[1]] = i
		}

		line, e = t.ReadString('\n')
		checkErr(e)
	}
	return slabs
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln("Error:", err)
	}
}

func sendln(t *telnet.Conn, s string) {
	checkErr(t.SetWriteDeadline(time.Now().Add(timeout)))
	buf := make([]byte, len(s)+1)
	copy(buf, s)
	buf[len(s)] = '\n'
	_, err := t.Write(buf)
	checkErr(err)
}
