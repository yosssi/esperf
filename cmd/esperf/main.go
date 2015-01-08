package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Threads int
	Mins    int
	Hosts   []string
	Index   string
	Type    string
}

func (c *config) url() string {
	return "http://" + c.Hosts[rand.Intn(len(c.Hosts))] + "/" + c.Index + "/" + c.Type + "/_search"
}

type result struct {
	start      time.Time
	end        time.Time
	err        error
	statusCode int
}

func (r *result) strings() []string {
	var s []string

	s = append(s, strconv.Itoa(int(r.end.Sub(r.start)/1000000)))

	if r.err == nil {
		s = append(s, "0")
		s = append(s, "")
		s = append(s, strconv.Itoa(r.statusCode))
	} else {
		s = append(s, "1")
		s = append(s, r.err.Error())
		s = append(s, "")
	}

	return s
}

func (r *result) writeCSVTo(w *csv.Writer) error {
	muW.Lock()
	defer muW.Unlock()

	if err := w.Write(r.strings()); err != nil {
		return err
	}

	w.Flush()

	return nil
}

// Flags
var (
	confPath  = flag.String("conf", "", "configuration file path")
	condsPath = flag.String("conds", "", "conditions file paths")
)

// Config
var conf config

// Conditions
var conds []interface{}

// Wait group
var wg sync.WaitGroup

// Close signaling channel
var closec = make(chan struct{})

// CSV writer
var muW sync.Mutex
var w *csv.Writer

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
}

func main() {
	confd, err := ioutil.ReadFile(*confPath)
	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(confd, &conf); err != nil {
		panic(err)
	}

	condsd, err := ioutil.ReadFile(*condsPath)
	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(condsd, &conds); err != nil {
		panic(err)
	}

	f, err := os.Create("esperf.log")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w = csv.NewWriter(f)

	for i := 0; i < conf.Threads; i++ {
		wg.Add(1)
		go load()
	}

	fmt.Printf("%+v\n%+v\n", conf, conds)

	time.Sleep(time.Duration(conf.Mins) * time.Minute)

	close(closec)

	wg.Wait()

	fmt.Printf("%+v\n%+v\n", conf, conds)
}

func load() {
	defer wg.Done()

	for {
		select {
		case <-closec:
			return
		default:
			rslt := &result{
				start: time.Now(),
			}

			resp, err := http.Post(conf.url(), "application/json", nil)
			rslt.end = time.Now()
			if err == nil {
				rslt.statusCode = resp.StatusCode
			} else {
				rslt.err = err
			}

			rslt.writeCSVTo(w)

			time.Sleep(1 * time.Second)
		}

	}
}
