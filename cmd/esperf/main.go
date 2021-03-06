package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
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
	cond       *cond
	err        error
	statusCode int
	hits       int
}

func (r *result) strings() []string {
	var s []string

	if r.start.IsZero() {
		s = append(s, "")
	} else {
		s = append(s, r.start.Format(timeFormat))
	}

	if r.end.IsZero() {
		s = append(s, "")
	} else {
		s = append(s, r.end.Format(timeFormat))
	}

	if r.start.IsZero() || r.end.IsZero() {
		s = append(s, "")
	} else {
		s = append(s, strconv.Itoa(int(r.end.Sub(r.start)/1000000)))
	}

	s = append(s, strconv.Itoa(r.cond.ID))

	if r.err == nil {
		s = append(s, "0")
		s = append(s, "")
	} else {
		s = append(s, "1")
		s = append(s, r.err.Error())
	}

	s = append(s, strconv.Itoa(r.statusCode))
	s = append(s, strconv.Itoa(r.hits))

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

func (r *result) setResponse(resp *http.Response) {
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		r.err = err
		return
	}

	var rb respBody
	if err := json.Unmarshal(b, &rb); err != nil {
		r.err = err
		return
	}

	r.hits = rb.Hits.Total
}

type respBody struct {
	Hits struct {
		Total int
	}
}

type cond struct {
	ID   int
	Cond interface{}
}

// Time format
const timeFormat = "2006-01-02 15:04:05.999"

// Flags
var (
	confPath  = flag.String("conf", "", "configuration file path")
	condsPath = flag.String("conds", "", "conditions file paths")
	dataPath  = flag.String("data", "", "data file paths")
)

// Config
var conf config

// Conditions
var conds []cond

// Data
var data map[string][]string

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

	datad, err := ioutil.ReadFile(*dataPath)
	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(datad, &data); err != nil {
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

	time.Sleep(time.Duration(conf.Mins) * time.Minute)

	close(closec)

	wg.Wait()
}

func load() {
	defer wg.Done()

	for {
		select {
		case <-closec:
			return
		default:
			wg.Add(1)
			go post()
		}

		time.Sleep(1 * time.Second)
	}
}

func post() {
	defer wg.Done()

	rslt := new(result)

	defer rslt.writeCSVTo(w)

	r, cnd, err := reqBody()
	rslt.cond = cnd
	if err != nil {
		rslt.err = err
		return
	}

	rslt.start = time.Now()
	resp, err := http.Post(conf.url(), "application/json", r)
	rslt.end = time.Now()
	if err != nil {
		rslt.err = err
		return
	}

	rslt.statusCode = resp.StatusCode

	if resp.StatusCode != http.StatusOK {
		return
	}

	rslt.setResponse(resp)
}

func reqBody() (io.Reader, *cond, error) {
	cnd := conds[rand.Intn(len(conds))]

	b, err := json.Marshal(cnd.Cond)

	if err != nil {
		return nil, nil, err
	}

	s := string(b)

	for k, values := range data {
		key := "$" + k

		if strings.Index(s, key) == -1 {
			continue
		}

		s = strings.Replace(s, key, values[rand.Intn(len(values))], -1)
	}

	return bytes.NewReader([]byte(s)), &cnd, nil
}
