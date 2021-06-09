package parser

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"logloader/config"
	"net/url"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/idna"
)

var conf config.Data

type logFile interface {
	getPath() string
	getFileName() string
}

type cms struct {
	logFile
	verID int
	date  time.Time
}

type site struct {
	logFile
	domain string
	date   time.Time
}

type unsorted struct {
	logFile
	date time.Time
}

func (c cms) getPath() string {
	return conf.Paths.CMS + strconv.Itoa(c.verID) + "/"
}

func (c cms) getFileName() string {
	return c.date.Format("2006-01-02.gz")
}

func (s site) getPath() string {
	return conf.Paths.Site + s.domain + "/"
}

func (s site) getFileName() string {
	return s.date.Format("2006-01-02.gz")
}

func (u unsorted) getPath() string {
	return conf.Paths.Unsorted
}

func (u unsorted) getFileName() string {
	return u.date.Format("2006-01-02.gz")
}

// ProcessLogFile разбирает файл по пути path на части в соответствии с конфигом config
func ProcessLogFile(path string, config config.Data) (err error) {
	conf = config

	osf, err := os.Open(path)
	if err != nil {
		return
	}

	defer func() {
		cerr := osf.Close()
		if err == nil {
			err = cerr
		}
	}()

	gzr, err := gzip.NewReader(osf)
	if err != nil {
		return
	}

	defer func() {
		cerr := gzr.Close()
		if err == nil {
			err = cerr
		}
	}()

	stats, err := getStats(gzr)
	if err != nil {
		return
	}

	osf.Seek(0, io.SeekStart)
	if err = gzr.Reset(osf); err != nil {
		return
	}

	if err = writeData(gzr, stats); err != nil {
		return
	}

	return nil
}

type stats struct {
	tatal   int                 // всего объектов
	top     []logFile           // топ объектов по общей длине записей
	lineNos <-chan int          // канал с номерами строк общего файла по возрастанию, в которых содержится последняя запись объекта
	cache   map[string]struct{} // кэш валидных доменов
}

func asmd5(o interface{}) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

// getStats осуществляет проход по файлу и собирает статистику
func getStats(gzr *gzip.Reader) (stats, error) {
	lines, errors := getLines(gzr)

	// logFile - объект, информация о котором будет записана в отдельный файл
	// s - общая длина записей объекта
	// i - номер строки общего файла, в которой содержится последняя запись объекта
	m := make(map[logFile]struct {
		s int
		i int
	})
	cache := make(map[string]struct{})
	f, _ := os.Create("1.csv")
	i := 0
	for l := range lines {
		t := getType(l, cache)
		s := len(l)
		f.WriteString(fmt.Sprintln(i, s))
		if v, ok := m[t]; ok {
			s += v.s
		}
		m[t] = struct {
			s int
			i int
		}{s, i}
		i++
	}
	f.Close()
	f, _ = os.Create("2.csv")
	for _, v := range m {
		f.WriteString(fmt.Sprintln(v.i, v.s))
	}
	f.Close()
	total := len(m)

	if err, open := <-errors; open {
		return stats{}, err
	}

	// Переписываем данные из map в slice для последующей сортировки
	s := make([]struct {
		lf logFile
		s  int
		i  int
	}, len(m))
	i = 0
	for k, v := range m {
		s[i] = struct {
			lf logFile
			s  int
			i  int
		}{k, v.s, v.i}
		i++
	}

	// Находим топ объектов по общей длине записей
	sort.Slice(s, func(i, j int) bool { return s[i].s > s[j].s })
	top := make([]logFile, conf.WriteLimit)
	for i, v := range s[0:conf.WriteLimit] {
		top[i] = v.lf
	}

	// Передаем в канал номера строк по возрастанию
	sort.Slice(s, func(i, j int) bool { return s[i].i < s[j].i })
	lineNos := make(chan int)
	go func() {
		for _, v := range s {
			lineNos <- v.i
		}

		close(lineNos)
	}()

	return stats{total, top, lineNos, cache}, nil
}

func getLines(gzr *gzip.Reader) (<-chan string, <-chan error) {
	cs := make(chan string)
	ce := make(chan error)

	go func() {
		bs := bufio.NewScanner(gzr)

		for bs.Scan() {
			cs <- bs.Text()
		}
		close(cs)

		if err := bs.Err(); err != nil {
			ce <- err
		}
		close(ce)
	}()

	return cs, ce
}

func getType(line string, cache map[string]struct{}) logFile {
	a := strings.Split(line, "|==|")
	d, err := time.Parse("2006-01-02", a[0][:10])
	if err != nil {
		d = time.Now()
	}
	if len(a) < 6 {
		return unsorted{date: d}
	}
	dom := strings.TrimPrefix(strings.ToLower(a[5]), "www.")
	if _, ok := conf.CMSDomains[dom]; ok {
		if len(a) < 8 {
			return unsorted{date: d}
		}
		u, err := url.Parse(a[7])
		if err != nil {
			return unsorted{date: d}
		}
		vid, err := strconv.Atoi(u.Query().Get("ver_id"))
		if err != nil || vid == 0 {
			return unsorted{date: d}
		}
		return cms{verID: vid, date: d}
	}
	if _, ok := cache[dom]; ok {
		return site{domain: dom, date: d}
	}
	p, err := idna.ToASCII(dom)
	if err != nil {
		return unsorted{date: d}
	}
	if !conf.DomainValidator.MatchString(p) {
		return unsorted{date: d}
	}
	cache[dom] = struct{}{}
	return site{domain: dom, date: d}
}

type writers map[logFile]struct {
	gzw *gzip.Writer
	iow io.Writer
}

func writeData(gzr *gzip.Reader, s stats) error {
	w := make(writers)
	for _, lf := range s.top {
		if err := w.add(lf, true); err != nil {
			return err
		}
	}

	lines, errors := getLines(gzr)

	nextLineToWrite := <-s.lineNos
	i := 0

	f, err := os.Create("mem.prof")
	if err != nil {
		log.Fatal(err)
	}
	err = pprof.WriteHeapProfile(f)
	if err != nil {
		log.Fatal(err)
	}

	for l := range lines {
		if i < 1 {
			lf := getType(l, s.cache)
			if _, ok := w[lf]; !ok {
				if err := w.add(lf, false); err != nil {
					return err
				}
			}
			if _, err := w[lf].gzw.Write([]byte(l)); err != nil {
				return err
			}
		}
		if nextLineToWrite == i {
			nextLineToWrite = <-s.lineNos
		}
		i++
	}

	defer pprof.StopCPUProfile()

	if err, open := <-errors; open {
		return err
	}

	return nil
}

func (w writers) add(lf logFile, writeToFile bool) error {
	var iow io.Writer
	if writeToFile {
		if err := os.MkdirAll(lf.getPath(), 0755); err != nil {
			return err
		}
		osf, err := os.OpenFile(lf.getPath()+lf.getFileName(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		iow = osf
	} else {
		var b bytes.Buffer
		iow = &b
	}
	gzw, err := gzip.NewWriterLevel(iow, gzip.BestCompression)
	if err != nil {
		return err
	}
	w[lf] = struct {
		gzw *gzip.Writer
		iow io.Writer
	}{gzw, iow}
	return nil
}
