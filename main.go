package main

import (
	"log"
	"logloader/config"
	"logloader/parser"
)

var conf config.Data

func init() {
	c, err := config.Load("conf.yaml")
	if err != nil {
		log.Fatal(err)
	}
	conf = c
}

func main() {
	if err := parser.ProcessLogFile("/home/volynets/access_dynamic.log.2020122612.gz", conf); err != nil {
		log.Fatal(err)
	}

	// str := "2020-12-26T11:10:40.386249+03:00 php-master3.mrn.site.m nginx_access_parsable: 1608970239995|==|26/Dec/2020:11:10:40 +0300|==|5.255.253.148|==|05FFFD94:C898_B920398A:01BB_5FE6EFFF3E2143DAC7A|==|116213|==|a2-a4.ru|==|GET|==|/termofix-6h-nt-prizhimnaya-shajba-fischer-s-shurupom-power-fast-dlya-krepleniya-k-panelnym-materialam-i-derevu|==|HTTP/1.1|==|Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)|==|200|==|0.390"
	// var buf bytes.Buffer
	// zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// if _, err := zw.Write([]byte(str)); err != nil {
	// 	log.Fatal(err)
	// }
	// if _, err := zw.Write([]byte(str)); err != nil {
	// 	log.Fatal(err)
	// }
	// // if _, err := zw.Write([]byte("test")); err != nil {
	// // 	log.Fatal(err)
	// // }
	// if err := zw.Close(); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("bytes: %X\ngz len: %v\nstr len: %v\n", buf.Bytes(), buf.Len(), len(str))
}
