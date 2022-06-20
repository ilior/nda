package main

import (
	"database/sql"
	"fmt"
	"log"
	"nda/productsImporter/internal/entity"
	"nda/productsImporter/internal/fileReader"
	"nda/productsImporter/internal/normalizer"
	"os"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Not enough parameters")
	}

	filepath := os.Args[1]

	if filepath == "" {
		log.Fatal("File path is empty")
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	cfg := mysql.NewConfig()
	cfg.User = os.Getenv("DB_USER")
	cfg.Passwd = os.Getenv("DB_PASSWD")
	cfg.Net = os.Getenv("DB_PROTOCOL")
	cfg.Addr = os.Getenv("DB_ADDR")
	cfg.DBName = os.Getenv("DB_NAME")

	db, err := sql.Open("mysql", cfg.FormatDSN())

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ping := db.Ping()

	if ping != nil {
		log.Fatal(ping)
	}

	sshconfig := &ssh.ClientConfig{
		User:            os.Getenv("SFTP_USER"),
		Auth:            []ssh.AuthMethod{ssh.Password(os.Getenv("SFTP_PASS"))},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial(os.Getenv("SFTP_PROTOCOL"), os.Getenv("SFTP_ADDR"), sshconfig)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client, err := sftp.NewClient(conn)

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	done := make(chan struct{})
	defer close(done)

	datach, errc := fileReader.Process(client, filepath, done)

	res := make(chan entity.ChLine)

	var wg sync.WaitGroup

	const numNormalizers = 5

	wg.Add(numNormalizers)

	for i := 0; i < numNormalizers; i++ {
		go func() {
			normalizer.Normalize(db, datach, res, done)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(res)
	}()

	for i := range res {
		fmt.Println(i)
	}

	if e := <-errc; e != nil {
		fmt.Println(e)
	}
}
