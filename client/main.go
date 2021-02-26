package main

import (
	"database/sql"
	"fmt"
	"grpc/proto"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func dbConn() (db *sql.DB) {
	dbDriver := "mysql"
	dbUser := "raj"
	dbPass := "Rajshekar@456"
	dbName := "sekhar"
	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	return db
}

func main() {
	conn, err := grpc.Dial("localhost:4040", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := proto.NewAddServiceClient(conn)

	g := gin.Default()
	g.GET("/add/:username/:name", func(ctx *gin.Context) {
		a := ctx.Param("username")
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Parameter A"})
			return
		}

		b := ctx.Param("name")
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Parameter B"})
			return
		}

		req := &proto.Request{Username: string(a), Name: string(b)}
		if response, err := client.AddtoKafka(ctx, req); err == nil {
			ctx.JSON(http.StatusOK, gin.H{
				"result": fmt.Sprint(response.Status),
			})
			fmt.Println("Start receiving from Kafka")
			c, err := kafka.NewConsumer(&kafka.ConfigMap{
				"bootstrap.servers": "localhost:9092",
				"group.id":          "group-id-1",
				"auto.offset.reset": "earliest",
			})

			if err != nil {
				panic(err)
			}

			c.SubscribeTopics([]string{"jobs-topic1"}, nil)

			for {
				msg, err := c.ReadMessage(-1)

				if err == nil {
					fmt.Printf("Received from Kafka %s: %s\n", msg.TopicPartition, string(msg.Value))
					job := string(msg.Value)
					ctx.JSON(http.StatusOK, gin.H{
						"result1": fmt.Sprint(job),
					})
					saveJobToMySQL(job)
				} else {
					fmt.Printf("Consumer error: %v (%v)\n", err, msg)
					break
				}
			}

			c.Close()

		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error1": err.Error()})
		}
	})

	if err := g.Run(":10000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

}

func saveJobToMySQL(jobString string) {

	fmt.Println("Save to MySQL")
	db := dbConn()
	//Save data into Job struct
	s := strings.Split(jobString, "&")
	in, err := db.Prepare("INSERT INTO user(username,name) VALUES(?,?)")
	if err != nil {
		panic(err)
	}
	in.Exec(s[0], s[1])
	fmt.Printf("data %s: %s\n", s[0], s[1])

	fmt.Printf("Saved to Mysql : %s", jobString)

}
