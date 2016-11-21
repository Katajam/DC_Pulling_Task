package main

import (
	"gopkg.in/mgo.v2"
	"net/http"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type Transaction struct {
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
	Time       string `json:"time_local"`
	Trade_type string `json:"type"`
}

type NewTransaction struct {
	Coin       string
	Price      string
	Quantity   string
	Time       string
	Trade_type string
}

type HistData struct {
	Exch_code           string        `json:"exch_code"`
	Primary_curr_code   string        `json:"primary_curr_code"`
	Secondary_curr_code string        `json:"secondary_curr_code"`
	Tran_type           string        `json:"type"`
	History             []Transaction `json:"history"`
}

type ReBody struct {
	Exchange_code   string `json:"exchange_code"`
	Exchange_market string `json:"exchange_market"`
	Tran_type       string `json:"type"`
}

type ReplyMaster struct {
	Data          HistData `json:data"`
	Notifications []string `json:notifications`
}

func main() {
    go CurrencyJob("ETH/BTC")
    go CurrencyJob("ETC/BTC")
    go CurrencyJob("XMR/BTC")
    go CurrencyJob("FCT/BTC")
    go CurrencyJob("XRP/BTC")
    go CurrencyJob("ZEC/BTC")
    go CurrencyJob("REP/BTC")
    go CurrencyJob("DASH/BTC")
    go CurrencyJob("STEEM/BTC")
    go CurrencyJob("GAME/BTC")
    
    for{
        time.Sleep(60 * time.Minute)
    }
}

func CurrencyJob(Currency string){
    session, err := mgo.Dial("mongodb://cmpt436:cmpt436master@127.0.0.1:27017/admin")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	fmt.Println("Loading Server start to working......")
		
	TimeLimit, _ := time.Parse("2006-01-02 15:04:05","2006-01-02 23:59:59")
	for {
		apiURL := "https://www.coinigy.com/api/v1/data"
		sendBody := ReBody{Exchange_code: "PLNX", Exchange_market: Currency, Tran_type: "history"}
		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(sendBody)

		request, err := http.NewRequest("POST", apiURL, b)
		if err != nil {
			panic(err)
		}
		request.Header.Set("X-API-SECRET", "a061cacda71b10a494225257b77e8a3d")
		request.Header.Set("X-API-KEY", "90de5e53e3a1c343daead2af623b0fcd")
		request.Header.Set("Content-Type", "application/json")

		var resp *http.Response
		resp, err = http.DefaultClient.Do(request)
		if err != nil {
			panic(err)
		}

		decoder := json.NewDecoder(resp.Body)
		var rep ReplyMaster
		err = decoder.Decode(&rep)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		//Store
		c := session.DB("local").C("Ten_Transaction")
		for i := 0; i < len(rep.Data.History); i++ {
			file := rep.Data.History[i]
			rec_time,_ := time.Parse("2006-01-02 15:04:05",file.Time)
			
			if !rec_time.After(TimeLimit) {
				fmt.Println("Time Conflict")
				break
			}
			err = c.Insert(&NewTransaction{Coin: Currency, Price: file.Price, Quantity: file.Quantity, Time: file.Time, Trade_type: file.Trade_type})
			if err != nil {
				log.Fatal(err)
			}
		}

		TimeLimit, _ = time.Parse("2006-01-02 15:04:05", rep.Data.History[0].Time)
		fmt.Println("Data loaded")
		time.Sleep(10 * time.Minute)
	}
}
