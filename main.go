package redisforlightauth

import (
	"encoding/hex"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dchest/uniuri"
	"github.com/faurehu/lightauth"
	"github.com/go-redis/redis"
)

type database struct {
	client *redis.Client
}

func (db database) Create(r lightauth.Record) (string, error) {
	// TODO Last time we used reflect it didn't work, test this.
	rType := reflect.TypeOf(r).Name()
	id := uniuri.New()
	for {
		exists, err := db.client.Exists(rType + ":" + id).Result()
		if err != nil {
			log.Printf("redisforlightauth: Could not query for key in redis: %v\n", err)
			return "", err
		}

		if exists == 0 {
			break
		}

		id = uniuri.New()
	}

	switch v := r.(type) {
	case *lightauth.Invoice:
		v.ID = id
	case *lightauth.Client:
		v.ID = id
	case *lightauth.Route:
		v.ID = id
	case *lightauth.Path:
		v.ID = id
	}

	db.Edit(r)

	return id, nil
}

func (db database) Edit(r lightauth.Record) {
	switch v := r.(type) {
	case *lightauth.Invoice:
		key := "Invoice:" + v.ID
		db.client.HSet(key, "PaymentRequest", v.PaymentRequest)
		db.client.HSet(key, "PaymentHash", hex.EncodeToString(v.PaymentHash))
		db.client.HSet(key, "PreImage", hex.EncodeToString(v.PreImage))
		db.client.HSet(key, "Fee", strconv.Itoa(v.Fee))
		db.client.HSet(key, "Settled", strconv.FormatBool(v.Settled))
		db.client.HSet(key, "Claimed", strconv.FormatBool(v.Claimed))
		db.client.HSet(key, "ExpirationTime", v.ExpirationTime.Format("2006-01-02T15:04:05Z07:00"))

		if v.Client != nil && v.Client.ID != "" {
			db.client.SAdd("ClientInvoices:"+v.Client.ID, v.ID)
		} else if v.Path != nil && v.Path.ID != "" {
			db.client.SAdd("PathInvoices:"+v.Path.ID, v.ID)
		}
	case *lightauth.Route:
		key := "Route:" + v.ID
		db.client.HSet(key, "Name", v.Name)
		db.client.HSet(key, "Fee", strconv.Itoa(v.Fee))
		db.client.HSet(key, "MaxInvoices", strconv.Itoa(v.MaxInvoices))
		db.client.HSet(key, "Mode", v.Mode)
		db.client.HSet(key, "Period", v.Period)

		for _, w := range v.Clients {
			db.client.SAdd("RouteClients:"+v.ID, w.ID)
		}
	case *lightauth.Client:
		key := "Client:" + v.ID
		db.client.HSet(key, "Token", v.Token)
		db.client.HSet(key, "ExpirationTime", v.ExpirationTime.Format("2006-01-02T15:04:05Z07:00"))
		db.client.HSet(key, "Route", v.Route.ID)

		for _, w := range v.Invoices {
			db.client.SAdd("ClientInvoices:"+v.ID, w.ID)
		}

		db.client.SAdd("RouteClients:"+v.Route.ID, v.ID)
	case *lightauth.Path:
		key := "Path:" + v.ID
		db.client.HSet(key, "Fee", strconv.Itoa(v.Fee))
		db.client.HSet(key, "MaxInvoices", strconv.Itoa(v.MaxInvoices))
		db.client.HSet(key, "Mode", v.Mode)
		db.client.HSet(key, "URL", v.URL)
		db.client.HSet(key, "Period", v.TimePeriod)
		db.client.HSet(key, "Token", v.Token)
		db.client.HSet(key, "LocalExpirationTime", v.LocalExpirationTime.Format("2006-01-02T15:04:05Z07:00"))
		db.client.HSet(key, "SyncExpirationTime", v.SyncExpirationTime.Format("2006-01-02T15:04:05Z07:00"))

		for _, w := range v.Invoices {
			db.client.SAdd("PathInvoices:"+v.ID, w.ID)
		}
	}
}

func (db database) GetClientData() (map[string]*lightauth.Path, error) {
	store := make(map[string]*lightauth.Path)
	pathKeys, err := db.client.Keys("Path:*").Result()
	if err != nil {
		log.Printf("redisforlightauth error: Could not query keys in Redis: %v\n", err)
		return store, err
	}

	for _, pk := range pathKeys {
		pathID := strings.Split(pk, ":")[1]
		pathKey := "Path:" + pathID

		pathFeeStr, err := db.client.HGet(pathKey, "Fee").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathFee, err := strconv.Atoi(pathFeeStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		pathMaxInvoicesStr, err := db.client.HGet(pathKey, "MaxInvoices").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathMaxInvoices, err := strconv.Atoi(pathMaxInvoicesStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		pathMode, err := db.client.HGet(pathKey, "Mode").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathPeriod, err := db.client.HGet(pathKey, "Period").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathToken, err := db.client.HGet(pathKey, "Token").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathLocalExpirationTimeStr, err := db.client.HGet(pathKey, "LocalExpirationTime").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathLocalExpirationTime, err := time.Parse("2006-01-02T15:04:05Z07:00", pathLocalExpirationTimeStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		pathSyncExpirationTimeStr, err := db.client.HGet(pathKey, "SyncExpirationTime").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathSyncExpirationTime, err := time.Parse("2006-01-02T15:04:05Z07:00", pathSyncExpirationTimeStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		pathURL, err := db.client.HGet(pathKey, "URL").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		pathInvoicesIds, err := db.client.SMembers("PathInvoices:" + pathID).Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query set in Redis: %v\n", err)
			return store, err
		}

		p := lightauth.Path{
			ID:                  pathID,
			Invoices:            make(map[string]*lightauth.Invoice),
			Token:               pathToken,
			Fee:                 pathFee,
			MaxInvoices:         pathMaxInvoices,
			Mode:                pathMode,
			TimePeriod:          pathPeriod,
			LocalExpirationTime: pathLocalExpirationTime,
			SyncExpirationTime:  pathSyncExpirationTime,
			URL:                 pathURL,
		}

		store[pathURL] = &p

		for _, invoiceID := range pathInvoicesIds {
			invoiceKey := "Invoice:" + invoiceID

			invoicePaymentRequest, err := db.client.HGet(invoiceKey, "PaymentRequest").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoicePaymentHashStr, err := db.client.HGet(invoiceKey, "PaymentHash").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoicePaymentHash, err := hex.DecodeString(invoicePaymentHashStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			invoicePreImageStr, err := db.client.HGet(invoiceKey, "PreImage").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoicePreImage, err := hex.DecodeString(invoicePreImageStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			invoiceFeeStr, err := db.client.HGet(invoiceKey, "Fee").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoiceFee, err := strconv.Atoi(invoiceFeeStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			invoiceSettledStr, err := db.client.HGet(invoiceKey, "Settled").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoiceSettled, err := strconv.ParseBool(invoiceSettledStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			invoiceClaimedStr, err := db.client.HGet(invoiceKey, "Claimed").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoiceClaimed, err := strconv.ParseBool(invoiceClaimedStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			invoiceExpirationTimeStr, err := db.client.HGet(pathKey, "ExpirationTime").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			invoiceExpirationTime, err := time.Parse("2006-01-02T15:04:05Z07:00", invoiceExpirationTimeStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			p.Invoices[invoicePaymentHashStr] = &lightauth.Invoice{
				PaymentRequest: invoicePaymentRequest,
				PaymentHash:    invoicePaymentHash,
				PreImage:       invoicePreImage,
				Fee:            invoiceFee,
				Settled:        invoiceSettled,
				Claimed:        invoiceClaimed,
				Path:           &p,
				ID:             invoiceID,
				ExpirationTime: invoiceExpirationTime,
			}
		}
	}

	return store, nil
}

func (db database) GetServerData() (map[string]*lightauth.Route, error) {
	store := make(map[string]*lightauth.Route)
	routeKeys, err := db.client.Keys("Route:*").Result()
	if err != nil {
		log.Printf("redisforlightauth error: Could not query keys in Redis: %v\n", err)
		return store, err
	}

	for _, rk := range routeKeys {
		routeID := strings.Split(rk, ":")[1]
		routeKey := "Route:" + routeID

		routeName, err := db.client.HGet(routeKey, "Name").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		routeFeeStr, err := db.client.HGet(routeKey, "Fee").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		routeFee, err := strconv.Atoi(routeFeeStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		routeMaxInvoicesStr, err := db.client.HGet(routeKey, "MaxInvoices").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		routeMaxInvoices, err := strconv.Atoi(routeMaxInvoicesStr)
		if err != nil {
			log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
			return store, err
		}

		routeMode, err := db.client.HGet(routeKey, "Mode").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		routePeriod, err := db.client.HGet(routeKey, "Period").Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
			return store, err
		}

		routeClientsIds, err := db.client.SMembers("RouteClients:" + routeID).Result()
		if err != nil {
			log.Printf("redisforlightauth error: Could not query set in Redis: %v\n", err)
			return store, err
		}

		r := lightauth.Route{
			ID:      routeID,
			Clients: make(map[string]*lightauth.Client),
			RouteInfo: lightauth.RouteInfo{
				Name:        routeName,
				Fee:         routeFee,
				MaxInvoices: routeMaxInvoices,
				Mode:        routeMode,
				Period:      routePeriod,
			},
		}

		store[r.Name] = &r

		for _, clientID := range routeClientsIds {
			clientKey := "Client:" + clientID

			clientToken, err := db.client.HGet(clientKey, "Token").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			clientExpirationTimeStr, err := db.client.HGet(clientKey, "ExpirationTime").Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
				return store, err
			}

			clientExpirationTime, err := time.Parse("2006-01-02T15:04:05Z07:00", clientExpirationTimeStr)
			if err != nil {
				log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
				return store, err
			}

			r.Clients[clientToken] = &lightauth.Client{
				Token:          clientToken,
				ExpirationTime: clientExpirationTime,
				Route:          &r,
				ID:             clientID,
				Invoices:       make(map[string]*lightauth.Invoice),
			}

			clientInvoicesIds, err := db.client.SMembers("ClientInvoices:" + clientID).Result()
			if err != nil {
				log.Printf("redisforlightauth error: Could not query set in Redis: %v\n", err)
				return store, err
			}

			for _, invoiceID := range clientInvoicesIds {
				invoiceKey := "Invoice:" + invoiceID

				invoicePaymentRequest, err := db.client.HGet(invoiceKey, "PaymentRequest").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoicePaymentHashStr, err := db.client.HGet(invoiceKey, "PaymentHash").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoicePaymentHash, err := hex.DecodeString(invoicePaymentHashStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				invoicePreImageStr, err := db.client.HGet(invoiceKey, "PreImage").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoicePreImage, err := hex.DecodeString(invoicePreImageStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				invoiceFeeStr, err := db.client.HGet(invoiceKey, "Fee").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoiceFee, err := strconv.Atoi(invoiceFeeStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				invoiceSettledStr, err := db.client.HGet(invoiceKey, "Settled").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoiceSettled, err := strconv.ParseBool(invoiceSettledStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				invoiceClaimedStr, err := db.client.HGet(invoiceKey, "Claimed").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoiceClaimed, err := strconv.ParseBool(invoiceClaimedStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				invoiceExpirationTimeStr, err := db.client.HGet(clientKey, "ExpirationTime").Result()
				if err != nil {
					log.Printf("redisforlightauth error: Could not query hash field in Redis: %v\n", err)
					return store, err
				}

				invoiceExpirationTime, err := time.Parse("2006-01-02T15:04:05Z07:00", invoiceExpirationTimeStr)
				if err != nil {
					log.Printf("redisforlightauth error: Corrupted data could not be parsed: %v\n", err)
					return store, err
				}

				r.Clients[clientToken].Invoices[invoicePaymentRequest] = &lightauth.Invoice{
					PaymentRequest: invoicePaymentRequest,
					PaymentHash:    invoicePaymentHash,
					PreImage:       invoicePreImage,
					Fee:            invoiceFee,
					Settled:        invoiceSettled,
					Claimed:        invoiceClaimed,
					ExpirationTime: invoiceExpirationTime,
					Client:         r.Clients[clientToken],
				}
			}
		}
	}

	return store, nil
}

// GetDB is used to start a redis connection and then returns the dataprovider
func GetDB(addr string, pass string, db int) lightauth.DataProvider {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       db,
	})

	dp := database{client: client}

	return dp
}
