package main

import (
	"fmt"
	"github.com/thapovan-inc/orion-trace-aggregator/aggregator"
	"github.com/thapovan-inc/orion-trace-aggregator/bookkeeper"
	"github.com/thapovan-inc/orion-trace-aggregator/consumer"
	"github.com/thapovan-inc/orion-trace-aggregator/storage"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	fmt.Println(`
   ____       _           
  / __ \_____(_)___  ____ 
 / / / / ___/ / __ \/ __ \
/ /_/ / /  / / /_/ / / / /
\____/_/  /_/\____/_/ /_/ 
	
	`)
	fmt.Println("Loading config file from default.toml")
	util.LoadConfigFromFile("default.toml")
	util.SetupLoggerConfig()
	logger := util.GetLogger("main", "main")
	err := storage.InitStorageProviderFromConfig()
	if err != nil {
		logger.Fatal("Unable to init storage provider", zap.Error(err))
	}

	err = consumer.InitConsumerFromConfig()
	if err != nil {
		logger.Fatal("Unable to init spanDataConsumer", zap.Error(err))
	}
	spanDataConsumer, _ := consumer.GetConsumer()

	storageProvider := storage.GetStorageProvider()
	consumerControlPID := spanDataConsumer.GetControlPID()
	aggregator.InitAggregator(storageProvider, consumerControlPID)
	asyncAggregator := aggregator.GetAggregator()
	actorPID := asyncAggregator.PrepareActor()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		topic := "incoming-spans"
		err := spanDataConsumer.Subscribe(actorPID, topic)
		if err != nil {
			logger.Error("Unable to subscribe to topic ", zap.String("topic", topic), zap.Error(err))
		}
	}()
	wg.Wait()
	defer bookkeeper.Cleanup()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logger.Info("Received signal. Exiting now", zap.String("signal", sig.String()))
		spanDataConsumer.GetControlPID().Tell("sig_close")
		actorPID.Poison()
		done <- true
	}()
	<-done
}
