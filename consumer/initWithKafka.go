// +build kafka

package consumer

func InitConsumerFromConfig() error {
	logger := util.GetLogger("consumer", "InitConsumerFromConfig")
	config := util.GetConfig()
	switch config.EventSourceConfig.Type {
	case KAFKA:
		kafkaConfig := config.EventSourceConfig.KafkaConsumerConfig
		consumer = &KafkaConsumer{ConfigMap: kafkaConfig}
		err := consumer.connect()
		if err != nil {
			logger.Debug("Error when connecting", zap.Error(err))
			return err
		} else {
			return nil
		}
	case NATS:
		natsConfig := config.EventSourceConfig.NatsConsumerConfig
		consumer = &NatsConsumer{URL: natsConfig.URL, ClientID: natsConfig.ClientID, ClusterID: natsConfig.ClusterID, groupID: natsConfig.GroupID}
		err := consumer.connect()
		if err != nil {
			logger.Debug("Error when connecting", zap.Error(err))
			return err
		} else {
			return nil
		}
	default:
		consumer = nil
		return errors.New("unable to find publisher backend configuration")
	}
}
