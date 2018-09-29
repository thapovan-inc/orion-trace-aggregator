package util

import "go.uber.org/zap"

var logger *zap.Logger

func GetLogger(packageName, function string) *zap.Logger {
	if logger == nil {
		SetupLoggerConfig()
	}
	//l := logger.With(zap.String("package", packageName), zap.String("function", function))
	return logger
}

func SetupLoggerConfig() {
	//config := zap.NewDevelopmentConfig()
	//fmt.Printf("%+v", config)
	//l, err := config.Build()
	//if err != nil {
	//	fmt.Println(err)
	//	panic(err)
	//}
	config := zap.NewProductionConfig()
	config.Level = GetConfig().Logger.Level
	logger, _ = config.Build()
}
