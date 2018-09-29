package storage

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	jerrors "github.com/juju/errors"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"go.uber.org/zap"
	"time"
)

func (p *MySqlProvider) connect() error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::connect")
	var err error
	if p.db != nil {
		logger.Warn("db is already connected. Closing and reconnecting")
		err = p.db.Close()
		if err != nil {
			return jerrors.Wrap(err, jerrors.New("error when closing old connections"))
		}
	}
	config := p.MySqlConfig
	defaultConfig := mysql.NewConfig()
	if config.Collation == "" {
		config.Collation = defaultConfig.Collation
	}
	if config.Loc == nil {
		config.Loc = defaultConfig.Loc
	}
	if config.MaxAllowedPacket == 0 {
		config.MaxAllowedPacket = defaultConfig.MaxAllowedPacket
	}
	dsn := config.FormatDSN()
	p.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return jerrors.Wrap(err, jerrors.New("unable to connect to the database"))
	}
	err = p.checkConnection()
	if err != nil {
		return jerrors.Wrap(err, jerrors.New("unable to check the database connection"))
	}
	p.db.SetMaxIdleConns(2)
	p.db.SetMaxOpenConns(32)
	p.db.SetConnMaxLifetime(16 * time.Minute)
	logger.Info("Connected to MySQL server")
	return nil
}

func (p *MySqlProvider) checkConnection() error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::checkConnection")
	result, err := p.db.Query("select 1")
	if err == nil {
		defer result.Close()
		if result.Next() {
			var value uint
			err = result.Scan(&value)
			if err == nil && value != 1 {
				err = jerrors.Errorf("Test value %d does not match with returned value %d", 1, value)
			}
		}
	}
	if err != nil {
		logger.Error("Error when testing connection", zap.Error(err))
		return jerrors.Wrap(err, jerrors.New("unable to connect to the database"))
	}
	return nil
}
