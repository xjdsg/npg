package srv

//the config file used by partita is like - 
//	self port: port
//	backend postgres: xxx
//	username: xxx
//	passwd:	xxx
//	dbname:	xxx

type Config struct {
	//todo
}

func LoadConfigFile(file string) *Config {
	cfg := new(Config)
	//parse the file
	return cfg
}

func (cfg *Config) GetString(key string) string {
	//todo
	return nil
}
