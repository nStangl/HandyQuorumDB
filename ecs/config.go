package ecs

type Config struct {
	Port       int
	ClientPort int
	Debug      bool
	Address    string
	Logfile    string
	Loglevel   string
}

const (
	readReplicationFactor  = 2
	writeReplicationFactor = 2
)
