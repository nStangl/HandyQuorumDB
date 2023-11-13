package web

type Config struct {
	Port        int
	PrivatePort int
	CacheSize   int
	Address     string
	Bootstrap   string
	Directory   string
	Logfile     string
	Loglevel    string
	DisStrategy string
}
