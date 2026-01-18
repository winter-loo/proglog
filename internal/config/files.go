package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("acl-model.conf")
	ACLPolicyFile        = configFile("acl-policy.csv")
)

// Returns a string path is either `$PROGLOG_HOME/filename` or `$HOME/.proglog/filename`
func configFile(filename string) string {
	if home := os.Getenv("PROGLOG_HOME"); home != "" {
		return filepath.Join(home, filename)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, ".proglog", filename)
}
