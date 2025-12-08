package dht

// DefaultBootstrapNodes returns a list of well-known DHT bootstrap nodes
func DefaultBootstrapNodes() []string {
	return []string{
		"router.bittorrent.com:6881",
		"dht.transmissionbt.com:6881",
		"router.utorrent.com:6881",
		"dht.libtorrent.org:25401",
		"dht.aelitis.com:6881",
	}
}

// BootstrapWithDefaults bootstraps the DHT using default bootstrap nodes
func (d *DHT) BootstrapWithDefaults() error {
	return d.Bootstrap(DefaultBootstrapNodes())
}
