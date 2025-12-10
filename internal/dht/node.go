package dht

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// NodeID represents a 160-bit DHT node ID
type NodeID [20]byte

// Node represents a DHT node in the network
type Node struct {
	ID       NodeID
	IP       net.IP
	Port     int
	LastSeen time.Time
}

// DHT implements the BitTorrent DHT protocol (BEP 5)
type DHT struct {
	nodeID       NodeID
	port         int
	conn         *net.UDPConn
	routingTable *RoutingTable
	transactions map[string]*Transaction
	transID      uint16
	logger       *slog.Logger

	// Peer storage: info_hash -> list of peers
	peers map[string][]Peer

	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// Peer represents a peer discovered via DHT
type Peer struct {
	IP   net.IP
	Port int
}

// Transaction represents a pending DHT query
type Transaction struct {
	ID       string
	Query    string
	Response chan *Message
	Timeout  time.Time
}

// NewDHT creates a new DHT node
func NewDHT(port int, logger *slog.Logger) (*DHT, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Generate random node ID
	var nodeID NodeID
	if _, err := rand.Read(nodeID[:]); err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}

	dht := &DHT{
		nodeID:       nodeID,
		port:         port,
		routingTable: NewRoutingTable(nodeID),
		transactions: make(map[string]*Transaction),
		peers:        make(map[string][]Peer),
		stopCh:       make(chan struct{}),
		logger:       logger,
	}

	return dht, nil
}

// Start starts the DHT node
func (d *DHT) Start() error {
	// Bind UDP socket
	addr := &net.UDPAddr{
		Port: d.port,
		IP:   net.IPv4zero,
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return fmt.Errorf("failed to bind UDP socket: %w", err)
	}

	d.conn = conn
	d.logger.Info("DHT node started", "nodeID", d.nodeID.String(), "port", d.port)

	// Start message handler
	d.wg.Add(1)
	go d.messageLoop()

	// Start maintenance tasks
	d.wg.Add(1)
	go d.maintenanceLoop()

	return nil
}

// Stop stops the DHT node
func (d *DHT) Stop() error {
	close(d.stopCh)

	if d.conn != nil {
		d.conn.Close()
	}

	d.wg.Wait()
	d.logger.Info("DHT node stopped")
	return nil
}

// Bootstrap bootstraps the DHT by connecting to known nodes
func (d *DHT) Bootstrap(bootstrapNodes []string) error {
	if len(bootstrapNodes) == 0 {
		return fmt.Errorf("no bootstrap nodes provided")
	}

	d.logger.Info("bootstrapping DHT", "nodes", len(bootstrapNodes))

	for _, nodeAddr := range bootstrapNodes {
		// Resolve address
		addr, err := net.ResolveUDPAddr("udp4", nodeAddr)
		if err != nil {
			d.logger.Warn("failed to resolve bootstrap node", "addr", nodeAddr, "error", err)
			continue
		}

		// Send find_node query for our own ID to populate routing table
		node := &Node{
			IP:   addr.IP,
			Port: addr.Port,
		}

		if err := d.findNode(node, d.nodeID); err != nil {
			d.logger.Debug("bootstrap find_node failed", "addr", nodeAddr, "error", err)
			continue
		}
	}

	// Wait a bit for responses
	time.Sleep(2 * time.Second)

	nodeCount := d.routingTable.Len()
	d.logger.Info("bootstrap complete", "nodes", nodeCount)

	if nodeCount == 0 {
		return fmt.Errorf("failed to bootstrap: no nodes found")
	}

	return nil
}

// GetPeers finds peers for a given info hash
func (d *DHT) GetPeers(infoHash [20]byte) ([]Peer, error) {
	d.mu.RLock()
	infoHashStr := hex.EncodeToString(infoHash[:])
	if peers, exists := d.peers[infoHashStr]; exists {
		d.mu.RUnlock()
		return peers, nil
	}
	d.mu.RUnlock()

	d.logger.Debug("looking up peers for info hash", "infoHash", infoHashStr)

	// Find closest nodes to the info hash
	target := NodeID(infoHash)
	closest := d.routingTable.FindClosest(target, 8)

	if len(closest) == 0 {
		return nil, fmt.Errorf("no nodes in routing table")
	}

	// Query closest nodes
	var peers []Peer
	visited := make(map[string]bool)

	for _, node := range closest {
		if visited[node.ID.String()] {
			continue
		}
		visited[node.ID.String()] = true

		// Send get_peers query
		resp, err := d.getPeers(node, infoHash)
		if err != nil {
			d.logger.Debug("get_peers failed", "node", node.String(), "error", err)
			continue
		}

		// Check if we got peers
		if resp.Peers != nil {
			peers = append(peers, resp.Peers...)
		}

		// Add new nodes to search
		if resp.Nodes != nil {
			for _, newNode := range resp.Nodes {
				if !visited[newNode.ID.String()] {
					d.routingTable.AddNode(&newNode)
				}
			}
		}
	}

	// Store peers for future lookups
	if len(peers) > 0 {
		d.mu.Lock()
		d.peers[infoHashStr] = peers
		d.mu.Unlock()
	}

	return peers, nil
}

// AnnouncePeer announces that we have a torrent
func (d *DHT) AnnouncePeer(infoHash [20]byte, port int) error {
	d.logger.Debug("announcing peer", "infoHash", hex.EncodeToString(infoHash[:]), "port", port)

	// Find closest nodes to the info hash
	target := NodeID(infoHash)
	closest := d.routingTable.FindClosest(target, 8)

	if len(closest) == 0 {
		return fmt.Errorf("no nodes in routing table")
	}

	// Announce to closest nodes
	announced := 0
	for _, node := range closest {
		if err := d.announcePeer(node, infoHash, port); err != nil {
			d.logger.Debug("announce_peer failed", "node", node.String(), "error", err)
			continue
		}
		announced++
	}

	if announced == 0 {
		return fmt.Errorf("failed to announce to any nodes")
	}

	d.logger.Info("announced to DHT", "nodes", announced)
	return nil
}

// messageLoop handles incoming DHT messages
func (d *DHT) messageLoop() {
	defer d.wg.Done()

	buf := make([]byte, 65536)

	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		d.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, addr, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			d.logger.Warn("failed to read UDP packet", "error", err)
			continue
		}

		// Parse message
		msg, err := ParseMessage(buf[:n])
		if err != nil {
			d.logger.Debug("failed to parse DHT message", "error", err)
			continue
		}

		// Handle message
		if err := d.handleMessage(msg, addr); err != nil {
			d.logger.Debug("failed to handle message", "error", err)
		}
	}
}

// maintenanceLoop performs periodic maintenance tasks
func (d *DHT) maintenanceLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.performMaintenance()
		}
	}
}

// performMaintenance performs routing table maintenance
func (d *DHT) performMaintenance() {
	// Clean up old transactions
	d.mu.Lock()
	now := time.Now()
	for id, tx := range d.transactions {
		if now.After(tx.Timeout) {
			close(tx.Response)
			delete(d.transactions, id)
		}
	}
	d.mu.Unlock()

	// Refresh routing table buckets
	d.routingTable.Refresh()
}

// Helper methods

// NodeID.String returns hex-encoded node ID
func (id NodeID) String() string {
	return hex.EncodeToString(id[:])
}

// Node.String returns string representation of a node
func (n *Node) String() string {
	return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

// Distance calculates XOR distance between two node IDs
func Distance(a, b NodeID) NodeID {
	var result NodeID
	for i := 0; i < 20; i++ {
		result[i] = a[i] ^ b[i]
	}
	return result
}

// Compare compares two node IDs (-1, 0, 1)
func (id NodeID) Compare(other NodeID) int {
	for i := 0; i < 20; i++ {
		if id[i] < other[i] {
			return -1
		}
		if id[i] > other[i] {
			return 1
		}
	}
	return 0
}

// CommonPrefixLen returns the number of leading bits in common
func CommonPrefixLen(a, b NodeID) int {
	dist := Distance(a, b)

	for i := 0; i < 160; i++ {
		byteIndex := i / 8
		bitIndex := 7 - (i % 8)

		if (dist[byteIndex] & (1 << bitIndex)) != 0 {
			return i
		}
	}

	return 160
}

// GenerateTransactionID generates a unique transaction ID
func (d *DHT) GenerateTransactionID() string {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.transID++
	return fmt.Sprintf("aa%04x", d.transID)
}
