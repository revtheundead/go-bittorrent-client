package dht

import (
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/revtheundead/revtorrent/internal/protocol/bencode"
)

// Message types
const (
	QueryMsg    = "q"
	ResponseMsg = "r"
	ErrorMsg    = "e"
)

// Query types
const (
	PingQuery         = "ping"
	FindNodeQuery     = "find_node"
	GetPeersQuery     = "get_peers"
	AnnouncePeerQuery = "announce_peer"
)

// Message represents a DHT message
type Message struct {
	Type         string                 // "q", "r", or "e"
	TransactionID string                // Transaction ID
	Query        string                 // Query type (for queries)
	Args         map[string]interface{} // Query arguments
	Response     map[string]interface{} // Response data
	Error        []interface{}          // Error info

	// Parsed response fields
	Nodes []Node
	Peers []Peer
	Token string
}

// ParseMessage parses a bencoded DHT message
func ParseMessage(data []byte) (*Message, error) {
	decoded, err := bencode.Decode(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("message must be a dictionary")
	}

	msg := &Message{}

	// Parse message type
	if msgType, ok := dict["y"].(string); ok {
		msg.Type = msgType
	} else {
		return nil, fmt.Errorf("missing message type")
	}

	// Parse transaction ID
	if txID, ok := dict["t"].(string); ok {
		msg.TransactionID = txID
	}

	// Parse based on message type
	switch msg.Type {
	case QueryMsg:
		// Query message
		if q, ok := dict["q"].(string); ok {
			msg.Query = q
		}
		if args, ok := dict["a"].(map[string]interface{}); ok {
			msg.Args = args
		}

	case ResponseMsg:
		// Response message
		if resp, ok := dict["r"].(map[string]interface{}); ok {
			msg.Response = resp
			msg.parseResponse()
		}

	case ErrorMsg:
		// Error message
		if err, ok := dict["e"].([]interface{}); ok {
			msg.Error = err
		}
	}

	return msg, nil
}

// parseResponse parses response-specific fields
func (m *Message) parseResponse() {
	if m.Response == nil {
		return
	}

	// Parse nodes (compact node info)
	if nodesStr, ok := m.Response["nodes"].(string); ok {
		m.Nodes = parseCompactNodes([]byte(nodesStr))
	}

	// Parse peers (compact peer info)
	if valuesRaw, ok := m.Response["values"].([]interface{}); ok {
		for _, v := range valuesRaw {
			if peerStr, ok := v.(string); ok {
				peers := parseCompactPeers([]byte(peerStr))
				m.Peers = append(m.Peers, peers...)
			}
		}
	}

	// Parse token (for announce_peer)
	if token, ok := m.Response["token"].(string); ok {
		m.Token = token
	}
}

// Encode encodes a message to bencode
func (m *Message) Encode() ([]byte, error) {
	dict := make(map[string]interface{})

	dict["t"] = m.TransactionID
	dict["y"] = m.Type

	switch m.Type {
	case QueryMsg:
		dict["q"] = m.Query
		dict["a"] = m.Args

	case ResponseMsg:
		dict["r"] = m.Response

	case ErrorMsg:
		dict["e"] = m.Error
	}

	encoded, err := bencode.Encode(dict)
	if err != nil {
		return nil, err
	}

	return []byte(encoded), nil
}

// RPC methods

// ping sends a ping query to a node
func (d *DHT) ping(node *Node) error {
	txID := d.GenerateTransactionID()

	msg := &Message{
		Type:          QueryMsg,
		TransactionID: txID,
		Query:         PingQuery,
		Args: map[string]interface{}{
			"id": string(d.nodeID[:]),
		},
	}

	_, err := d.sendQuery(node, msg, 5*time.Second)
	return err
}

// findNode sends a find_node query
func (d *DHT) findNode(node *Node, target NodeID) error {
	txID := d.GenerateTransactionID()

	msg := &Message{
		Type:          QueryMsg,
		TransactionID: txID,
		Query:         FindNodeQuery,
		Args: map[string]interface{}{
			"id":     string(d.nodeID[:]),
			"target": string(target[:]),
		},
	}

	resp, err := d.sendQuery(node, msg, 5*time.Second)
	if err != nil {
		return err
	}

	// Add returned nodes to routing table
	for _, n := range resp.Nodes {
		d.routingTable.AddNode(&n)
	}

	return nil
}

// getPeers sends a get_peers query
func (d *DHT) getPeers(node *Node, infoHash [20]byte) (*Message, error) {
	txID := d.GenerateTransactionID()

	msg := &Message{
		Type:          QueryMsg,
		TransactionID: txID,
		Query:         GetPeersQuery,
		Args: map[string]interface{}{
			"id":        string(d.nodeID[:]),
			"info_hash": string(infoHash[:]),
		},
	}

	return d.sendQuery(node, msg, 5*time.Second)
}

// announcePeer sends an announce_peer query
func (d *DHT) announcePeer(node *Node, infoHash [20]byte, port int) error {
	// First get a token via get_peers
	resp, err := d.getPeers(node, infoHash)
	if err != nil {
		return err
	}

	if resp.Token == "" {
		return fmt.Errorf("no token received")
	}

	// Now announce with the token
	txID := d.GenerateTransactionID()

	msg := &Message{
		Type:          QueryMsg,
		TransactionID: txID,
		Query:         AnnouncePeerQuery,
		Args: map[string]interface{}{
			"id":        string(d.nodeID[:]),
			"info_hash": string(infoHash[:]),
			"port":      int64(port),
			"token":     resp.Token,
		},
	}

	_, err = d.sendQuery(node, msg, 5*time.Second)
	return err
}

// sendQuery sends a query and waits for response
func (d *DHT) sendQuery(node *Node, msg *Message, timeout time.Duration) (*Message, error) {
	// Create transaction
	tx := &Transaction{
		ID:       msg.TransactionID,
		Query:    msg.Query,
		Response: make(chan *Message, 1),
		Timeout:  time.Now().Add(timeout),
	}

	d.mu.Lock()
	d.transactions[tx.ID] = tx
	d.mu.Unlock()

	// Encode and send message
	data, err := msg.Encode()
	if err != nil {
		return nil, err
	}

	addr := &net.UDPAddr{
		IP:   node.IP,
		Port: node.Port,
	}

	if _, err := d.conn.WriteToUDP(data, addr); err != nil {
		d.mu.Lock()
		delete(d.transactions, tx.ID)
		d.mu.Unlock()
		return nil, err
	}

	// Wait for response
	select {
	case resp := <-tx.Response:
		d.mu.Lock()
		delete(d.transactions, tx.ID)
		d.mu.Unlock()
		return resp, nil

	case <-time.After(timeout):
		d.mu.Lock()
		delete(d.transactions, tx.ID)
		d.mu.Unlock()
		return nil, fmt.Errorf("query timeout")
	}
}

// handleMessage handles an incoming DHT message
func (d *DHT) handleMessage(msg *Message, addr *net.UDPAddr) error {
	switch msg.Type {
	case QueryMsg:
		return d.handleQuery(msg, addr)

	case ResponseMsg:
		return d.handleResponse(msg)

	case ErrorMsg:
		d.logger.Debug("received error", "error", msg.Error)
		return nil

	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleQuery handles an incoming query
func (d *DHT) handleQuery(msg *Message, addr *net.UDPAddr) error {
	// Extract querying node's ID
	var nodeID NodeID
	if id, ok := msg.Args["id"].(string); ok && len(id) == 20 {
		copy(nodeID[:], id)

		// Add querying node to routing table
		node := &Node{
			ID:       nodeID,
			IP:       addr.IP,
			Port:     addr.Port,
			LastSeen: time.Now(),
		}
		d.routingTable.AddNode(node)
	}

	var response map[string]interface{}

	switch msg.Query {
	case PingQuery:
		response = map[string]interface{}{
			"id": string(d.nodeID[:]),
		}

	case FindNodeQuery:
		if targetStr, ok := msg.Args["target"].(string); ok && len(targetStr) == 20 {
			var target NodeID
			copy(target[:], targetStr)

			closest := d.routingTable.FindClosest(target, K)
			compactNodes := encodeCompactNodes(closest)

			response = map[string]interface{}{
				"id":    string(d.nodeID[:]),
				"nodes": string(compactNodes),
			}
		}

	case GetPeersQuery:
		if infoHashStr, ok := msg.Args["info_hash"].(string); ok && len(infoHashStr) == 20 {
			infoHashHex := hex.EncodeToString([]byte(infoHashStr))

			// Check if we have peers
			d.mu.RLock()
			peers, hasPeers := d.peers[infoHashHex]
			d.mu.RUnlock()

			response = map[string]interface{}{
				"id":    string(d.nodeID[:]),
				"token": "aoeusnth", // Simple token (should be more secure in production)
			}

			if hasPeers && len(peers) > 0 {
				// Return peers
				values := make([]interface{}, 0)
				for _, peer := range peers {
					compact := encodeCompactPeer(peer)
					values = append(values, string(compact))
				}
				response["values"] = values
			} else {
				// Return closest nodes
				var target NodeID
				copy(target[:], infoHashStr)
				closest := d.routingTable.FindClosest(target, K)
				response["nodes"] = string(encodeCompactNodes(closest))
			}
		}

	case AnnouncePeerQuery:
		// Store the peer info
		if infoHashStr, ok := msg.Args["info_hash"].(string); ok && len(infoHashStr) == 20 {
			port := addr.Port
			if portArg, ok := msg.Args["port"].(int64); ok {
				port = int(portArg)
			}

			peer := Peer{
				IP:   addr.IP,
				Port: port,
			}

			infoHashHex := hex.EncodeToString([]byte(infoHashStr))

			d.mu.Lock()
			d.peers[infoHashHex] = append(d.peers[infoHashHex], peer)
			d.mu.Unlock()

			response = map[string]interface{}{
				"id": string(d.nodeID[:]),
			}
		}
	}

	// Send response
	if response != nil {
		resp := &Message{
			Type:          ResponseMsg,
			TransactionID: msg.TransactionID,
			Response:      response,
		}

		data, err := resp.Encode()
		if err != nil {
			return err
		}

		_, err = d.conn.WriteToUDP(data, addr)
		return err
	}

	return nil
}

// handleResponse handles an incoming response
func (d *DHT) handleResponse(msg *Message) error {
	d.mu.RLock()
	tx, exists := d.transactions[msg.TransactionID]
	d.mu.RUnlock()

	if !exists {
		// Unknown transaction - ignore
		return nil
	}

	// Send response to waiting goroutine
	select {
	case tx.Response <- msg:
	default:
	}

	return nil
}

// Helper functions

// parseCompactNodes parses compact node info (26 bytes per node)
func parseCompactNodes(data []byte) []Node {
	nodes := make([]Node, 0)

	for i := 0; i+26 <= len(data); i += 26 {
		var node Node
		copy(node.ID[:], data[i:i+20])
		node.IP = net.IP(data[i+20 : i+24])
		node.Port = int(data[i+24])<<8 | int(data[i+25])
		node.LastSeen = time.Now()
		nodes = append(nodes, node)
	}

	return nodes
}

// encodeCompactNodes encodes nodes to compact format
func encodeCompactNodes(nodes []*Node) []byte {
	result := make([]byte, 0, len(nodes)*26)

	for _, node := range nodes {
		result = append(result, node.ID[:]...)
		result = append(result, node.IP.To4()...)
		result = append(result, byte(node.Port>>8), byte(node.Port&0xff))
	}

	return result
}

// parseCompactPeers parses compact peer info (6 bytes per peer)
func parseCompactPeers(data []byte) []Peer {
	peers := make([]Peer, 0)

	for i := 0; i+6 <= len(data); i += 6 {
		peer := Peer{
			IP:   net.IP(data[i : i+4]),
			Port: int(data[i+4])<<8 | int(data[i+5]),
		}
		peers = append(peers, peer)
	}

	return peers
}

// encodeCompactPeer encodes a peer to compact format
func encodeCompactPeer(peer Peer) []byte {
	result := make([]byte, 6)
	copy(result[0:4], peer.IP.To4())
	result[4] = byte(peer.Port >> 8)
	result[5] = byte(peer.Port & 0xff)
	return result
}
