package dht

import (
	"sort"
	"sync"
	"time"
)

const (
	// K is the maximum number of nodes per bucket (standard is 8)
	K = 8

	// NumBuckets is the number of buckets (160 for SHA-1)
	NumBuckets = 160

	// NodeTimeout is how long before a node is considered stale
	NodeTimeout = 15 * time.Minute
)

// RoutingTable implements the Kademlia routing table
type RoutingTable struct {
	nodeID  NodeID
	buckets [NumBuckets]*Bucket
	mu      sync.RWMutex
}

// Bucket represents a K-bucket in the routing table
type Bucket struct {
	nodes      []*Node
	lastUpdate time.Time
	mu         sync.RWMutex
}

// NewRoutingTable creates a new routing table
func NewRoutingTable(nodeID NodeID) *RoutingTable {
	rt := &RoutingTable{
		nodeID: nodeID,
	}

	// Initialize buckets
	for i := 0; i < NumBuckets; i++ {
		rt.buckets[i] = &Bucket{
			nodes:      make([]*Node, 0, K),
			lastUpdate: time.Now(),
		}
	}

	return rt
}

// AddNode adds a node to the routing table
func (rt *RoutingTable) AddNode(node *Node) bool {
	if node == nil {
		return false
	}

	// Don't add ourselves
	if node.ID == rt.nodeID {
		return false
	}

	// Find the appropriate bucket
	bucketIndex := rt.bucketIndex(node.ID)
	bucket := rt.buckets[bucketIndex]

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// Check if node already exists
	for i, n := range bucket.nodes {
		if n.ID == node.ID {
			// Update existing node
			bucket.nodes[i] = node
			bucket.lastUpdate = time.Now()
			return true
		}
	}

	// Add new node if bucket has space
	if len(bucket.nodes) < K {
		bucket.nodes = append(bucket.nodes, node)
		bucket.lastUpdate = time.Now()
		return true
	}

	// Bucket is full - check for stale nodes
	now := time.Now()
	for i, n := range bucket.nodes {
		if now.Sub(n.LastSeen) > NodeTimeout {
			// Replace stale node
			bucket.nodes[i] = node
			bucket.lastUpdate = time.Now()
			return true
		}
	}

	// Bucket is full with fresh nodes - drop the new node
	return false
}

// RemoveNode removes a node from the routing table
func (rt *RoutingTable) RemoveNode(nodeID NodeID) {
	bucketIndex := rt.bucketIndex(nodeID)
	bucket := rt.buckets[bucketIndex]

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for i, node := range bucket.nodes {
		if node.ID == nodeID {
			// Remove node
			bucket.nodes = append(bucket.nodes[:i], bucket.nodes[i+1:]...)
			return
		}
	}
}

// FindClosest finds the K closest nodes to a target ID
func (rt *RoutingTable) FindClosest(target NodeID, count int) []*Node {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Collect all nodes
	allNodes := make([]*Node, 0)
	for _, bucket := range rt.buckets {
		bucket.mu.RLock()
		allNodes = append(allNodes, bucket.nodes...)
		bucket.mu.RUnlock()
	}

	if len(allNodes) == 0 {
		return nil
	}

	// Sort by distance to target
	sort.Slice(allNodes, func(i, j int) bool {
		distI := Distance(allNodes[i].ID, target)
		distJ := Distance(allNodes[j].ID, target)
		return distI.Compare(distJ) < 0
	})

	// Return top K
	if len(allNodes) > count {
		allNodes = allNodes[:count]
	}

	return allNodes
}

// GetNode retrieves a specific node by ID
func (rt *RoutingTable) GetNode(nodeID NodeID) *Node {
	bucketIndex := rt.bucketIndex(nodeID)
	bucket := rt.buckets[bucketIndex]

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	for _, node := range bucket.nodes {
		if node.ID == nodeID {
			return node
		}
	}

	return nil
}

// Len returns the total number of nodes in the routing table
func (rt *RoutingTable) Len() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	count := 0
	for _, bucket := range rt.buckets {
		bucket.mu.RLock()
		count += len(bucket.nodes)
		bucket.mu.RUnlock()
	}

	return count
}

// Refresh refreshes buckets by querying random IDs
func (rt *RoutingTable) Refresh() {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	now := time.Now()

	for _, bucket := range rt.buckets {
		bucket.mu.RLock()
		lastUpdate := bucket.lastUpdate
		bucket.mu.RUnlock()

		// Refresh buckets that haven't been updated in a while
		if now.Sub(lastUpdate) > 15*time.Minute {
			// In a full implementation, we would:
			// 1. Generate a random ID in this bucket's range
			// 2. Query nodes for this random ID to discover new nodes
			// For now, just update the lastUpdate time
			bucket.mu.Lock()
			bucket.lastUpdate = now
			bucket.mu.Unlock()
		}
	}
}

// bucketIndex calculates which bucket a node ID belongs to
// Returns the index of the first differing bit (distance)
func (rt *RoutingTable) bucketIndex(nodeID NodeID) int {
	// Calculate XOR distance
	distance := Distance(rt.nodeID, nodeID)

	// Find the position of the most significant bit
	for i := 0; i < 160; i++ {
		byteIndex := i / 8
		bitIndex := 7 - (i % 8)

		if (distance[byteIndex] & (1 << bitIndex)) != 0 {
			// Found first differing bit at position i
			// Bucket index is 159 - i (inverted so closer nodes are in higher buckets)
			return 159 - i
		}
	}

	// IDs are identical (shouldn't happen) - use last bucket
	return 159
}

// generateRandomIDInBucket generates a random node ID that would fall into the given bucket
func (rt *RoutingTable) generateRandomIDInBucket(bucketIndex int) NodeID {
	// This is a simplified implementation
	// A full implementation would generate an ID that matches the bucket's prefix
	var id NodeID
	copy(id[:], rt.nodeID[:])

	// Flip bits to create distance appropriate for this bucket
	bitPosition := 159 - bucketIndex
	byteIndex := bitPosition / 8
	bitIndex := 7 - (bitPosition % 8)

	if byteIndex < 20 {
		id[byteIndex] ^= (1 << bitIndex)
	}

	return id
}

// GetAllNodes returns all nodes in the routing table
func (rt *RoutingTable) GetAllNodes() []*Node {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	nodes := make([]*Node, 0)
	for _, bucket := range rt.buckets {
		bucket.mu.RLock()
		nodes = append(nodes, bucket.nodes...)
		bucket.mu.RUnlock()
	}

	return nodes
}

// GetBucketNodes returns all nodes in a specific bucket
func (rt *RoutingTable) GetBucketNodes(bucketIndex int) []*Node {
	if bucketIndex < 0 || bucketIndex >= NumBuckets {
		return nil
	}

	bucket := rt.buckets[bucketIndex]
	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	nodes := make([]*Node, len(bucket.nodes))
	copy(nodes, bucket.nodes)
	return nodes
}

// String returns a string representation of the routing table
func (rt *RoutingTable) String() string {
	count := rt.Len()
	return string(rune(count)) + " nodes in routing table"
}
