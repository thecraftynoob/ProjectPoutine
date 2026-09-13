package wsserver

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// writeTimeout bounds how long a single event delivery write may block,
// so one slow/stalled client can never hang the goroutine delivering to
// it indefinitely (the NATS relay's fan-out delivery path, see
// internal/relay/fanout.go, calls Send synchronously).
const writeTimeout = 5 * time.Second

// wsConnection adapts a *websocket.Conn to registry.Connection, and
// tracks the (tenantID, agentID) it was registered under so the
// disconnect path can call registry.UnregisterIfCurrent with the exact
// same connection value that was registered.
type wsConnection struct {
	conn     *websocket.Conn
	tenantID string
	agentID  string

	// writeMu serializes writes: coder/websocket's Conn.Write must not be
	// called concurrently from multiple goroutines, but this connection
	// can receive concurrent Send calls (e.g. two different relay
	// deliveries racing) and delivers over the same underlying socket.
	writeMu sync.Mutex
}

// Send implements registry.Connection: writes message as a single
// WebSocket text frame (the wire contract, per the service README, is a
// JSON document per message).
func (c *wsConnection) Send(message []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	return c.conn.Write(ctx, websocket.MessageText, message)
}
