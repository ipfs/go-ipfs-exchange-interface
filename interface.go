// Package exchange defines the IPFS exchange interface
package exchange

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// Interface defines the functionality of the IPFS block exchange protocol.
type Interface interface { // type Exchanger interface
	Fetcher

	// TODO Should callers be concerned with whether the block was made
	// available on the network?
	HasBlock(blocks.Block) error

	IsOnline() bool

	io.Closer
}

// Fetcher is an object that can be used to retrieve blocks
type Fetcher interface {
	// GetBlock returns the block associated with a given key.
	GetBlock(context.Context, cid.Cid) (blocks.Block, error)
	GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error)
}

// SessionExchange is an exchange.Interface which supports
// sessions.
type SessionExchange interface {
	Interface
	NewSession(context.Context) Fetcher
}

// SessionID is an opaque type uniquely identifying a session.
type SessionID struct {
	// Opaque type to ensure users don't cook this up and break things.
	id uint64
}

// String formats the session ID as a string.
func (id SessionID) String() string {
	return fmt.Sprintf("session-%d", id.id)
}

// IsZero returns true if this SessionId is the zero value (not a valid session
// ID).
func (id SessionID) IsZero() bool {
	return id.id == 0
}

type sessionContextKey struct{}
type sessionContextValue struct {
	sesID  SessionID
	sesCtx context.Context
}

// NewSession registers a new session with the context. The session will be
// closed when the passed-in context is canceled.
//
// If there's already a session associated with the context, the existing
// session will be used.
//
// This function does not initialize any state, it just reserves a new SessionID
// associates it with the context.
func NewSession(ctx context.Context) context.Context {
	if _, ok := ctx.Value(sessionContextKey{}).(*sessionContextValue); ok {
		return ctx
	}
	_, ctx = createSession(ctx)
	return ctx
}

// last allocated session ID. 0 is _never_ used.
var lastSessionID uint64

// GetOrCreateSession loads the session from the context, or creates one if
// there is no associated session.
//
// This function also returns the context used to create the session. The
// session should be stopped when this context is canceled.
func GetOrCreateSession(ctx context.Context) (SessionID, context.Context) {
	if s, ok := ctx.Value(sessionContextKey{}).(*sessionContextValue); ok {
		return s.sesID, s.sesCtx
	}
	return createSession(ctx)
}

func createSession(ctx context.Context) (SessionID, context.Context) {
	// Allocate a new session ID
	id := SessionID{atomic.AddUint64(&lastSessionID, 1)}

	// Create a spot to spot to hold the session information.
	ctxValue := &sessionContextValue{sesID: id}

	// Derive a new context with this information.
	ctx = context.WithValue(ctx, sessionContextKey{}, ctxValue)

	// Cyclically reference the session context so the session's context
	// also references the session.
	//
	// We could reference the original context, but that doesn't have the
	// session attached to it.
	ctxValue.sesCtx = ctx

	return id, ctx
}
