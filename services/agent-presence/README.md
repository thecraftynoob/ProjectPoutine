# agent-presence

**Responsibility:** Holds the live WebSocket connection to every logged-in
Agent Desktop; is the single source of truth for real-time agent status
(Available, On Call, Wrap-Up, Offline); pushes task-offer notifications and
pulls accept/reject responses from agents (architecture doc Section 2.2).

**State:** Stateful (live WebSocket registry).

**Status:** scaffold only, but the proto pipeline is proven end-to-end here:
this service registers the real generated `presence.v1.PresenceService` gRPC
server (embedding `UnimplementedPresenceServiceServer`), so
`GetAvailableAgents` currently returns `codes.Unimplemented` rather than not
existing at all. No live WebSocket registry, Redis-backed presence keys, or
real matching data yet.
