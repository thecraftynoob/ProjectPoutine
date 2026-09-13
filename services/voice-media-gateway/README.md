# voice-media-gateway

**Responsibility:** Terminates SIP trunks and WebRTC (browser softphone)
sessions; manages media negotiation and RTP via FreeSWITCH; converts call
setup into a generic Task (voice modality) published to the bus; executes
agent-side call control commands (hold, transfer, mute) issued by the Task
Router / Agent Desktop (architecture doc Section 2.2).

**State:** Stateful (active call = live RTP session + FreeSWITCH channel
state).

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired; no FreeSWITCH integration, domain RPCs, or NATS/Redis
connections yet.
