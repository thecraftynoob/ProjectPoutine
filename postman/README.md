# ProjectPoutine — Postman Collection

A working Postman collection for every REST route this platform exposes,
kept in sync with the code as a **living artifact** — same discipline as
`ARCHITECTURE_FLOW.md` (`CLAUDE.md` Rule 1) and `GAPS.md` (Rule 4).

## Files

- `ProjectPoutine.postman_collection.json` — the collection itself.
  Mirrors `ARCHITECTURE_FLOW.md` §1.1's REST route table exactly, grouped
  by service/domain in the order a fresh tenant would actually be set up:
  1. Tenant & Identity, 2. Task Router admin registries, 3. Agents,
  4. Tasks & Reservations, 5. Digital Channels Gateway's webhook,
  6. API Gateway's ws-ticket (a separate flow, run after folder 1), and
  7. Cleanup & Alternate Paths — every destructive or mutually-exclusive
  request (Remove Queue/Status/Attribute, Delete Agent, Reject
  Reservation) lives here instead of inline in folders 1-5, specifically
  so a straight top-to-bottom run of 1→5 never deletes or invalidates a
  resource a later step still needs. This was a real bug caught while
  building this collection (see git history) — folders 2/3 originally had
  Remove/Delete requests inline, which silently broke folders 3/4/5 when
  run in sequence; don't reintroduce that pattern when adding new routes.
- `ProjectPoutine.Local-K8s.postman_environment.json` — the one
  environment that exists today, pointing at the `kubectl port-forward`
  ports this repo's own docs (`README.md`, chat walkthroughs) already
  assume (`18080` → `api-gateway-svc:8080`, `18086` →
  `digital-channels-gateway-svc:8086`, `18085` →
  `agent-presence-svc:8085`). Add a second environment file here
  (e.g. `ProjectPoutine.Prod.postman_environment.json`) if/when a real
  non-port-forward endpoint exists — don't hardcode a second URL into the
  collection itself.

## Maintenance rule (mandatory — mirrors CLAUDE.md Rules 1 and 4)

**Whenever a REST route is added, changed, or removed — in the same
change that adds the `google.api.http` annotation and updates
`ARCHITECTURE_FLOW.md` §1.1's route table — update this collection too.**
A stale collection is worse than none (same principle `CLAUDE.md` Rule 1
states for `ARCHITECTURE_FLOW.md`): if you're not sure a request here
still matches the code, verify against the actual proto/handler before
trusting or extending it.

- New service, new webhook, or any non-`/v1/...` REST surface (like
  Digital Channels Gateway's `/webhooks/...`): add a new top-level folder,
  matching this collection's numbering convention.
- New RPC with a `google.api.http` annotation on an existing service: add
  one request to that service's existing folder.
- Request/response shape changes on an existing RPC: update the example
  body and any `pm.test`/variable-capture script that reads response
  fields.
- A route's auth requirement changes (exempt → required or vice versa):
  update that request's `auth` override (folder/collection-level auth is
  `bearer` using `{{jwt}}` by default; individual exempt routes override
  with `{"type": "noauth"}` — see "Create Tenant", "Login", the webhook,
  etc. for the pattern).

## How chaining works

Requests that create something (Create Tenant, Create User, Login, Create
Agent, Enqueue Task, the webhook, Mint WS Ticket) have a `test` script
that reads the response and sets a **collection variable**
(`tenant_id`, `user_id`, `jwt`, `agent_id`, `task_id`, `reservation_id`,
`ws_ticket`, ...). Every later request in the collection references those
same variables in its URL/body, so running folders 1 → 5 top-to-bottom
against a fresh tenant works with zero manual copy-pasting —
**verified end-to-end via `newman` against the real cluster** (Create
Tenant through Complete Task and the webhook all chain correctly with
0 failed assertions).

`reservation_id` is captured by "List Agent Pending Offers (post-match)"
in folder 4 (run right after task creation/matching) — NOT by "List
Agent Pending Offers" in folder 3, which runs before any task exists and
is always empty; that copy exists purely to demonstrate the RPC in
isolation.

## Running it

1. Two port-forwards, left running (matches this repo's own chat-flow
   walkthrough docs):
   ```powershell
   kubectl port-forward -n ccaas-dev svc/api-gateway-svc 18080:8080
   kubectl port-forward -n ccaas-dev svc/digital-channels-gateway-svc 18086:8086
   ```
   Add `kubectl port-forward -n ccaas-dev svc/agent-presence-svc 18085:8085`
   too if you're also testing the WebSocket ticket flow.
2. Import both files into Postman (collection + environment), select the
   "ProjectPoutine - Local K8s (port-forward)" environment.
3. Run folders 1 → 5 in order for a full create-interaction-and-complete-it
   walkthrough (the same steps documented as manual curl calls earlier in
   this project's history) — or use Postman's Collection Runner to fire
   the whole sequence at once.
4. Folder 6 (ws-ticket) is a separate flow, needing only folder 1 to have
   run first (for `{{jwt}}`) — mint the ticket, then open a WebSocket
   connection to `{{api_gateway_base}}/ws?ticket={{ws_ticket}}` within
   its ~45s TTL using Postman's WebSocket Request type (or an external
   tool like `wscat`) — Postman's standard `test`-script automation
   doesn't drive WebSocket connections the same way, so this step is
   manual.
5. Folder 7 (Cleanup & Alternate Paths) is never part of the default
   sequential run — run individual requests from it standalone, after
   you're done exercising folders 1-5 against a given tenant, or when you
   specifically want to test a Remove/Delete/Reject path.

## Known gaps this collection intentionally reflects, not hides

See `../GAPS.md` for the full registry. The two most relevant to this
collection:

- **Digital Channels Gateway's webhook has no auth** — the "Inbound Chat
  Webhook" request has no Authorization header by design, not by
  oversight; it documents the real, current, open state of that endpoint.
- **Historical Reporting has no read API** — there is deliberately no
  folder for it here. "Viewing" ingested events today means a direct SQL
  query against `historical_events`, not a REST call — see this repo's
  chat-interaction walkthrough (or `ARCHITECTURE_FLOW.md`) for the exact
  query. A folder will be added here the moment a real read RPC exists.
