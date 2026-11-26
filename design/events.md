# Events (SEP-3415)

## Context

The events extension (`io.modelcontextprotocol/events`, [SEP-3415](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/seps/3415-events-extension.md),
Beta, [specification](https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx)
at `c77b8439`) lets a client subscribe to upstream happenings and receive occurrences
`{eventId, name, timestamp, data, cursor}`:

| Method | Mode | Result |
|---|---|---|
| `events/list` | | Event types: `name`, `description`, `delivery`, `inputSchema`, `payloadSchema` |
| `events/poll` | poll | `events`, `cursor`, `truncated`, `hasMore`, `nextPollMs` |
| `events/stream` | push | `notifications/events/active` first, then `event`, `heartbeat`, `error`; `terminated` ends it; an empty result when the server closes it |
| `events/subscribe`, `events/unsubscribe` | webhook | Not implemented |

The extension is defined for 2026-07-28 only: it relies on per-request capabilities,
`server/discover` and `subscriptions/listen`, where `notifications/events/list_changed` is
delivered (`SubscriptionFilter.eventsListChanged`). It adds the error codes `NotFound` (-32023),
`Forbidden` (-32024), `ResourceExhausted` (-32025), `Unsupported` (-32026) and
`CallbackEndpointError` (-32027, webhook only). Over Streamable HTTP, `Mcp-Name` carries
`params.name` of every `events/*` request but `events/list`.

## Decisions awaiting the maintainers

[#376](https://github.com/modelcontextprotocol/swift-sdk/issues/376) asks three questions that
are not answered yet. The implementation takes these guesses:

1. **It builds on `rel/1.0.0`.** The extension needs the 2026-07-28 foundations, which exist only
   there; the PR targets `main` once #372 merges.
2. **It ships in the `MCP` module**, like tasks and skills, not as a separate product. It is
   self-contained so it can move: the wire types are `Base/EventTypes.swift`, the server
   `Server/Events.swift`, the client `Client/ClientEvents.swift`. The hooks into the actors are
   small: in `Server.swift`, the streams `stop()` ends (`beginEventStream`, `endEventStream`),
   `sendOnStream`, the `listChanged` filter and the capability, which is stripped before
   2026-07-28; in `Client.swift`, the routes of `events/stream` notifications (`eventRoutes`,
   `routeEvent`). Moving it to a product would turn those into `package` symbols.
3. **The error codes are `MCPError.Code` constants** (`.notFound`, `.forbidden`,
   `.resourceExhausted`, `.unsupported`, `.callbackEndpointError`), sent as
   `.serverError(code:message:data:)`, because the cases of `MCPError` are frozen for 1.x.
   `MCPError.notFound(_:data:)` and its siblings build them; `catch MCPError.Code.forbidden`
   matches them.

## Goals

- A host that spawns workers over stdio pushes one `emit` to every worker's stream.
- One definition per event type serves poll and push.
- No occurrence is lost between a stream's registration and its `active`, and none is repeated.
- A cancelled or disconnected stream releases its server task and its registry entry.
- Clients resume from a cursor; a cursor that cannot be served says so with `truncated`.

## Design

### Wire types (`Base/EventTypes.swift`)

`EventDescriptor`, `EventOccurrence`, `EventDeliveryMode` (open struct), `EventsSettings`,
`ListEvents`, `PollEvents`, `StreamEvents`, the six notifications and `EventsErrorParameters`
(`{error}` for `error` and `terminated`). Results carry no `resultType`; the stateless result
annotation adds it, as for every 2026-07-28 result. `cursor` is optional everywhere and an
absent one reads as `nil`; requests, poll results, `active` and `heartbeat` encode it as `null`,
occurrences omit it.

### Registry (`Server/Events.swift`)

`EventType` is one type: its descriptor, `delivery` (`[.poll, .push]` by default), and three
optional hooks: `validate(arguments, context)`, which refuses a subscription by throwing;
`check(EventCheck) -> EventBatch`, which reads the upstream; and `match(occurrence, arguments)`,
which routes emitted occurrences.

`EventRegistry` (an actor) holds the types, the buffer and the open streams. Servers share it
like a `TaskManager`; `withEvents(_:)` attaches a server for list-changed notifications.

- **Emit-only types** (no `check`) are served from a buffer per type (`EventBuffer`: 10 minutes,
  10,000 occurrences). Every emit takes the next number of one sequence; the cursor is
  `<epoch>.<sequence>`, the epoch random per registry. A cursor of another epoch or beyond the
  head is truncated to the head; one older than the type's last eviction is truncated to the
  oldest kept occurrence; `maxAgeMs` skips older occurrences and marks the result truncated.
  Poll pages by `maxEvents` (default 100, at most 1,000): a full page returns the cursor of its
  last occurrence and `hasMore`, a last page the head, so a quiet poll still advances.
- **`check` types**: poll calls `check` once (a `nil` cursor bootstraps: the events are
  dropped, the cursor kept); the SDK drops occurrences older than `maxAgeMs` and reports
  `truncated`. An event without a cursor of its own gets the cursor before its batch, the last
  one the batch's, so every cursor sent is safe to persist. `emit` refuses these types.

### `events/stream`

The handler follows `serveListen`:

1. Resolve the type (`NotFound`, `Unsupported` for a mode outside `delivery`), run `validate`,
   grant the heartbeat (the suggestion clamped to `heartbeatRange`, 30 s by default).
2. Make an `AsyncStream` of items (`bufferingOldest(buffer.maxEvents)`), hand its continuation
   to the server (so `stop()` can finish it) and to the registry. The registry registers it and
   computes the replay in one actor step: an emit lands either in the replay or on the stream.
3. For a `check` type, call `check` once from the request's cursor.
4. Send `active` with `relatedRequestID: context.id` and `_meta.subscriptionId`, then the
   replay, then each item as it comes. Only the handler task writes, so `active` is first.
5. A heartbeat task yields `.heartbeat` into the same stream every interval, and a `check` task
   yields batches; both are cancelled in `defer`. The heartbeat carries the cursor of the last
   item, including `advance` items for occurrences the stream's `match` skipped.
6. `notifications/cancelled` cancels the handler task, which ends the `for await` and throws;
   no result is sent. `terminated` (from `unregister`, a schema change, `terminate`, or a
   `check` that throws `NotFound`, `Forbidden` or `Unsupported`), the end of the item stream
   (`stop()`, an overflowing stream) and the registry closing it end the loop with the result.

`stop()` finishes the streams of its server and waits for their results before it disconnects,
as for listen streams. A stream that falls `buffer.maxEvents` items behind is finished: its
client resumes from its cursor, which the buffer replays.

Over `StatelessHTTPServerTransport`, `events/stream` without `text/event-stream` is 406, its SSE
response gets no `: keep-alive` comments (ext-events#16: the heartbeat must carry the cursor),
the client id is restored in `subscriptionId` as for listen, and `HTTPClientTransport` gives
it the listen idle timeout. `events/*` are in `StatelessRequests.statelessOnlyMethods` (-32601
at handshake revisions) and gated on the declaration at 2026-07-28 (`isDeclared`).

### Client (`Client/ClientEvents.swift`)

`listEvents(cursor:)`, `pollEvents(_:)` and `streamEvents(_:)` throw -32601 locally without the
server's declaration. `streamEvents` registers a route under the request id before sending,
waits for `active` or the end of the request (a stream of outcomes, not a task group, as
`listen` does), and returns an `EventSubscription`: `active`, `messages` (an
`AsyncThrowingStream<EventStreamMessage, Error>`) and `cancel()`. The receive loop routes the
`notifications/events/*` messages by `subscriptionId` inline; `terminated` finishes `messages`
with its error, the end of the request without one.

## Where this differs from the brief and the specification

- The specification has five new codes; `CallbackEndpointError` (-32027) is added as a constant
  although webhook delivery is not implemented.
- `delivery` is a required member of each descriptor; `EventType` defaults it to poll and push,
  and the server answers `Unsupported` for a mode the type does not list.
- `emit` throws (`NotFound` for an unknown type, -32600 for a `check` type) and returns the
  occurrence, so a caller learns its cursor.
- The heartbeat is sent every interval, not only after an interval without other messages; the
  specification asks for at least that.
- The spec's `transform` hook, the subscription lifecycle hooks (`onSubscribe`, poll leases),
  targeted emit and `x-mcp-header` on event arguments are not implemented.

## Alternatives considered

- **A separate `MCPEvents` product**, outside the 1.x promise while the extension is Beta. Left
  to the maintainers (question 2 above); the code is laid out so the move is mechanical.
- **Typed `MCPError` cases** for the new codes. Not possible in 1.x: the cases are frozen.
- **A task group for the stream's heartbeat and check loops.** The cancellation pitfall of
  [subscriptions.md](subscriptions.md#pitfall-waiting-with-a-task-group): unstructured tasks
  that feed the handler's stream are cancelled in `defer`.
- **`bufferingNewest` for stream items**, as for listen. Listen events are refetch hints;
  occurrences are not, so a stream that overflows ends and the client resumes from its cursor
  instead of losing occurrences silently.
- **Cursors that survive a restart for emit-only types.** The buffer is in memory, as the SEP's
  guidance says; a durable upstream belongs in a `check`.

## Open questions

- The SEP's open question 3 (several event names per subscription) would change the wire.
- Long polling (ext-events#5) is not specified yet.
- Conformance: the pinned harness has no events scenarios; modelcontextprotocol/conformance#521
  (server) and #541 (client) are open and grade an older revision of the SEP. The everything
  server needs its control tools (`events_conformance_*`) once they land.
- The client has no mode selection, poll loop or reconnection; the TypeScript SDK's has.
