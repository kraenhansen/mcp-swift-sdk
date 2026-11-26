import Foundation
import MCP

// snippet: events-server
/// One registry for every server the host runs, such as one per stdio pipe.
let incidents = EventRegistry([
    EventType(
        name: "incident.created",
        description: "A new incident was opened",
        inputSchema: ["type": "object", "properties": ["severity": ["type": "string", "enum": ["P1", "P2", "P3"]]]],
        payloadSchema: ["type": "object", "properties": ["incidentId": ["type": "string"]]],
        match: { occurrence, arguments in
            arguments["severity"].map { $0 == occurrence.data["severity"] } ?? true
        })
])

func makeEventsServer() async -> Server {
    let server = Server(name: "pager", version: "1.0.0")
    await server.withEvents(incidents)
    return server
}

/// Wherever the upstream reports an incident:
func incidentOpened(id: String, severity: String) async throws {
    try await incidents.emit(
        "incident.created", data: ["incidentId": .string(id), "severity": .string(severity)], eventId: id)
}
// endsnippet

// snippet: events-check
/// An event type read from an upstream that can answer "what happened since this cursor".
func ticketEvents(log: TicketLog) -> EventType {
    EventType(
        name: "ticket.updated",
        inputSchema: ["type": "object", "properties": ["queue": ["type": "string"]]],
        check: { check in
            let changes = await log.changes
            guard let cursor = check.cursor, let from = Int(cursor) else {
                // A subscription that starts now: the position, no events.
                return EventBatch(cursor: String(changes.count))
            }
            let to = min(changes.count, from + check.maxEvents)
            let page = changes[from..<to].filter { $0.queue == check.arguments["queue"]?.stringValue }
            return EventBatch(
                events: page.map { EventBatch.Event(eventId: $0.id, data: ["status": .string($0.status)]) },
                cursor: String(to), hasMore: to < changes.count)
        })
}

actor TicketLog {
    struct Change: Sendable {
        let id: String
        let queue: String
        let status: String
    }
    private(set) var changes: [Change] = []
    func append(_ change: Change) { changes.append(change) }
}
// endsnippet
