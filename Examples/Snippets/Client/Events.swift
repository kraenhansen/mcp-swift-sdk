import Foundation
import MCP

// snippet: client-events
/// Follows P1 incidents on a push stream until `limit` arrive, and returns
/// the cursor to resume from.
func followIncidents(_ client: Client, from cursor: String?, limit: Int) async throws -> (
    incidents: [String], cursor: String?
) {
    let subscription = try await client.streamEvents(
        .init(name: "incident.created", arguments: ["severity": "P1"], cursor: cursor))
    var cursor = subscription.active.cursor
    var incidents: [String] = []
    for try await message in subscription.messages {
        switch message {
        case .event(let event):
            incidents.append(event.data["incidentId"]?.stringValue ?? event.eventId)
            cursor = event.cursor
        case .heartbeat(let position):
            cursor = position
        case .active(let gap):
            cursor = gap.cursor
        case .error(let error):
            print("Upstream trouble, the stream stays open: \(error)")
        }
        if incidents.count == limit { break }
    }
    await subscription.cancel()
    return (incidents, cursor)
}
// endsnippet

// snippet: client-events-poll
/// Polls once, draining a backlog, and returns the cursor for the next poll.
func pollIncidents(_ client: Client, from cursor: String?) async throws -> (incidents: [String], cursor: String?) {
    var cursor = cursor
    var incidents: [String] = []
    while true {
        let page = try await client.pollEvents(.init(name: "incident.created", cursor: cursor, maxEvents: 50))
        incidents += page.events.map(\.eventId)
        cursor = page.cursor
        // Otherwise wait page.nextPollMs before the next poll.
        guard page.hasMore else { return (incidents, cursor) }
    }
}
// endsnippet
