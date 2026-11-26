import Foundation
import Testing

@testable import MCP

private func encoded<T: Encodable>(_ value: T) throws -> Value {
    try JSONDecoder().decode(Value.self, from: JSONEncoder().encode(value))
}

private func decoded<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(T.self, from: Data(json.utf8))
}

private func statelessMessage(id: Int, method: String, params: [String: Value] = [:]) -> String {
    var params = params
    params["_meta"] = [MetaKey.protocolVersion: .string(Version.v2026_07_28), MetaKey.clientCapabilities: [:]]
    let value: Value = ["jsonrpc": "2.0", "id": .int(id), "method": .string(method), "params": .object(params)]
    return String(decoding: try! JSONEncoder().encode(value), as: UTF8.self)
}

@Suite("Event wire types")
struct EventTests {
    @Test("A descriptor encodes the spec members; absent schemas and delivery decode with defaults")
    func descriptor() throws {
        let descriptor = EventDescriptor(
            name: "email.received", description: "New mail", delivery: [.poll, .push],
            inputSchema: ["type": "object"], payloadSchema: ["type": "object", "properties": ["from": ["type": "string"]]])
        expectWire(
            try encoded(descriptor),
            matches: try wireJSON(
                #"{"name":"email.received","description":"New mail","delivery":["poll","push"],"inputSchema":{"type":"object"},"payloadSchema":{"type":"object","properties":{"from":{"type":"string"}}}}"#
            ))
        let bare = try decoded(EventDescriptor.self, #"{"name":"x"}"#)
        #expect(bare.delivery.isEmpty)
        #expect(bare.inputSchema == ["type": "object"])
        #expect(try decoded(EventDescriptor.self, #"{"name":"x","delivery":["webhook","later"]}"#).delivery == [
            .webhook, "later",
        ])
    }

    @Test("An occurrence omits a nil cursor, and an absent cursor reads as nil")
    func occurrence() throws {
        let occurrence = EventOccurrence(
            eventId: "evt_1", name: "a", timestamp: "2026-02-19T15:30:00Z", data: ["k": "v"])
        let json = try encoded(occurrence)
        expectWire(json, matches: try wireJSON(#"{"eventId":"evt_1","name":"a","timestamp":"2026-02-19T15:30:00Z","data":{"k":"v"}}"#))
        expectWireLacks(json, "cursor", "_meta")
        let parsed = try decoded(
            EventOccurrence.self, #"{"eventId":"e","name":"a","timestamp":"2026-02-19T15:30:00.250Z","data":{}}"#)
        #expect(parsed.cursor == nil)
        #expect(parsed.date == Date(timeIntervalSince1970: 1_771_515_000.25))
        #expect(try decoded(EventOccurrence.self, #"{"eventId":"e","name":"a","timestamp":"t","data":{},"cursor":null}"#).cursor == nil)
    }

    @Test("Poll parameters send cursor null to start from now; a poll result always has cursor")
    func poll() throws {
        expectWire(
            try encoded(PollEvents.Parameters(name: "a")),
            matches: try wireJSON(#"{"name":"a","arguments":{},"cursor":null}"#))
        let start = try decoded(PollEvents.Parameters.self, #"{"name":"a"}"#)
        #expect(start.arguments.isEmpty)
        #expect(start.cursor == nil)

        expectWire(
            try encoded(PollEvents.Result(events: [], cursor: nil, nextPollMs: 5000)),
            matches: try wireJSON(#"{"events":[],"cursor":null,"truncated":false,"hasMore":false,"nextPollMs":5000}"#))
        let result = try decoded(PollEvents.Result.self, #"{"events":[]}"#)
        #expect(result.cursor == nil)
        #expect(!result.truncated)
        #expect(!result.hasMore)
        #expect(result.nextPollMs == nil)
    }

    @Test("Stream parameters, active and heartbeat send cursor null")
    func stream() throws {
        expectWire(
            try encoded(StreamEvents.Parameters(name: "a", heartbeatMs: 15000)),
            matches: try wireJSON(#"{"name":"a","arguments":{},"cursor":null,"heartbeatMs":15000}"#))
        expectWire(
            try encoded(EventsActiveNotification.Parameters(cursor: nil, heartbeatMs: 30000)),
            matches: try wireJSON(#"{"cursor":null,"truncated":false,"heartbeatMs":30000}"#))
        expectWire(try encoded(EventsHeartbeatNotification.Parameters(cursor: nil)), matches: ["cursor": .null])
        #expect(try decoded(EventsHeartbeatNotification.Parameters.self, "{}").cursor == nil)
        #expect(try decoded(EventsActiveNotification.Parameters.self, #"{"heartbeatMs":1}"#).truncated == false)
    }

    @Test("Messages on a stream carry the subscription id in params._meta")
    func streamMessages() throws {
        var meta = Metadata()
        meta.subscriptionID = .number(1)
        let event = Message<EventNotification>(
            method: EventNotification.name,
            params: EventOccurrence(eventId: "e", name: "a", timestamp: "t", data: [:], cursor: "c1"), _meta: meta)
        expectWire(
            try encoded(event),
            matches: try wireJSON(
                #"{"jsonrpc":"2.0","method":"notifications/events/event","params":{"eventId":"e","name":"a","timestamp":"t","data":{},"cursor":"c1","_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#
            ))
        let terminated = Message<EventsTerminatedNotification>(
            method: EventsTerminatedNotification.name,
            params: .init(error: .forbidden(data: ["reason": "Access revoked"])), _meta: meta)
        expectWire(
            try encoded(terminated),
            matches: try wireJSON(
                #"{"method":"notifications/events/terminated","params":{"error":{"code":-32024,"message":"Forbidden","data":{"reason":"Access revoked"}},"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#
            ))
    }

    @Test("Error codes -32023 to -32027 are constants; their errors decode and match by code")
    func errors() throws {
        #expect(MCPError.Code.notFound.rawValue == -32023)
        #expect(MCPError.Code.forbidden.rawValue == -32024)
        #expect(MCPError.Code.resourceExhausted.rawValue == -32025)
        #expect(MCPError.Code.unsupported.rawValue == -32026)
        #expect(MCPError.Code.callbackEndpointError.rawValue == -32027)
        expectWire(
            try encoded(MCPError.notFound(data: ["kind": "event"])),
            matches: try wireJSON(#"{"code":-32023,"message":"NotFound","data":{"kind":"event"}}"#))
        let error = try decoded(MCPError.self, #"{"code":-32026,"message":"Unsupported","data":{"feature":"deliveryMode"}}"#)
        #expect(error.errorCode == .unsupported)
        #expect(error.data == ["feature": "deliveryMode"])
        do {
            throw MCPError.resourceExhausted(data: ["limit": "subscriptions"])
        } catch MCPError.Code.resourceExhausted {
        } catch {
            Issue.record("Not matched by code: \(error)")
        }
    }

    @Test("Settings, the listen filter and Mcp-Name use the spec member names")
    func members() throws {
        #expect(EventsSettings([ExtensionID.events: [:]]) == EventsSettings())
        #expect(EventsSettings([ExtensionID.events: ["listChanged": true]])?.listChanged == true)
        #expect(EventsSettings([ExtensionID.events: true]) == nil)
        #expect(EventsSettings(nil) == nil)
        expectWire(try encoded(SubscriptionFilter(eventsListChanged: true)), matches: ["eventsListChanged": true])
        for method in [PollEvents.name, StreamEvents.name, "events/subscribe", "events/unsubscribe"] {
            #expect(MCPRequestHeaders.nameValue(method: method, params: ["name": "email.received"]) == "email.received")
        }
        #expect(MCPRequestHeaders.nameValue(method: ListEvents.name, params: ["cursor": "c"]) == nil)
    }

    @Test("events/list and events/poll results carry resultType complete at 2026-07-28")
    func resultType() async throws {
        let server = Server(name: "s", version: "1")
        await server.withEvents(EventRegistry([EventType(name: "a")]))
        let wire = WireExchange(server: server)
        try await wire.start()
        defer { Task { await wire.stop() } }
        let listed = try await wire.send(statelessMessage(id: 1, method: ListEvents.name))
        expectWire(
            listed[0],
            matches: try wireJSON(
                #"{"id":1,"result":{"resultType":"complete","events":[{"name":"a","delivery":["poll","push"],"inputSchema":{"type":"object"},"payloadSchema":{"type":"object"}}]}}"#
            ))
        let polled = try await wire.send(statelessMessage(id: 2, method: PollEvents.name, params: ["name": "a"]))
        expectWire(
            polled[0],
            matches: try wireJSON(
                #"{"id":2,"result":{"resultType":"complete","events":[],"cursor":"<any>","truncated":false,"hasMore":false,"nextPollMs":5000}}"#
            ))
    }
}
