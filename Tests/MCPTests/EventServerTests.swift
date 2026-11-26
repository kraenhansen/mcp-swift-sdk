import Foundation
import Logging
import Testing

@testable import MCP

private let statelessMeta: Value = [
    MetaKey.protocolVersion: .string(Version.v2026_07_28), MetaKey.clientCapabilities: [:],
]

private func request(_ id: String, _ method: String, _ params: [String: Value] = [:]) -> String {
    var params = params
    params["_meta"] = statelessMeta
    let value: Value = ["jsonrpc": "2.0", "id": .string(id), "method": .string(method), "params": .object(params)]
    return String(decoding: try! JSONEncoder().encode(value), as: UTF8.self)
}

private func cancel(_ id: String) -> String {
    #"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"\#(id)"}}"#
}

private let fast = EventRegistry.Configuration(
    heartbeatRange: .milliseconds(10)...(.seconds(300)), checkInterval: .milliseconds(20))

private let alerts = EventType(
    name: "alert", inputSchema: ["type": "object", "properties": ["severity": ["type": "string"]]],
    match: { occurrence, arguments in
        arguments["severity"].map { $0 == occurrence.data["severity"] } ?? true
    })

private func stream(_ peer: RawPeer, _ id: String) async -> [Value] {
    await peer.received.filter {
        $0.objectValue?["params"]?.objectValue?["_meta"]?.objectValue?[MetaKey.subscriptionID] == .string(id)
            || $0.objectValue?["id"] == .string(id)
    }
}

private func methods(_ messages: [Value]) -> [String] {
    messages.map { $0.objectValue?["method"]?.stringValue ?? "result" }
}

private func params(_ message: Value?) -> [String: Value] {
    message?.objectValue?["params"]?.objectValue ?? [:]
}

private func eventually(_ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + .seconds(5)
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}

private actor HoldingTransport: Transport {
    nonisolated let logger = Logger(label: "mcp.transport.holding")
    private let incoming: AsyncThrowingStream<Data, Swift.Error>
    private let input: AsyncThrowingStream<Data, Swift.Error>.Continuation
    private var gate: CheckedContinuation<Void, Never>?
    private var holds = true
    private(set) var isHolding = false
    private(set) var sent: [Value] = []

    init() {
        (incoming, input) = AsyncThrowingStream<Data, Swift.Error>.makeStream()
    }

    func connect() async throws {}
    func disconnect() async { input.finish() }
    func receive() -> AsyncThrowingStream<Data, Swift.Error> { incoming }
    func queue(_ json: String) { input.yield(Data(json.utf8)) }

    func send(_ data: Data) async throws {
        if holds, String(decoding: data, as: UTF8.self).contains(EventsActiveNotification.name) {
            holds = false
            isHolding = true
            await withCheckedContinuation { gate = $0 }
        }
        sent.append(try JSONDecoder().decode(Value.self, from: data))
    }

    func release() {
        gate?.resume()
        gate = nil
    }
}

@Suite("Serving events", .timeLimit(.minutes(1)))
struct EventServerTests {
    private func serve(
        _ registry: EventRegistry, capabilities: Server.Capabilities = .init()
    ) async throws -> (Server, RawPeer) {
        let server = Server(name: "events", version: "1", capabilities: capabilities)
        await server.withEvents(registry)
        let (peerEnd, serverEnd) = await InMemoryTransport.createConnectedPair()
        try await server.start(transport: serverEnd)
        let peer = RawPeer(peerEnd)
        try await peer.start()
        return (server, peer)
    }

    private func reply(_ peer: RawPeer, _ id: String) async throws -> Value {
        try #require(await peer.first { $0.objectValue?["id"] == .string(id) })
    }

    private func result(_ peer: RawPeer, _ id: String, _ method: String, _ params: [String: Value] = [:])
        async throws -> [String: Value]
    {
        try await peer.send(request(id, method, params))
        return try #require(try await reply(peer, id).objectValue?["result"]?.objectValue)
    }

    @Test("server/discover declares the extension with listChanged; initialize does not")
    func discover() async throws {
        let (server, peer) = try await serve(EventRegistry())
        let discovered = try await result(peer, "d", ServerDiscover.name)
        expectWire(
            .object(discovered), matches: ["capabilities": ["extensions": [ExtensionID.events: ["listChanged": true]]]])
        await peer.stop()
        await server.stop()

        let handshake = Server(name: "events", version: "1")
        await handshake.withEvents(EventRegistry())
        let (peerEnd, serverEnd) = await InMemoryTransport.createConnectedPair()
        try await handshake.start(transport: serverEnd)
        let initializing = RawPeer(peerEnd)
        try await initializing.start()
        try await initializing.initialize()
        let initialized = try #require(await initializing.first { $0.objectValue?["id"] == "init" })
        #expect(initialized.objectValue?["result"]?.objectValue?["capabilities"]?.objectValue?["extensions"] == nil)
        await initializing.stop()
        await handshake.stop()
    }

    @Test("Unknown names are NotFound, a mode the type does not offer is Unsupported, validate refuses")
    func refusals() async throws {
        let registry = EventRegistry([
            EventType(name: "poll.only", delivery: [.poll]),
            EventType(
                name: "private",
                validate: { arguments, _ in
                    guard arguments["token"] == "ok" else { throw MCPError.forbidden(data: ["reason": "No access"]) }
                }),
        ])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("1", PollEvents.name, ["name": "nope"]))
        expectWire(
            try await reply(peer, "1"), matches: try wireJSON(#"{"error":{"code":-32023,"data":{"kind":"event"}}}"#))
        try await peer.send(request("2", StreamEvents.name, ["name": "poll.only"]))
        expectWire(
            try await reply(peer, "2"),
            matches: try wireJSON(#"{"error":{"code":-32026,"data":{"feature":"deliveryMode","value":"push"}}}"#))
        try await peer.send(request("3", StreamEvents.name, ["name": "private"]))
        expectWire(try await reply(peer, "3"), matches: try wireJSON(#"{"error":{"code":-32024,"data":{"reason":"No access"}}}"#))
        #expect(await stream(peer, "3").count == 1)
        #expect(await registry.openStreamCount == 0)
    }

    @Test("Polling an emit-only type: null starts from now, match filters, results carry no per-event cursor")
    func pollEmitted() async throws {
        let registry = EventRegistry([alerts])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        let start = try await result(peer, "1", PollEvents.name, ["name": "alert", "arguments": ["severity": "P1"]])
        #expect(start["events"] == [])
        let cursor = try #require(start["cursor"]?.stringValue)
        try await registry.emit("alert", data: ["severity": "P2"])
        let p1 = try await registry.emit("alert", data: ["severity": "P1"], eventId: "INC-1")

        let polled = try await result(
            peer, "2", PollEvents.name,
            ["name": "alert", "arguments": ["severity": "P1"], "cursor": .string(cursor)])
        let events = try #require(polled["events"]?.arrayValue)
        #expect(events.count == 1)
        expectWire(events[0], matches: ["eventId": "INC-1", "name": "alert", "data": ["severity": "P1"]])
        expectWireLacks(events[0], "cursor")
        #expect(polled["cursor"]?.stringValue == p1.cursor)
        #expect(polled["truncated"] == false)
        #expect(polled["hasMore"] == false)

        let again = try await result(
            peer, "3", PollEvents.name, ["name": "alert", "cursor": polled["cursor"] ?? .null])
        #expect(again["events"] == [])
        #expect(again["cursor"] == polled["cursor"])
    }

    @Test("hasMore drains a backlog a page at a time")
    func hasMore() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        var cursor = try #require(try await result(peer, "0", PollEvents.name, ["name": "a"])["cursor"])
        for index in 0..<5 { try await registry.emit("a", data: ["i": .int(index)]) }
        var pages: [[Int]] = []
        var more = true
        while more {
            let page = try await result(
                peer, "p\(pages.count)", PollEvents.name, ["name": "a", "cursor": cursor, "maxEvents": 2])
            pages.append(page["events"]?.arrayValue?.compactMap { $0.objectValue?["data"]?.objectValue?["i"]?.intValue } ?? [])
            more = page["hasMore"] == true
            cursor = try #require(page["cursor"])
        }
        #expect(pages == [[0, 1], [2, 3], [4]])
    }

    @Test("A cursor from another process, an evicted one, or past maxAgeMs is truncated")
    func truncated() async throws {
        let registry = EventRegistry([EventType(name: "a")], buffer: .init(maxEvents: 2))
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        let stale = try await result(peer, "1", PollEvents.name, ["name": "a", "cursor": "0badc0de.3"])
        #expect(stale["truncated"] == true)
        #expect(stale["events"] == [])
        let fresh = try #require(stale["cursor"])
        #expect(fresh != "0badc0de.3")

        try await registry.emit("a", data: ["i": 0], date: Date().addingTimeInterval(-60))
        try await registry.emit("a", data: ["i": 1])
        let aged = try await result(peer, "2", PollEvents.name, ["name": "a", "cursor": fresh, "maxAgeMs": 10_000])
        #expect(aged["truncated"] == true)
        #expect(aged["events"]?.arrayValue?.count == 1)
        let unbounded = try await result(peer, "3", PollEvents.name, ["name": "a", "cursor": fresh])
        #expect(unbounded["truncated"] == false)
        #expect(unbounded["events"]?.arrayValue?.count == 2)

        try await registry.emit("a", data: ["i": 2])
        let evicted = try await result(peer, "4", PollEvents.name, ["name": "a", "cursor": fresh])
        #expect(evicted["truncated"] == true)
        #expect(
            evicted["events"]?.arrayValue?.compactMap { $0.objectValue?["data"]?.objectValue?["i"]?.intValue } == [1, 2])
    }

    @Test("active comes first, and an occurrence emitted before it is sent is not lost")
    func activeFirst() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let server = Server(name: "events", version: "1")
        await server.withEvents(registry)
        let transport = HoldingTransport()
        try await server.start(transport: transport)
        defer { Task { await server.stop() } }

        await transport.queue(request("s", StreamEvents.name, ["name": "a"]))
        #expect(await eventually { await transport.isHolding })
        let emitted = try await registry.emit("a", data: ["early": true])
        await transport.release()
        #expect(await eventually { await transport.sent.count == 2 })
        let sent = await transport.sent
        #expect(methods(sent) == [EventsActiveNotification.name, EventNotification.name])
        #expect(params(sent[1])["eventId"] == .string(emitted.eventId))
        #expect(params(sent[1])["cursor"] == .string(emitted.cursor ?? ""))
    }

    @Test("A stream replays from its cursor after active, then delivers live, each with its cursor")
    func replay() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        let cursor = try #require(try await result(peer, "0", PollEvents.name, ["name": "a"])["cursor"])
        let first = try await registry.emit("a", data: ["i": 0])
        try await peer.send(request("s", StreamEvents.name, ["name": "a", "cursor": cursor, "heartbeatMs": 60_000]))
        #expect(await eventually { await stream(peer, "s").count == 2 })
        let second = try await registry.emit("a", data: ["i": 1])
        #expect(await eventually { await stream(peer, "s").count == 3 })

        let messages = await stream(peer, "s")
        #expect(methods(messages) == [EventsActiveNotification.name, EventNotification.name, EventNotification.name])
        expectWire(.object(params(messages[0])), matches: ["cursor": cursor, "truncated": false, "heartbeatMs": 60_000])
        #expect(params(messages[1])["cursor"] == .string(first.cursor ?? ""))
        #expect(params(messages[2])["cursor"] == .string(second.cursor ?? ""))
        try await peer.send(cancel("s"))
    }

    @Test("A stream from a stale cursor starts with active truncated and a fresh cursor")
    func staleStream() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("s", StreamEvents.name, ["name": "a", "cursor": "0badc0de.9"]))
        #expect(await eventually { await stream(peer, "s").count == 1 })
        let active = params(await stream(peer, "s").first)
        #expect(active["truncated"] == true)
        #expect(active["cursor"] != "0badc0de.9")
        try await peer.send(cancel("s"))
    }

    @Test("Heartbeats carry the cursor of the last occurrence, also one the stream's match skipped")
    func heartbeat() async throws {
        let registry = EventRegistry([alerts], configuration: fast)
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(
            request("s", StreamEvents.name, ["name": "alert", "arguments": ["severity": "P1"], "heartbeatMs": 1]))
        #expect(await eventually { await stream(peer, "s").count == 1 })
        #expect(params(await stream(peer, "s").first)["heartbeatMs"] == 10)
        let skipped = try await registry.emit("alert", data: ["severity": "P3"])
        #expect(
            await eventually {
                await stream(peer, "s").contains {
                    $0.objectValue?["method"] == .string(EventsHeartbeatNotification.name)
                        && params($0)["cursor"] == .string(skipped.cursor ?? "")
                }
            })
        #expect(!methods(await stream(peer, "s")).contains(EventNotification.name))
        try await peer.send(cancel("s"))
    }

    @Test("notifications/cancelled releases the stream, with no result")
    func cancelled() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("s", StreamEvents.name, ["name": "a"]))
        #expect(await eventually { await stream(peer, "s").count == 1 })
        #expect(await registry.openStreamCount == 1)
        try await peer.send(cancel("s"))
        #expect(await eventually { await registry.openStreamCount == 0 })
        #expect(await eventually { await server.openEventStreamCount == 0 })
        try await peer.sync("after")
        #expect(methods(await stream(peer, "s")) == [EventsActiveNotification.name])
    }

    @Test("unregister ends the type's streams with terminated NotFound, then the result")
    func unregister() async throws {
        let registry = EventRegistry([EventType(name: "a"), EventType(name: "b")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("a", StreamEvents.name, ["name": "a"]))
        try await peer.send(request("b", StreamEvents.name, ["name": "b"]))
        #expect(await eventually { await registry.openStreamCount == 2 })
        await registry.unregister("a")
        #expect(await eventually { await stream(peer, "a").count == 3 })
        let messages = await stream(peer, "a")
        #expect(methods(messages) == [EventsActiveNotification.name, EventsTerminatedNotification.name, "result"])
        expectWire(.object(params(messages[1])), matches: try wireJSON(#"{"error":{"code":-32023,"data":{"kind":"event"}}}"#))
        expectWire(
            messages[2],
            matches: try wireJSON(
                #"{"id":"a","result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":"a"}}}"#))
        #expect(await registry.openStreamCount == 1)
        try await peer.send(request("p", PollEvents.name, ["name": "a"]))
        expectWire(try await reply(peer, "p"), matches: try wireJSON(#"{"error":{"code":-32023}}"#))
    }

    @Test("Re-registering a type with another schema ends its streams with Unsupported")
    func schemaChange() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("s", StreamEvents.name, ["name": "a"]))
        #expect(await eventually { await registry.openStreamCount == 1 })
        await registry.register(EventType(name: "a", description: "additive"))
        #expect(await registry.openStreamCount == 1)
        await registry.register(EventType(name: "a", payloadSchema: ["type": "object", "required": ["x"]]))
        #expect(await eventually { await stream(peer, "s").count == 3 })
        expectWire(
            .object(params(await stream(peer, "s")[1])),
            matches: try wireJSON(
                #"{"error":{"code":-32026,"data":{"feature":"payloadSchema","reason":"schema_changed"}}}"#))
    }

    @Test("terminate ends the streams the predicate selects")
    func terminate() async throws {
        let registry = EventRegistry([alerts])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("p1", StreamEvents.name, ["name": "alert", "arguments": ["severity": "P1"]]))
        try await peer.send(request("p2", StreamEvents.name, ["name": "alert", "arguments": ["severity": "P2"]]))
        #expect(await eventually { await registry.openStreamCount == 2 })
        await registry.terminate("alert", with: .forbidden("Access revoked")) { arguments, _ in
            arguments["severity"] == "P1"
        }
        #expect(await eventually { await stream(peer, "p1").count == 3 })
        #expect(params(await stream(peer, "p1")[1])["error"]?.objectValue?["code"] == -32024)
        #expect(await registry.openStreamCount == 1)
    }

    @Test("Two streams stay apart: each gets its own matches, under its own subscription id")
    func twoStreams() async throws {
        let registry = EventRegistry([alerts])
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("p1", StreamEvents.name, ["name": "alert", "arguments": ["severity": "P1"]]))
        try await peer.send(request("p2", StreamEvents.name, ["name": "alert", "arguments": ["severity": "P2"]]))
        #expect(await eventually { await registry.openStreamCount == 2 })
        try await registry.emit("alert", data: ["severity": "P2"], eventId: "two")
        try await registry.emit("alert", data: ["severity": "P1"], eventId: "one")
        #expect(await eventually { await stream(peer, "p1").count == 2 })
        #expect(await eventually { await stream(peer, "p2").count == 2 })
        #expect(params(await stream(peer, "p1")[1])["eventId"] == "one")
        #expect(params(await stream(peer, "p2")[1])["eventId"] == "two")
        try await peer.send(cancel("p1"))
        try await peer.send(cancel("p2"))
    }

    @Test("stop() ends every stream with its result before it closes the transport")
    func stop() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (server, peer) = try await serve(registry)

        try await peer.send(request("s1", StreamEvents.name, ["name": "a"]))
        try await peer.send(request("s2", StreamEvents.name, ["name": "a"]))
        #expect(await eventually { await registry.openStreamCount == 2 })
        await server.stop()
        #expect(await eventually { methods(await stream(peer, "s1")).last == "result" })
        #expect(await eventually { methods(await stream(peer, "s2")).last == "result" })
        #expect(await registry.openStreamCount == 0)
        await peer.stop()
    }

    @Test("One registry serves several servers: one emit reaches the streams of each")
    func sharedRegistry() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (first, firstPeer) = try await serve(registry)
        let (second, secondPeer) = try await serve(registry)
        defer {
            Task {
                await firstPeer.stop()
                await secondPeer.stop()
                await first.stop()
                await second.stop()
            }
        }

        try await firstPeer.send(request("s", StreamEvents.name, ["name": "a"]))
        try await secondPeer.send(request("s", StreamEvents.name, ["name": "a"]))
        #expect(await eventually { await registry.openStreamCount == 2 })
        try await registry.emit("a", data: [:], eventId: "shared")
        #expect(await eventually { await stream(firstPeer, "s").count == 2 })
        #expect(await eventually { await stream(secondPeer, "s").count == 2 })
        #expect(params(await stream(secondPeer, "s")[1])["eventId"] == "shared")
    }

    @Test("A listen stream that asks for eventsListChanged gets it when a type is registered")
    func listChanged() async throws {
        let registry = EventRegistry()
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(
            request("l", SubscriptionsListen.name, ["notifications": ["eventsListChanged": true, "toolsListChanged": true]]))
        #expect(await eventually { await stream(peer, "l").count == 1 })
        expectWire(.object(params(await stream(peer, "l").first)), matches: ["notifications": ["eventsListChanged": true]])
        expectWireLacks(params(await stream(peer, "l").first)["notifications"] ?? .null, "toolsListChanged")

        await registry.register(EventType(name: "new"))
        #expect(await eventually { await stream(peer, "l").count == 2 })
        #expect(methods(await stream(peer, "l")).last == EventsListChangedNotification.name)
        let listed = try await result(peer, "list", ListEvents.name)
        #expect(listed["events"]?.arrayValue?.first?.objectValue?["name"] == "new")
    }

    @Test("A check type: poll bootstraps and reads from the cursor; a stream checks at its interval")
    func check() async throws {
        let log = ChangeLog()
        let registry = EventRegistry(
            [
                EventType(
                    name: "ticket",
                    check: { check in
                        let changes = await log.changes
                        guard let cursor = check.cursor, let from = Int(cursor) else {
                            return EventBatch(cursor: String(changes.count))
                        }
                        let to = min(changes.count, from + check.maxEvents)
                        return EventBatch(
                            events: changes[from..<to].map { EventBatch.Event(eventId: $0, data: ["id": .string($0)]) },
                            cursor: String(to), hasMore: to < changes.count)
                    })
            ], configuration: fast)
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        await log.add("t1")
        let start = try await result(peer, "0", PollEvents.name, ["name": "ticket"])
        #expect(start["events"] == [])
        #expect(start["cursor"] == "1")
        await log.add("t2")
        await log.add("t3")
        let polled = try await result(peer, "1", PollEvents.name, ["name": "ticket", "cursor": "1", "maxEvents": 1])
        #expect(polled["events"]?.arrayValue?.first?.objectValue?["eventId"] == "t2")
        #expect(polled["cursor"] == "2")
        #expect(polled["hasMore"] == true)

        try await peer.send(request("s", StreamEvents.name, ["name": "ticket", "cursor": "1"]))
        #expect(await eventually { await stream(peer, "s").count == 3 })
        let messages = await stream(peer, "s")
        #expect(params(messages[0])["cursor"] == "1")
        #expect(messages[1...].map { params($0)["eventId"] } == ["t2", "t3"])
        #expect(params(messages[2])["cursor"] == "3")
        await log.add("t4")
        #expect(await eventually { await stream(peer, "s").contains { params($0)["eventId"] == "t4" } })
        await #expect(throws: MCPError.self) { try await registry.emit("ticket", data: [:]) }
        try await peer.send(cancel("s"))
    }

    @Test("A check that throws Forbidden terminates the stream; another error is reported and the stream stays")
    func checkErrors() async throws {
        let failures = ChangeLog()
        let registry = EventRegistry(
            [
                EventType(
                    name: "flaky",
                    check: { check in
                        guard check.cursor != nil else { return EventBatch(cursor: "0") }
                        let count = await failures.add("x")
                        if count == 1 { throw MCPError.internalError("Upstream 503") }
                        throw MCPError.forbidden("Access revoked")
                    })
            ], configuration: fast)
        let (server, peer) = try await serve(registry)
        defer { Task { await peer.stop(); await server.stop() } }

        try await peer.send(request("s", StreamEvents.name, ["name": "flaky"]))
        #expect(await eventually { methods(await stream(peer, "s")).last == "result" })
        #expect(
            methods(await stream(peer, "s")) == [
                EventsActiveNotification.name, EventsErrorNotification.name, EventsTerminatedNotification.name,
                "result",
            ])
        #expect(await registry.openStreamCount == 0)
    }
}

private actor ChangeLog {
    private(set) var changes: [String] = []
    @discardableResult
    func add(_ change: String) -> Int {
        changes.append(change)
        return changes.count
    }
}
