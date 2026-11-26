import Foundation
import Testing

@testable import MCP

private let fast = EventRegistry.Configuration(heartbeatRange: .milliseconds(10)...(.seconds(300)))

private func connect(_ registry: EventRegistry?, configuration: Client.Configuration = .default) async throws
    -> (Client, Server)
{
    let server = Server(name: "events", version: "1", capabilities: .init(tools: .init()))
    if let registry { await server.withEvents(registry) }
    let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
    try await server.start(transport: serverTransport)
    let client = Client(name: "c", version: "1", configuration: configuration)
    try await client.connect(transport: clientTransport)
    return (client, server)
}

private func code(of operation: () async throws -> Void) async -> MCPError.Code? {
    do {
        try await operation()
        return nil
    } catch {
        return (error as? MCPError)?.errorCode
    }
}

private func next(
    _ count: Int, of subscription: EventSubscription, within limit: Duration = .seconds(5)
) async throws -> [EventStreamMessage] {
    let received = await outcome(within: limit) {
        var messages: [EventStreamMessage] = []
        for try await message in subscription.messages {
            messages.append(message)
            if messages.count == count { break }
        }
        return messages
    }
    switch received {
    case .returned(let messages): return messages
    case .threw(let error): throw error
    case .timedOut: return []
    }
}

private func eventually(_ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + .seconds(5)
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}

@Suite("Events on the client", .timeLimit(.minutes(1)))
struct EventClientTests {
    @Test("Without the server's declaration, the event methods throw -32601 without a request")
    func undeclared() async throws {
        let (client, server) = try await connect(nil)
        defer { Task { await client.disconnect(); await server.stop() } }
        #expect(await client.serverEvents == nil)
        await #expect(throws: MCPError.methodNotFound("events/list: the server does not declare \(ExtensionID.events)")) {
            _ = try await client.listEvents()
        }
        await #expect(throws: MCPError.self) { _ = try await client.pollEvents(.init(name: "a")) }
        await #expect(throws: MCPError.self) { _ = try await client.streamEvents(.init(name: "a")) }
    }

    @Test("listEvents and pollEvents round-trip the server's types and occurrences")
    func listAndPoll() async throws {
        let registry = EventRegistry([EventType(name: "a", description: "An a", delivery: [.poll])])
        let (client, server) = try await connect(registry)
        defer { Task { await client.disconnect(); await server.stop() } }

        #expect(await client.serverEvents?.listChanged == true)
        let listed = try await client.listEvents()
        #expect(listed.events == [EventDescriptor(
            name: "a", description: "An a", delivery: [.poll], inputSchema: ["type": "object"],
            payloadSchema: ["type": "object"])])
        let start = try await client.pollEvents(.init(name: "a"))
        try await registry.emit("a", data: ["n": 1], eventId: "e1")
        let polled = try await client.pollEvents(.init(name: "a", cursor: start.cursor))
        #expect(polled.events.map(\.eventId) == ["e1"])
        #expect(polled.events.first?.cursor == nil)
        #expect(polled.nextPollMs == 5000)
        #expect(await code { _ = try await client.pollEvents(.init(name: "b")) } == .notFound)
    }

    @Test("streamEvents returns after active and routes events and heartbeats by subscription")
    func stream() async throws {
        let registry = EventRegistry(
            [
                EventType(name: "a"),
                EventType(name: "b"),
            ], configuration: fast)
        let (client, server) = try await connect(registry)
        defer { Task { await client.disconnect(); await server.stop() } }

        let a = try await client.streamEvents(.init(name: "a", heartbeatMs: 60_000))
        let b = try await client.streamEvents(.init(name: "b", heartbeatMs: 10))
        #expect(a.id != b.id)
        #expect(a.active.heartbeatMs == 60_000)
        #expect(!a.active.truncated)
        let emitted = try await registry.emit("a", data: ["x": true], eventId: "only-a")
        let first = try await next(1, of: a)
        #expect(first == [.event(emitted)])
        let beats = try await next(2, of: b)
        #expect(beats == [.heartbeat(cursor: b.active.cursor), .heartbeat(cursor: b.active.cursor)])
        await a.cancel()
        await b.cancel()
        #expect(await eventually { await registry.openStreamCount == 0 })
    }

    @Test("Cancel ends the messages without an error and releases the server's stream")
    func cancel() async throws {
        let registry = EventRegistry([EventType(name: "a")])
        let (client, server) = try await connect(registry)
        defer { Task { await client.disconnect(); await server.stop() } }

        let subscription = try await client.streamEvents(.init(name: "a"))
        #expect(await registry.openStreamCount == 1)
        await subscription.cancel()
        #expect(try await next(1, of: subscription).isEmpty)
        #expect(await eventually { await registry.openStreamCount == 0 })
        #expect(await client.eventRoutes.isEmpty)
    }

    @Test("terminated ends the messages with its error; stop() ends them without one")
    func endings() async throws {
        let registry = EventRegistry([EventType(name: "gone"), EventType(name: "kept")])
        let (client, server) = try await connect(registry)
        defer { Task { await client.disconnect() } }

        let gone = try await client.streamEvents(.init(name: "gone"))
        let kept = try await client.streamEvents(.init(name: "kept"))
        await registry.unregister("gone")
        #expect(await code { _ = try await next(1, of: gone) } == .notFound)
        await server.stop()
        #expect(try await next(1, of: kept).isEmpty)
    }

    @Test("A refused subscription throws the server's error")
    func refused() async throws {
        let registry = EventRegistry([
            EventType(name: "secret", validate: { _, _ in throw MCPError.forbidden() })
        ])
        let (client, server) = try await connect(registry)
        defer { Task { await client.disconnect(); await server.stop() } }

        #expect(await code { _ = try await client.streamEvents(.init(name: "secret")) } == .forbidden)
        #expect(await client.eventRoutes.isEmpty)
    }

    @Test("Over initialize the server declares no events, and the client sends none")
    func handshake() async throws {
        let (client, server) = try await connect(EventRegistry(), configuration: .handshake)
        defer { Task { await client.disconnect(); await server.stop() } }
        #expect(await client.serverEvents == nil)
        #expect(await code { _ = try await client.listEvents() } == .methodNotFound)
    }
}
