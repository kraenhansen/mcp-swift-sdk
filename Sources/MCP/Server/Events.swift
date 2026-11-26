import Foundation

/// An event type a server offers, fed either by `EventRegistry.emit` or by a `check` function.
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx
public struct EventType: Sendable {
    /// The event type's name, such as `email.received`.
    public var name: String
    /// What the event type reports.
    public var description: String?
    /// The JSON Schema of subscription `arguments`.
    public var inputSchema: Value
    /// The JSON Schema of each occurrence's `data`.
    public var payloadSchema: Value
    /// The delivery modes offered; `webhook` is not served.
    public var delivery: [EventDeliveryMode]
    /// Metadata listed with the event type.
    public var _meta: Metadata?
    /// Checks a subscription's arguments; throw an `MCPError` such as
    /// ``MCPError/forbidden(_:data:)`` to refuse it.
    public var validate: (@Sendable ([String: Value], Server.HandlerContext) async throws -> Void)?
    /// Reads the events since a cursor from the upstream; `nil` for an
    /// emit-only type, served from the registry's buffer.
    public var check: (@Sendable (EventCheck) async throws -> EventBatch)?
    /// Whether an emitted occurrence belongs to a subscription with these
    /// arguments; `nil` accepts every occurrence.
    public var match: (@Sendable (EventOccurrence, [String: Value]) -> Bool)?

    /// Creates an event type.
    public init(
        name: String,
        description: String? = nil,
        inputSchema: Value = ["type": "object"],
        payloadSchema: Value = ["type": "object"],
        delivery: [EventDeliveryMode] = [.poll, .push],
        _meta: Metadata? = nil,
        validate: (@Sendable ([String: Value], Server.HandlerContext) async throws -> Void)? = nil,
        check: (@Sendable (EventCheck) async throws -> EventBatch)? = nil,
        match: (@Sendable (EventOccurrence, [String: Value]) -> Bool)? = nil
    ) {
        self.name = name
        self.description = description
        self.inputSchema = inputSchema
        self.payloadSchema = payloadSchema
        self.delivery = delivery
        self._meta = _meta
        self.validate = validate
        self.check = check
        self.match = match
    }

    /// The event type as `events/list` lists it.
    public var descriptor: EventDescriptor {
        EventDescriptor(
            name: name, description: description, delivery: delivery, inputSchema: inputSchema,
            payloadSchema: payloadSchema, _meta: _meta)
    }
}

/// What a `check` function is asked for.
public struct EventCheck: Sendable {
    /// The event type's name.
    public let name: String
    /// The subscription arguments.
    public let arguments: [String: Value]
    /// The position to read from; `nil` asks for the current position and no events.
    public let cursor: String?
    /// The most events to return; set `hasMore` when more wait.
    public let maxEvents: Int
    /// Replay no events older than this; the SDK also drops older events it gets.
    public let maxAge: Duration?
    /// The context of the request that subscribed.
    public let context: Server.HandlerContext
}

/// What a `check` function returns.
public struct EventBatch: Sendable {
    /// An event a `check` function read.
    public struct Event: Hashable, Sendable {
        /// The upstream event's stable identifier.
        public var eventId: String
        /// When the event occurred.
        public var date: Date
        /// The payload.
        public var data: [String: Value]
        /// The position after this event, when the upstream has one per event.
        public var cursor: String?
        /// Metadata of the occurrence.
        public var _meta: Metadata?

        /// Creates an event.
        public init(
            eventId: String, date: Date = Date(), data: [String: Value], cursor: String? = nil, _meta: Metadata? = nil
        ) {
            self.eventId = eventId
            self.date = date
            self.data = data
            self.cursor = cursor
            self._meta = _meta
        }
    }

    /// The events since the cursor, oldest first.
    public var events: [Event]
    /// The position after these events; `nil` for an upstream without replay.
    public var cursor: String?
    /// Whether more events wait after these.
    public var hasMore: Bool
    /// Whether the upstream no longer has the events since the cursor.
    public var truncated: Bool
    /// When to poll next, in milliseconds; `nil` uses the registry's interval.
    public var nextPollMs: Int?

    /// Creates a batch.
    public init(
        events: [Event] = [], cursor: String?, hasMore: Bool = false, truncated: Bool = false, nextPollMs: Int? = nil
    ) {
        self.events = events
        self.cursor = cursor
        self.hasMore = hasMore
        self.truncated = truncated
        self.nextPollMs = nextPollMs
    }
}

/// How long the registry keeps emitted occurrences, per event type.
public struct EventBuffer: Hashable, Sendable {
    /// The oldest occurrence kept.
    public var maxAge: Duration
    /// The most occurrences kept per event type.
    public var maxEvents: Int

    /// Creates the buffer settings.
    public init(maxAge: Duration = .seconds(600), maxEvents: Int = 10_000) {
        self.maxAge = maxAge
        self.maxEvents = maxEvents
    }
}

enum EventStreamItem: Sendable {
    case event(EventOccurrence)
    case advance(String?)
    case batch([EventOccurrence], cursor: String?, truncated: Bool)
    case heartbeat
    case error(MCPError)
    case terminated(MCPError)
}

/// The event types of one or more servers, the buffer of their emitted
/// occurrences, and their open `events/stream` streams.
///
/// Servers that serve the same event types, such as one per stdio
/// connection, share one registry, so one `emit` reaches every stream.
public actor EventRegistry {
    /// Limits and defaults of a registry.
    public struct Configuration: Hashable, Sendable {
        /// The heartbeat interval of streams that suggest none.
        public var defaultHeartbeat: Duration
        /// The heartbeat intervals granted; a suggestion outside is clamped.
        public var heartbeatRange: ClosedRange<Duration>
        /// The `nextPollMs` of poll results.
        public var pollInterval: Duration
        /// How often a stream of a `check` type checks its upstream.
        public var checkInterval: Duration
        /// The events a poll returns when it sets no `maxEvents`, and a `check` is asked for on streams.
        public var defaultMaxEvents: Int
        /// The most events a poll returns.
        public var maxEventsLimit: Int
        /// The most `events/stream` streams open at once; more are refused with `ResourceExhausted`.
        public var maxStreams: Int

        /// Creates a configuration.
        public init(
            defaultHeartbeat: Duration = .seconds(30),
            heartbeatRange: ClosedRange<Duration> = .seconds(1)...(.seconds(300)),
            pollInterval: Duration = .seconds(5),
            checkInterval: Duration = .seconds(5),
            defaultMaxEvents: Int = 100,
            maxEventsLimit: Int = 1000,
            maxStreams: Int = 1024
        ) {
            self.defaultHeartbeat = defaultHeartbeat
            self.heartbeatRange = heartbeatRange
            self.pollInterval = pollInterval
            self.checkInterval = checkInterval
            self.defaultMaxEvents = defaultMaxEvents
            self.maxEventsLimit = maxEventsLimit
            self.maxStreams = maxStreams
        }
    }

    private struct Buffered {
        let sequence: Int
        let occurrence: EventOccurrence
        let date: Date
    }

    private struct Stream {
        let name: String
        let arguments: [String: Value]
        let match: (@Sendable (EventOccurrence, [String: Value]) -> Bool)?
        let context: Server.HandlerContext
        let items: AsyncStream<EventStreamItem>.Continuation
    }

    struct StreamStart: Sendable {
        let cursor: String?
        let truncated: Bool
        let replay: [EventOccurrence]
    }

    /// The registry's buffer settings.
    public nonisolated let buffer: EventBuffer
    /// The registry's limits and defaults.
    public nonisolated let configuration: Configuration

    private var types: [String: EventType] = [:]
    private var order: [String] = []
    /// A cursor from another process carries another epoch and reads as truncated.
    private let epoch = String(UUID().uuidString.prefix(8)).lowercased()
    private var sequence = 0
    private var buffers: [String: [Buffered]] = [:]
    private var evicted: [String: Int] = [:]
    private var streams: [Int: Stream] = [:]
    private var nextStreamToken = 0
    private var listChangedSinks: [ObjectIdentifier: @Sendable () async -> Void] = [:]

    /// Creates a registry with `types`.
    public init(_ types: [EventType] = [], buffer: EventBuffer = .init(), configuration: Configuration = .init()) {
        self.buffer = buffer
        self.configuration = configuration
        for type in types where self.types.updateValue(type, forKey: type.name) == nil {
            order.append(type.name)
        }
    }

    /// The registered event types, in registration order.
    public var eventTypes: [EventType] { order.compactMap { types[$0] } }

    /// Adds `type`, or replaces the type with its name, and sends
    /// `notifications/events/list_changed`. A replacement whose schemas
    /// differ ends that type's streams with `Unsupported`.
    public func register(_ type: EventType) async {
        if let previous = types.updateValue(type, forKey: type.name) {
            let feature: String? =
                previous.inputSchema != type.inputSchema
                ? "inputSchema" : previous.payloadSchema != type.payloadSchema ? "payloadSchema" : nil
            if let feature {
                terminateStreams(
                    of: type.name,
                    with: .unsupported(
                        "Event type \(type.name) changed", data: ["feature": .string(feature), "reason": "schema_changed"]))
            }
        } else {
            order.append(type.name)
        }
        await sendListChanged()
    }

    /// Removes the event type `name` and its buffered occurrences, ends its
    /// streams with `NotFound`, and sends `notifications/events/list_changed`.
    public func unregister(_ name: String) async {
        guard types.removeValue(forKey: name) != nil else { return }
        order.removeAll { $0 == name }
        buffers[name] = nil
        evicted[name] = nil
        terminateStreams(of: name, with: Self.unknownType(name))
        await sendListChanged()
    }

    /// Records an occurrence of the emit-only type `name` and delivers it to
    /// the streams whose `match` accepts it.
    ///
    /// - Throws: `NotFound` for an unknown type, -32600 for a type with a `check` function.
    @discardableResult
    public func emit(
        _ name: String, data: [String: Value], eventId: String? = nil, date: Date = Date(), _meta: Metadata? = nil
    ) throws -> EventOccurrence {
        guard let type = types[name] else { throw Self.unknownType(name) }
        guard type.check == nil else {
            throw MCPError.invalidRequest("Event type \(name) reads its events with check; it cannot be emitted")
        }
        sequence += 1
        let occurrence = EventOccurrence(
            eventId: eventId ?? UUID().uuidString.lowercased(), name: name, date: date, data: data,
            cursor: cursor(sequence), _meta: _meta)
        var buffered = buffers[name, default: []]
        buffered.append(Buffered(sequence: sequence, occurrence: occurrence, date: date))
        buffers[name] = buffered
        evict(name)
        for (token, stream) in streams where stream.name == name {
            let item: EventStreamItem =
                stream.match?(occurrence, stream.arguments) ?? true ? .event(occurrence) : .advance(occurrence.cursor)
            if case .dropped = stream.items.yield(item) {
                // A stream this far behind ends; its client resumes from its cursor.
                stream.items.finish()
                streams[token] = nil
            }
        }
        return occurrence
    }

    /// Ends the streams of `name` whose subscription `accepts` selects, for
    /// example `Forbidden` when a principal lost access.
    public func terminate(
        _ name: String, with error: MCPError,
        where accepts: (@Sendable ([String: Value], Server.HandlerContext) -> Bool)? = nil
    ) {
        terminateStreams(of: name, with: error, where: accepts)
    }

    /// The number of open `events/stream` streams.
    public var openStreamCount: Int { streams.count }

    // MARK: - Serving

    func attach(_ server: Server) {
        listChangedSinks[ObjectIdentifier(server)] = { [weak server] in
            try? await server?.notify(EventsListChangedNotification.message())
        }
    }

    func resolve(_ name: String, mode: EventDeliveryMode) throws -> EventType {
        guard let type = types[name] else { throw Self.unknownType(name) }
        guard type.delivery.contains(mode) else {
            throw MCPError.unsupported(
                "Event type \(name) is not delivered by \(mode)",
                data: ["feature": "deliveryMode", "value": .string(mode.rawValue)])
        }
        return type
    }

    func heartbeat(suggested milliseconds: Int?) -> Duration {
        let suggested = milliseconds.map { Duration.milliseconds($0) } ?? configuration.defaultHeartbeat
        return min(max(suggested, configuration.heartbeatRange.lowerBound), configuration.heartbeatRange.upperBound)
    }

    func maxEvents(_ requested: Int?) -> Int {
        min(max(1, requested ?? configuration.defaultMaxEvents), configuration.maxEventsLimit)
    }

    func poll(_ params: PollEvents.Parameters, type: EventType) -> PollEvents.Result {
        let replay = self.replay(
            type, arguments: params.arguments, cursor: params.cursor, maxAgeMs: params.maxAgeMs,
            limit: maxEvents(params.maxEvents))
        return PollEvents.Result(
            events: replay.events.map { occurrence in
                var occurrence = occurrence
                occurrence.cursor = nil
                return occurrence
            },
            cursor: replay.cursor, truncated: replay.truncated, hasMore: replay.hasMore,
            nextPollMs: Self.milliseconds(configuration.pollInterval))
    }

    /// Registration and replay happen in one actor step, so no occurrence is lost or repeated.
    func openStream(
        _ params: StreamEvents.Parameters, type: EventType, context: Server.HandlerContext,
        items: AsyncStream<EventStreamItem>.Continuation
    ) throws -> (token: Int, start: StreamStart) {
        guard streams.count < configuration.maxStreams else {
            throw MCPError.resourceExhausted(
                "Too many open \(StreamEvents.name) streams",
                data: ["limit": "subscriptions", "max": .int(configuration.maxStreams)])
        }
        guard types[type.name] != nil else { throw Self.unknownType(type.name) }
        let token = nextStreamToken
        nextStreamToken += 1
        streams[token] = Stream(
            name: type.name, arguments: params.arguments, match: type.match, context: context, items: items)
        guard type.check == nil else {
            return (token, StreamStart(cursor: nil, truncated: false, replay: []))
        }
        let replay = self.replay(
            type, arguments: params.arguments, cursor: params.cursor, maxAgeMs: params.maxAgeMs, limit: .max)
        let start = replay.truncated || params.cursor == nil ? cursor(replay.startSequence) : params.cursor
        return (token, StreamStart(cursor: start, truncated: replay.truncated, replay: replay.events))
    }

    func closeStream(_ token: Int) {
        streams[token] = nil
    }

    // MARK: - Buffer

    private func cursor(_ sequence: Int) -> String { "\(epoch).\(sequence)" }

    private func sequence(of cursor: String) -> Int? {
        let parts = cursor.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count == 2, parts[0] == epoch, let sequence = Int(parts[1]), sequence >= 0,
            sequence <= self.sequence
        else { return nil }
        return sequence
    }

    private func evict(_ name: String, now: Date = Date()) {
        guard var buffered = buffers[name] else { return }
        let oldest = now.addingTimeInterval(-Self.seconds(buffer.maxAge))
        let overflow = max(0, buffered.count - max(0, buffer.maxEvents))
        let expired = buffered.dropFirst(overflow).prefix { $0.date < oldest }.count
        let count = overflow + expired
        guard count > 0 else { return }
        evicted[name] = buffered[count - 1].sequence
        buffered.removeFirst(count)
        buffers[name] = buffered
    }

    private func replay(
        _ type: EventType, arguments: [String: Value], cursor: String?, maxAgeMs: Int?, limit: Int
    ) -> (events: [EventOccurrence], cursor: String, startSequence: Int, truncated: Bool, hasMore: Bool) {
        let head = self.cursor(sequence)
        guard let cursor else { return ([], head, sequence, false, false) }
        guard let from = self.sequence(of: cursor) else { return ([], head, sequence, true, false) }
        evict(type.name)
        let buffered = buffers[type.name] ?? []
        var truncated = (evicted[type.name] ?? 0) > from
        var after = buffered.drop { $0.sequence <= from }
        var start = truncated ? (after.first.map { $0.sequence - 1 } ?? sequence) : from
        if let maxAgeMs {
            let floor = Date().addingTimeInterval(-Double(maxAgeMs) / 1000)
            let skipped = after.prefix { $0.date < floor }
            if let last = skipped.last {
                truncated = true
                start = last.sequence
                after = after.dropFirst(skipped.count)
            }
        }
        let matching = after.filter { type.match?($0.occurrence, arguments) ?? true }
        guard matching.count > limit else {
            return (matching.map(\.occurrence), head, start, truncated, false)
        }
        let page = matching.prefix(limit)
        return (page.map(\.occurrence), self.cursor(page.last!.sequence), start, truncated, true)
    }

    // MARK: - Helpers

    private func terminateStreams(
        of name: String, with error: MCPError,
        where accepts: (@Sendable ([String: Value], Server.HandlerContext) -> Bool)? = nil
    ) {
        for (token, stream) in streams where stream.name == name {
            if let accepts, !accepts(stream.arguments, stream.context) { continue }
            stream.items.yield(.terminated(error))
            stream.items.finish()
            streams[token] = nil
        }
    }

    private func sendListChanged() async {
        for sink in listChangedSinks.values { await sink() }
    }

    static func unknownType(_ name: String) -> MCPError {
        .notFound("Unknown event type: \(MCPRequestHeaders.sanitized(name))", data: ["kind": "event"])
    }

    static func milliseconds(_ duration: Duration) -> Int {
        let (seconds, attoseconds) = duration.components
        return Int(seconds) * 1000 + Int(attoseconds / 1_000_000_000_000_000)
    }

    static func seconds(_ duration: Duration) -> TimeInterval {
        Double(milliseconds(duration)) / 1000
    }
}

// MARK: - Server

extension Server {
    /// Serves the event types of `registry`: declares the
    /// `io.modelcontextprotocol/events` extension at 2026-07-28 and answers
    /// `events/list`, `events/poll` and `events/stream`.
    ///
    /// Servers that serve the same event types should share one registry.
    @discardableResult
    public func withEvents(_ registry: EventRegistry) async -> Self {
        var extensions = capabilities.extensions ?? [:]
        extensions[ExtensionID.events] = EventsSettings(listChanged: true).value
        capabilities.extensions = extensions
        registerDefault(ListEvents.self) { params in
            guard params.cursor == nil else { throw MCPError.invalidParams("Invalid cursor") }
            return ListEvents.Result(events: await registry.eventTypes.map(\.descriptor))
        }
        registerDefault(PollEvents.self) { params in
            try await Self.poll(params, registry: registry)
        }
        registerDefault(StreamEvents.self) { [weak self] params in
            guard let self else { throw MCPError.internalError("Server is gone") }
            return try await self.serveEventStream(params, registry: registry)
        }
        await registry.attach(self)
        return self
    }

    private static func requestContext() throws -> HandlerContext {
        guard let context = Server.currentHandlerContext else {
            throw MCPError.internalError("No request is being handled")
        }
        return context
    }

    private static func poll(_ params: PollEvents.Parameters, registry: EventRegistry) async throws
        -> PollEvents.Result
    {
        let context = try requestContext()
        let type = try await registry.resolve(params.name, mode: .poll)
        try await type.validate?(params.arguments, context)
        guard let check = type.check else {
            return await registry.poll(params, type: type)
        }
        let batch = try await check(
            EventCheck(
                name: type.name, arguments: params.arguments, cursor: params.cursor,
                maxEvents: await registry.maxEvents(params.maxEvents),
                maxAge: params.maxAgeMs.map { .milliseconds($0) }, context: context))
        let (events, dropped) = occurrences(
            of: batch, type: type, maxAgeMs: params.cursor == nil ? nil : params.maxAgeMs, after: nil)
        return PollEvents.Result(
            events: params.cursor == nil ? [] : events.map { occurrence in
                var occurrence = occurrence
                occurrence.cursor = nil
                return occurrence
            },
            cursor: batch.cursor, truncated: params.cursor != nil && (batch.truncated || dropped),
            hasMore: params.cursor != nil && batch.hasMore,
            nextPollMs: batch.nextPollMs ?? EventRegistry.milliseconds(registry.configuration.pollInterval))
    }

    /// An event without a cursor of its own gets the position before the batch, which is safe to persist.
    private static func occurrences(
        of batch: EventBatch, type: EventType, maxAgeMs: Int?, after previous: String?
    ) -> (events: [EventOccurrence], dropped: Bool) {
        let floor = maxAgeMs.map { Date().addingTimeInterval(-Double($0) / 1000) } ?? .distantPast
        let kept = batch.events.filter { $0.date >= floor }
        let events = kept.enumerated().map { index, event in
            EventOccurrence(
                eventId: event.eventId, name: type.name, date: event.date, data: event.data,
                cursor: event.cursor ?? (index == kept.count - 1 ? batch.cursor : previous), _meta: event._meta)
        }
        return (events, kept.count < batch.events.count)
    }

    private static func isTerminal(_ error: MCPError) -> Bool {
        [MCPError.Code.notFound, .forbidden, .unsupported].contains(error.errorCode)
    }

    private func serveEventStream(_ params: StreamEvents.Parameters, registry: EventRegistry) async throws
        -> StreamEvents.Result
    {
        let context = try Self.requestContext()
        let type = try await registry.resolve(params.name, mode: .push)
        try await type.validate?(params.arguments, context)
        let heartbeat = await registry.heartbeat(suggested: params.heartbeatMs)
        let heartbeatMs = EventRegistry.milliseconds(heartbeat)
        let maxEvents = await registry.maxEvents(nil)

        let (items, continuation) = AsyncStream<EventStreamItem>.makeStream(
            bufferingPolicy: .bufferingOldest(max(1, registry.buffer.maxEvents)))
        try beginEventStream(context.id, continuation)
        defer { endEventStream(context.id) }
        // Registered before `active`, so no occurrence emitted meanwhile is lost.
        let (token, start) = try await registry.openStream(
            params, type: type, context: context, items: continuation)
        do {
            let result = try await streamEvents(
                params, type: type, start: start, items: items, continuation: continuation,
                heartbeat: heartbeat, heartbeatMs: heartbeatMs, maxEvents: maxEvents, context: context,
                checkInterval: registry.configuration.checkInterval)
            await registry.closeStream(token)
            return result
        } catch {
            await registry.closeStream(token)
            throw error
        }
    }

    private func streamEvents(
        _ params: StreamEvents.Parameters, type: EventType, start: EventRegistry.StreamStart,
        items: AsyncStream<EventStreamItem>, continuation: AsyncStream<EventStreamItem>.Continuation,
        heartbeat: Duration, heartbeatMs: Int, maxEvents: Int, context: HandlerContext, checkInterval: Duration
    ) async throws -> StreamEvents.Result {
        let id = context.id
        var meta = Metadata()
        meta.subscriptionID = id

        var cursor = start.cursor
        var truncated = start.truncated
        var replay = start.replay
        var hasMore = false
        if let check = type.check {
            let batch = try await check(
                EventCheck(
                    name: type.name, arguments: params.arguments, cursor: params.cursor, maxEvents: maxEvents,
                    maxAge: params.maxAgeMs.map { .milliseconds($0) }, context: context))
            if params.cursor == nil {
                cursor = batch.cursor
            } else {
                let (events, dropped) = Self.occurrences(
                    of: batch, type: type, maxAgeMs: params.maxAgeMs, after: params.cursor)
                replay = events
                truncated = batch.truncated || dropped
                cursor = events.isEmpty ? batch.cursor : params.cursor
                hasMore = batch.hasMore
            }
        }

        try await sendOnStream(
            Message<EventsActiveNotification>(
                method: EventsActiveNotification.name,
                params: .init(cursor: cursor, truncated: truncated, heartbeatMs: heartbeatMs), _meta: meta),
            of: id)
        for occurrence in replay {
            try await sendOnStream(
                Message<EventNotification>(method: EventNotification.name, params: occurrence, _meta: meta), of: id)
            cursor = occurrence.cursor
        }

        let beat = Task {
            while (try? await Task.sleep(for: heartbeat)) != nil {
                continuation.yield(.heartbeat)
            }
        }
        defer { beat.cancel() }
        let checker = type.check.map { check in
            Task { [arguments = params.arguments, cursor, hasMore] in
                var position = cursor
                var immediate = hasMore
                while !Task.isCancelled {
                    if !immediate {
                        guard (try? await Task.sleep(for: checkInterval)) != nil else { return }
                    }
                    do {
                        let batch = try await check(
                            EventCheck(
                                name: type.name, arguments: arguments, cursor: position, maxEvents: maxEvents,
                                maxAge: nil, context: context))
                        let (events, _) = Self.occurrences(of: batch, type: type, maxAgeMs: nil, after: position)
                        immediate = batch.hasMore && (!events.isEmpty || batch.cursor != position)
                        position = batch.cursor
                        if case .dropped = continuation.yield(
                            .batch(events, cursor: batch.cursor, truncated: batch.truncated))
                        {
                            continuation.finish()
                            return
                        }
                    } catch is CancellationError {
                        return
                    } catch let error as MCPError where Self.isTerminal(error) {
                        continuation.yield(.terminated(error))
                        continuation.finish()
                        return
                    } catch {
                        continuation.yield(.error(error as? MCPError ?? .internalError(error.localizedDescription)))
                        immediate = false
                    }
                }
            }
        }
        defer { checker?.cancel() }

        delivery: for await item in items {
            switch item {
            case .event(let occurrence):
                try await sendOnStream(
                    Message<EventNotification>(method: EventNotification.name, params: occurrence, _meta: meta),
                    of: id)
                cursor = occurrence.cursor
            case .advance(let position):
                cursor = position
            case .batch(let events, let position, let gap):
                if gap {
                    try await sendOnStream(
                        Message<EventsActiveNotification>(
                            method: EventsActiveNotification.name,
                            params: .init(cursor: events.isEmpty ? position : cursor, truncated: true, heartbeatMs: heartbeatMs),
                            _meta: meta),
                        of: id)
                }
                for occurrence in events {
                    try await sendOnStream(
                        Message<EventNotification>(method: EventNotification.name, params: occurrence, _meta: meta),
                        of: id)
                }
                cursor = position
            case .heartbeat:
                try await sendOnStream(
                    Message<EventsHeartbeatNotification>(
                        method: EventsHeartbeatNotification.name, params: .init(cursor: cursor), _meta: meta),
                    of: id)
            case .error(let error):
                try await sendOnStream(
                    Message<EventsErrorNotification>(
                        method: EventsErrorNotification.name, params: .init(error: error), _meta: meta),
                    of: id)
            case .terminated(let error):
                try await sendOnStream(
                    Message<EventsTerminatedNotification>(
                        method: EventsTerminatedNotification.name, params: .init(error: error), _meta: meta),
                    of: id)
                break delivery
            }
        }
        try Task.checkCancellation()
        return StreamEvents.Result(_meta: meta)
    }
}
