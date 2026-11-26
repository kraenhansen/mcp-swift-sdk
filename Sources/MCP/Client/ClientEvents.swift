import Foundation

/// A message on an `events/stream` stream after its first `active` notification.
public enum EventStreamMessage: Hashable, Sendable {
    /// An occurrence.
    case event(EventOccurrence)
    /// A heartbeat, with the position to persist.
    case heartbeat(cursor: String?)
    /// A gap: delivery continues from a later position than the client's.
    case active(EventsActiveNotification.Parameters)
    /// A recoverable failure; the subscription stays active.
    case error(MCPError)
}

/// An open `events/stream` stream.
///
/// ``messages`` ends when the server closes the stream or after
/// ``cancel()``, and throws the error of a `terminated` notification or of
/// the request.
public struct EventSubscription: Sendable {
    /// The JSON-RPC id of the `events/stream` request, the stream's `subscriptionId`.
    public let id: ID
    /// The stream's first `notifications/events/active`.
    public let active: EventsActiveNotification.Parameters
    /// The messages after ``active``.
    public let messages: AsyncThrowingStream<EventStreamMessage, Swift.Error>

    private let client: Client

    init(
        id: ID, active: EventsActiveNotification.Parameters,
        messages: AsyncThrowingStream<EventStreamMessage, Swift.Error>, client: Client
    ) {
        self.id = id
        self.active = active
        self.messages = messages
        self.client = client
    }

    /// Closes the stream: `notifications/cancelled` on stdio, the end of the
    /// response over HTTP.
    public func cancel() async {
        try? await client.cancelRequest(id, reason: "Subscription cancelled")
    }
}

struct EventRoute: Sendable {
    let messages: AsyncThrowingStream<EventStreamMessage, Swift.Error>.Continuation
    var activation: AsyncStream<Result<EventsActiveNotification.Parameters, Swift.Error>>.Continuation?
}

extension Client {
    /// The events extension settings the server declared, or `nil`.
    public var serverEvents: EventsSettings? {
        EventsSettings(serverCapabilities?.extensions)
    }

    private func requireEvents(_ method: String) throws {
        guard serverEvents != nil else {
            throw MCPError.methodNotFound("\(method): the server does not declare \(ExtensionID.events)")
        }
    }

    /// Lists one page of the server's event types (`events/list`).
    ///
    /// - Throws: -32601, without a request, when the server does not declare the events extension.
    public func listEvents(cursor: String? = nil) async throws -> ListEvents.Result {
        try requireEvents(ListEvents.name)
        return try await send(ListEvents.request(.init(cursor: cursor))).value
    }

    /// Polls one subscription (`events/poll`). Pass the result's `cursor`
    /// to the next poll, at once while `hasMore`, else after `nextPollMs`.
    ///
    /// - Throws: -32601, without a request, when the server does not declare
    ///   the events extension; `NotFound`, `Forbidden`, `Unsupported` or
    ///   -32602 from the server.
    public func pollEvents(_ parameters: PollEvents.Parameters) async throws -> PollEvents.Result {
        try requireEvents(PollEvents.name)
        return try await send(PollEvents.request(parameters)).value
    }

    /// Opens a push stream for one subscription (`events/stream`) and waits
    /// for its `notifications/events/active`.
    ///
    /// - Throws: -32601, without a request, when the server does not declare
    ///   the events extension; the server's error when it refuses the subscription.
    public func streamEvents(_ parameters: StreamEvents.Parameters) async throws -> EventSubscription {
        try requireEvents(StreamEvents.name)
        let request = StreamEvents.request(parameters)
        let (messages, continuation) = AsyncThrowingStream<EventStreamMessage, Swift.Error>.makeStream()
        let (outcomes, outcome) = AsyncStream<Result<EventsActiveNotification.Parameters, Swift.Error>>.makeStream()
        eventRoutes[request.id] = EventRoute(messages: continuation, activation: outcome)

        let context = try send(request)
        Task {
            let failure: (any Swift.Error)?
            do {
                _ = try await context.value
                failure = nil
            } catch is CancellationError {
                failure = nil
            } catch {
                failure = error
            }
            self.endEventRoute(request.id, failure: failure)
        }
        var active: EventsActiveNotification.Parameters?
        // A stream of outcomes, not a task group: a cancelled caller must stop waiting.
        for await result in outcomes {
            active = try result.get()
            break
        }
        guard let active else {
            try? await cancelRequest(request.id)
            throw CancellationError()
        }
        return EventSubscription(id: request.id, active: active, messages: messages, client: self)
    }

    private func endEventRoute(_ id: ID, failure: (any Swift.Error)?) {
        guard let route = eventRoutes.removeValue(forKey: id) else { return }
        route.activation?.yield(
            .failure(failure ?? MCPError.internalError("\(StreamEvents.name) ended before it was active")))
        if let failure {
            route.messages.finish(throwing: failure)
        } else {
            route.messages.finish()
        }
    }

    func routeEvent(_ message: Message<AnyNotification>) {
        guard let id = message._meta?.subscriptionID, var route = eventRoutes[id] else { return }
        func decode<T: Decodable>(_ type: T.Type) -> T? {
            guard let data = try? JSONEncoder().encode(message.params) else { return nil }
            return try? JSONDecoder().decode(T.self, from: data)
        }
        switch message.method {
        case EventsActiveNotification.name:
            guard let params = decode(EventsActiveNotification.Parameters.self) else { return }
            if let activation = route.activation {
                activation.yield(.success(params))
                route.activation = nil
                eventRoutes[id] = route
            } else {
                route.messages.yield(.active(params))
            }
        case EventNotification.name:
            guard var occurrence = decode(EventOccurrence.self) else { return }
            occurrence._meta?.fields[MetaKey.subscriptionID] = nil
            if occurrence._meta?.fields.isEmpty == true { occurrence._meta = nil }
            route.messages.yield(.event(occurrence))
        case EventsHeartbeatNotification.name:
            guard let params = decode(EventsHeartbeatNotification.Parameters.self) else { return }
            route.messages.yield(.heartbeat(cursor: params.cursor))
        case EventsErrorNotification.name:
            guard let params = decode(EventsErrorParameters.self) else { return }
            route.messages.yield(.error(params.error))
        case EventsTerminatedNotification.name:
            guard let params = decode(EventsErrorParameters.self) else { return }
            eventRoutes[id] = nil
            route.activation?.yield(.failure(params.error))
            route.messages.finish(throwing: params.error)
        default:
            break
        }
    }
}
