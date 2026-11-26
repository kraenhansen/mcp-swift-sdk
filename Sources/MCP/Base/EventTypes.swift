import Foundation

extension ExtensionID {
    /// The events extension (SEP-3415).
    public static let events = "io.modelcontextprotocol/events"
}

/// The events extension's settings in `capabilities.extensions`.
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#capability-negotiation
public struct EventsSettings: Hashable, Codable, Sendable {
    /// Whether the server sends `notifications/events/list_changed`.
    public var listChanged: Bool?

    /// Creates the settings.
    public init(listChanged: Bool? = nil) {
        self.listChanged = listChanged
    }

    /// The settings `extensions` declares, or `nil` when it declares none.
    public init?(_ extensions: [String: Value]?) {
        guard let value = extensions?[ExtensionID.events]?.objectValue else { return nil }
        listChanged = value["listChanged"]?.boolValue
    }

    var value: Value {
        listChanged.map { ["listChanged": .bool($0)] } ?? [:]
    }
}

/// A delivery mode of an event type; an open type.
public struct EventDeliveryMode: RawRepresentable, Hashable, Codable, Sendable,
    ExpressibleByStringLiteral, CustomStringConvertible
{
    public let rawValue: String

    public init(rawValue: String) {
        self.rawValue = rawValue
    }

    public init(stringLiteral value: String) {
        self.rawValue = value
    }

    public init(from decoder: Decoder) throws {
        rawValue = try decoder.singleValueContainer().decode(String.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public var description: String { rawValue }

    /// `events/poll`.
    public static let poll: EventDeliveryMode = "poll"
    /// `events/stream`.
    public static let push: EventDeliveryMode = "push"
    /// `events/subscribe`, which this SDK does not serve.
    public static let webhook: EventDeliveryMode = "webhook"
}

/// An event type as `events/list` describes it.
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#listing-available-events
public struct EventDescriptor: Hashable, Codable, Sendable {
    /// The event type's name, such as `email.received`.
    public var name: String
    /// What the event type reports.
    public var description: String?
    /// The delivery modes the event type supports; never empty.
    public var delivery: [EventDeliveryMode]
    /// The JSON Schema of subscription `arguments`.
    public var inputSchema: Value
    /// The JSON Schema of each occurrence's `data`.
    public var payloadSchema: Value
    /// Metadata of the event type.
    public var _meta: Metadata?

    /// Creates a descriptor.
    public init(
        name: String, description: String? = nil, delivery: [EventDeliveryMode], inputSchema: Value,
        payloadSchema: Value, _meta: Metadata? = nil
    ) {
        self.name = name
        self.description = description
        self.delivery = delivery
        self.inputSchema = inputSchema
        self.payloadSchema = payloadSchema
        self._meta = _meta
    }

    private enum CodingKeys: String, CodingKey {
        case name, description, delivery, inputSchema, payloadSchema, _meta
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        description = try container.decodeIfPresent(String.self, forKey: .description)
        delivery = try container.decodeIfPresent([EventDeliveryMode].self, forKey: .delivery) ?? []
        inputSchema = try container.decodeIfPresent(Value.self, forKey: .inputSchema) ?? ["type": "object"]
        payloadSchema = try container.decodeIfPresent(Value.self, forKey: .payloadSchema) ?? ["type": "object"]
        _meta = try container.decodeIfPresent(Metadata.self, forKey: ._meta)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encodeIfPresent(description, forKey: .description)
        try container.encode(delivery, forKey: .delivery)
        try container.encode(inputSchema, forKey: .inputSchema)
        try container.encode(payloadSchema, forKey: .payloadSchema)
        try container.encodeIfPresent(_meta, forKey: ._meta)
    }
}

/// One occurrence of an event.
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#event-occurrences
public struct EventOccurrence: Hashable, Codable, Sendable {
    /// The upstream event's stable identifier, for deduplication.
    public var eventId: String
    /// The event type's name.
    public var name: String
    /// When the event occurred, as an ISO 8601 timestamp.
    public var timestamp: String
    /// The payload, which conforms to the event type's `payloadSchema`.
    public var data: [String: Value]
    /// The position after this event on push streams; absent reads as `nil`.
    public var cursor: String?
    /// Metadata of the occurrence, not governed by `payloadSchema`.
    public var _meta: Metadata?

    /// Creates an occurrence.
    public init(
        eventId: String, name: String, timestamp: String, data: [String: Value], cursor: String? = nil,
        _meta: Metadata? = nil
    ) {
        self.eventId = eventId
        self.name = name
        self.timestamp = timestamp
        self.data = data
        self.cursor = cursor
        self._meta = _meta
    }

    /// Creates an occurrence that happened at `date`.
    public init(
        eventId: String, name: String, date: Date, data: [String: Value], cursor: String? = nil,
        _meta: Metadata? = nil
    ) {
        self.init(
            eventId: eventId, name: name, timestamp: TaskTimestamp.string(from: date), data: data, cursor: cursor,
            _meta: _meta)
    }

    /// ``timestamp`` as a date; `nil` when it is not an ISO 8601 timestamp.
    public var date: Date? { TaskTimestamp.date(from: timestamp) }

    private enum CodingKeys: String, CodingKey {
        case eventId, name, timestamp, data, cursor, _meta
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        eventId = try container.decode(String.self, forKey: .eventId)
        name = try container.decode(String.self, forKey: .name)
        timestamp = try container.decode(String.self, forKey: .timestamp)
        data = try container.decodeIfPresent([String: Value].self, forKey: .data) ?? [:]
        cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
        _meta = try container.decodeIfPresent(Metadata.self, forKey: ._meta)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(eventId, forKey: .eventId)
        try container.encode(name, forKey: .name)
        try container.encode(timestamp, forKey: .timestamp)
        try container.encode(data, forKey: .data)
        try container.encodeIfPresent(cursor, forKey: .cursor)
        try container.encodeIfPresent(_meta, forKey: ._meta)
    }
}

// MARK: - Methods

/// Lists the server's event types (`events/list`).
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#listing-available-events
public enum ListEvents: Method {
    public static let name = "events/list"

    /// The parameters of `events/list`.
    public struct Parameters: NotRequired, Hashable, Codable, Sendable {
        /// The cursor from the `nextCursor` of a previous result, or `nil` for the first page.
        public var cursor: String?

        /// Creates the parameters of a request for the first page.
        public init() {}

        /// Creates the parameters of a request for the page at `cursor`.
        public init(cursor: String?) {
            self.cursor = cursor
        }
    }

    /// The result of `events/list`.
    public struct Result: Hashable, Codable, Sendable {
        /// The event types on this page.
        public var events: [EventDescriptor]
        /// The cursor of the next page, or `nil` on the last page.
        public var nextCursor: String?
        /// Metadata of the result.
        public var _meta: Metadata?

        /// Creates the result of an `events/list` request.
        public init(events: [EventDescriptor], nextCursor: String? = nil, _meta: Metadata? = nil) {
            self.events = events
            self.nextCursor = nextCursor
            self._meta = _meta
        }
    }
}

/// Reads the occurrences of one subscription since a cursor (`events/poll`).
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#poll-based-delivery
public enum PollEvents: Method {
    public static let name = "events/poll"

    /// The parameters of `events/poll`.
    public struct Parameters: Hashable, Codable, Sendable {
        /// The event type's name.
        public var name: String
        /// The subscription arguments, valid against the type's `inputSchema`.
        public var arguments: [String: Value]
        /// The cursor of the previous poll; `nil` starts from now.
        public var cursor: String?
        /// Replay no events older than this many milliseconds.
        public var maxAgeMs: Int?
        /// The most events to return.
        public var maxEvents: Int?

        /// Creates poll parameters.
        public init(
            name: String, arguments: [String: Value] = [:], cursor: String? = nil, maxAgeMs: Int? = nil,
            maxEvents: Int? = nil
        ) {
            self.name = name
            self.arguments = arguments
            self.cursor = cursor
            self.maxAgeMs = maxAgeMs
            self.maxEvents = maxEvents
        }

        private enum CodingKeys: String, CodingKey {
            case name, arguments, cursor, maxAgeMs, maxEvents
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            name = try container.decode(String.self, forKey: .name)
            arguments = try container.decodeIfPresent([String: Value].self, forKey: .arguments) ?? [:]
            cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
            maxAgeMs = try container.decodeIfPresent(Int.self, forKey: .maxAgeMs)
            maxEvents = try container.decodeIfPresent(Int.self, forKey: .maxEvents)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(name, forKey: .name)
            try container.encode(arguments, forKey: .arguments)
            try container.encode(cursor, forKey: .cursor)
            try container.encodeIfPresent(maxAgeMs, forKey: .maxAgeMs)
            try container.encodeIfPresent(maxEvents, forKey: .maxEvents)
        }
    }

    /// The result of `events/poll`.
    public struct Result: Hashable, Codable, Sendable {
        /// The occurrences since the cursor, oldest first.
        public var events: [EventOccurrence]
        /// The cursor for the next poll; `nil` for event types without replay.
        public var cursor: String?
        /// Whether events after the supplied cursor were skipped.
        public var truncated: Bool
        /// Whether more events wait: poll again at once.
        public var hasMore: Bool
        /// When to poll next, in milliseconds, when ``hasMore`` is `false`.
        public var nextPollMs: Int?
        /// Metadata of the result.
        public var _meta: Metadata?

        /// Creates the result of an `events/poll` request.
        public init(
            events: [EventOccurrence], cursor: String?, truncated: Bool = false, hasMore: Bool = false,
            nextPollMs: Int? = nil, _meta: Metadata? = nil
        ) {
            self.events = events
            self.cursor = cursor
            self.truncated = truncated
            self.hasMore = hasMore
            self.nextPollMs = nextPollMs
            self._meta = _meta
        }

        private enum CodingKeys: String, CodingKey {
            case events, cursor, truncated, hasMore, nextPollMs, _meta
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            events = try container.decodeIfPresent([EventOccurrence].self, forKey: .events) ?? []
            cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
            truncated = try container.decodeIfPresent(Bool.self, forKey: .truncated) ?? false
            hasMore = try container.decodeIfPresent(Bool.self, forKey: .hasMore) ?? false
            nextPollMs = try container.decodeIfPresent(Int.self, forKey: .nextPollMs)
            _meta = try container.decodeIfPresent(Metadata.self, forKey: ._meta)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(events, forKey: .events)
            try container.encode(cursor, forKey: .cursor)
            try container.encode(truncated, forKey: .truncated)
            try container.encode(hasMore, forKey: .hasMore)
            try container.encodeIfPresent(nextPollMs, forKey: .nextPollMs)
            try container.encodeIfPresent(_meta, forKey: ._meta)
        }
    }
}

/// Opens a push stream for one subscription (`events/stream`).
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#push-based-delivery
public enum StreamEvents: Method {
    public static let name = "events/stream"

    /// The parameters of `events/stream`.
    public struct Parameters: Hashable, Codable, Sendable {
        /// The event type's name.
        public var name: String
        /// The subscription arguments, valid against the type's `inputSchema`.
        public var arguments: [String: Value]
        /// The cursor to replay from; `nil` starts from now.
        public var cursor: String?
        /// Replay no events older than this many milliseconds.
        public var maxAgeMs: Int?
        /// The heartbeat interval the client suggests, in milliseconds.
        public var heartbeatMs: Int?

        /// Creates stream parameters.
        public init(
            name: String, arguments: [String: Value] = [:], cursor: String? = nil, maxAgeMs: Int? = nil,
            heartbeatMs: Int? = nil
        ) {
            self.name = name
            self.arguments = arguments
            self.cursor = cursor
            self.maxAgeMs = maxAgeMs
            self.heartbeatMs = heartbeatMs
        }

        private enum CodingKeys: String, CodingKey {
            case name, arguments, cursor, maxAgeMs, heartbeatMs
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            name = try container.decode(String.self, forKey: .name)
            arguments = try container.decodeIfPresent([String: Value].self, forKey: .arguments) ?? [:]
            cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
            maxAgeMs = try container.decodeIfPresent(Int.self, forKey: .maxAgeMs)
            heartbeatMs = try container.decodeIfPresent(Int.self, forKey: .heartbeatMs)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(name, forKey: .name)
            try container.encode(arguments, forKey: .arguments)
            try container.encode(cursor, forKey: .cursor)
            try container.encodeIfPresent(maxAgeMs, forKey: .maxAgeMs)
            try container.encodeIfPresent(heartbeatMs, forKey: .heartbeatMs)
        }
    }

    /// The result that closes an `events/stream` stream the server ended.
    public struct Result: Hashable, Codable, Sendable {
        /// Carries `io.modelcontextprotocol/subscriptionId`.
        public var _meta: Metadata?

        /// Creates a stream result.
        public init(_meta: Metadata? = nil) {
            self._meta = _meta
        }
    }
}

enum EventWebhookMethods {
    static let subscribe = "events/subscribe"
    static let unsubscribe = "events/unsubscribe"
}

// MARK: - Notifications

/// The first message on an `events/stream` stream, and again after a gap.
public struct EventsActiveNotification: Notification {
    public static let name = "notifications/events/active"

    /// The parameters of `notifications/events/active`.
    public struct Parameters: Hashable, Codable, Sendable {
        /// The position delivery starts from; `nil` for event types without replay.
        public var cursor: String?
        /// Whether delivery starts later than the cursor the client supplied.
        public var truncated: Bool
        /// The granted heartbeat interval, in milliseconds.
        public var heartbeatMs: Int

        /// Creates the parameters.
        public init(cursor: String?, truncated: Bool = false, heartbeatMs: Int) {
            self.cursor = cursor
            self.truncated = truncated
            self.heartbeatMs = heartbeatMs
        }

        private enum CodingKeys: String, CodingKey {
            case cursor, truncated, heartbeatMs
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
            truncated = try container.decodeIfPresent(Bool.self, forKey: .truncated) ?? false
            heartbeatMs = try container.decode(Int.self, forKey: .heartbeatMs)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(cursor, forKey: .cursor)
            try container.encode(truncated, forKey: .truncated)
            try container.encode(heartbeatMs, forKey: .heartbeatMs)
        }
    }
}

/// An occurrence on an `events/stream` stream.
public struct EventNotification: Notification {
    public static let name = "notifications/events/event"

    /// The occurrence.
    public typealias Parameters = EventOccurrence
}

/// A keepalive on an `events/stream` stream, with the position checked up to.
public struct EventsHeartbeatNotification: Notification {
    public static let name = "notifications/events/heartbeat"

    /// The parameters of `notifications/events/heartbeat`.
    public struct Parameters: Hashable, Codable, Sendable {
        /// The position to persist; `nil` for event types without replay.
        public var cursor: String?

        /// Creates the parameters.
        public init(cursor: String?) {
            self.cursor = cursor
        }

        private enum CodingKeys: String, CodingKey {
            case cursor
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(cursor, forKey: .cursor)
        }
    }
}

/// The parameters of ``EventsErrorNotification`` and ``EventsTerminatedNotification``.
public struct EventsErrorParameters: Hashable, Codable, Sendable {
    /// The error.
    public var error: MCPError

    /// Creates the parameters.
    public init(error: MCPError) {
        self.error = error
    }
}

/// A recoverable failure on an `events/stream` stream.
public struct EventsErrorNotification: Notification {
    public static let name = "notifications/events/error"

    /// The error.
    public typealias Parameters = EventsErrorParameters
}

/// The end of the subscription of an `events/stream` stream.
///
/// - SeeAlso: https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx#subscription-termination
public struct EventsTerminatedNotification: Notification {
    public static let name = "notifications/events/terminated"

    /// The error.
    public typealias Parameters = EventsErrorParameters
}

/// The server's event types changed.
public struct EventsListChangedNotification: Notification {
    public static let name = "notifications/events/list_changed"
}

// MARK: - Errors

extension MCPError.Code {
    /// `NotFound`: an unknown event name (SEP-3415): -32023.
    public static let notFound: Self = -32023
    /// `Forbidden`: the principal may not subscribe, or lost access (SEP-3415): -32024.
    public static let forbidden: Self = -32024
    /// `ResourceExhausted`: a server limit or quota was reached (SEP-3415): -32025.
    public static let resourceExhausted: Self = -32025
    /// `Unsupported`: a requested option is not supported (SEP-3415): -32026.
    public static let unsupported: Self = -32026
    /// `CallbackEndpointError`: a webhook endpoint failed (SEP-3415): -32027.
    public static let callbackEndpointError: Self = -32027
}

extension MCPError {
    /// A `NotFound` error (-32023); `data.kind` is `"event"` for an unknown event name.
    public static func notFound(_ message: String = "NotFound", data: Value? = nil) -> MCPError {
        .serverError(code: Code.notFound.rawValue, message: message, data: data)
    }

    /// A `Forbidden` error (-32024).
    public static func forbidden(_ message: String = "Forbidden", data: Value? = nil) -> MCPError {
        .serverError(code: Code.forbidden.rawValue, message: message, data: data)
    }

    /// A `ResourceExhausted` error (-32025); `data.limit` names the limit.
    public static func resourceExhausted(_ message: String = "ResourceExhausted", data: Value? = nil) -> MCPError {
        .serverError(code: Code.resourceExhausted.rawValue, message: message, data: data)
    }

    /// An `Unsupported` error (-32026); `data.feature` names what is not supported.
    public static func unsupported(_ message: String = "Unsupported", data: Value? = nil) -> MCPError {
        .serverError(code: Code.unsupported.rawValue, message: message, data: data)
    }
}
