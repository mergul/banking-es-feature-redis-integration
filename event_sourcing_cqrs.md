# Event Sourcing (ES) and Command Query Responsibility Segregation (CQRS)

This document details the application of Event Sourcing and CQRS patterns within the system. These patterns are fundamental to how the system manages state, processes commands, and serves queries.

## Event Sourcing (ES)

Event Sourcing is a pattern where all changes to an application's state are stored as a sequence of immutable events. Instead of storing the current state of an entity, we store the history of events that have affected it.

### Core Concepts

*   **Events as the Source of Truth:** Events are facts about something that has happened in the past. They are immutable and represent the single source of truth. Examples: `AccountCreated`, `MoneyDeposited`, `TransactionInitiated`. These would typically be defined in `src/domain/events.rs`.
*   **Aggregates:** An aggregate is a cluster of domain objects (entities and value objects) that can be treated as a single unit. It has a root entity and a boundary. External references are restricted to the aggregate root. Aggregates are responsible for validating commands and, if successful, generating one or more domain events. Logic for this would reside in files like `src/domain/account.rs`.
*   **Event Store:** A specialized data store that persists events. Events are typically appended to a stream associated with a specific aggregate instance. The event store provides the capability to retrieve all events for an aggregate or events from a certain point in time.

### Event Storage (Example with PostgreSQL)

While the actual implementation is in `src/infrastructure/event_store.rs`, a common way to store events in PostgreSQL would be an `events` table similar to this:

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,          -- Unique event ID
    aggregate_id UUID NOT NULL,        -- ID of the aggregate instance
    aggregate_type VARCHAR(255) NOT NULL, -- Type of the aggregate (e.g., "Account", "User")
    version INT NOT NULL,              -- Sequence number of the event for this aggregate
    event_type VARCHAR(255) NOT NULL,  -- Type of the event (e.g., "AccountCreated")
    payload JSONB NOT NULL,            -- Event data (serialized)
    metadata JSONB,                    -- Additional metadata (e.g., user_id, correlation_id)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Timestamp of event creation

    CONSTRAINT unique_aggregate_event_version UNIQUE (aggregate_id, version)
);

CREATE INDEX idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX idx_events_event_type ON events (event_type);
CREATE INDEX idx_events_created_at ON events (created_at);
```
*(This SQL is illustrative; the actual schema is defined in migrations like `migrations/001_create_events_table.sql` and `migrations/007_add_unique_constraint_aggregate_id_version_to_events.sql`)*

### Reconstituting State

Aggregates load their current state by reading their historical events from the event store and applying each event to their internal state. This process is often called "rehydration."

### Benefits

*   **Auditability:** A complete log of all changes is available.
*   **Temporal Queries:** The state of an aggregate can be determined at any point in time.
*   **Debugging:** Easier to understand how an aggregate reached its current state.
*   **Flexibility for Read Models:** Events can be used to build various read models for different query needs.

## Command Query Responsibility Segregation (CQRS)

CQRS is an architectural pattern that separates the parts of a system that handle commands (writes/updates) from the parts that handle queries (reads).

### Core Concepts

*   **Command Side:**
    *   Handles incoming commands (e.g., `CreateAccountCommand`, `DepositMoneyCommand`). These would be defined in `src/domain/commands.rs`.
    *   Commands are processed by handlers (often in `src/application/handlers.rs` or within aggregates themselves).
    *   Command processing involves validating the command, executing business logic within an aggregate, and (if successful) generating and persisting events to the Event Store.
    *   The command side typically does not return data beyond acknowledging success or failure.

*   **Query Side:**
    *   Handles incoming queries to retrieve data.
    *   The query side reads from specialized "read models" or "projections." These read models are optimized for specific query requirements.
    *   Read models are updated asynchronously by consuming events published from the command side (often via an event bus like Kafka).
    *   This separation allows the read models to be denormalized and structured differently from the write-side aggregates, leading to efficient querying.

### Projections / Read Models

*   Projections are essentially event listeners that consume events and update read models. Logic for this could be in `src/infrastructure/projections.rs`.
*   Read models can be stored in various ways:
    *   Dedicated tables in a SQL database (e.g., PostgreSQL).
    *   A NoSQL database.
    *   In-memory caches like Redis (managed via `src/infrastructure/redis_abstraction.rs` and `src/infrastructure/cache_service.rs`).

### Benefits

*   **Scalability:** The command and query sides can be scaled independently.
*   **Optimized Data Models:** Read models can be tailored for specific query needs, improving query performance.
*   **Flexibility:** Different read models can be created from the same stream of events to serve diverse querying needs without impacting the write side.
*   **Separation of Concerns:** Simplifies both the command and query logic by separating them.

## Integration of ES and CQRS

Event Sourcing is often used as the persistence mechanism for the command side in a CQRS architecture.
1.  A **Command** is received by an Application Service/Handler.
2.  The handler loads the target **Aggregate** by replaying its events from the **Event Store**.
3.  The command is executed on the aggregate, which validates it and produces new **Domain Events**.
4.  These new events are atomically appended to the Event Store.
5.  The new events are then published (e.g., to Kafka).
6.  **Event Consumers** (Projections) on the query side listen to these events and update their respective **Read Models**.
7.  **Queries** are served directly from these optimized Read Models.

This combination provides a powerful and flexible architecture for complex systems.
