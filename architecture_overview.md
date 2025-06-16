# Architectural Overview

This document provides a high-level overview of the system's architecture, which is designed to be a robust, scalable, and maintainable event-driven platform. The architecture follows a layered approach, separating concerns to enhance modularity and testability.

## Layers

The system is primarily divided into the following distinct layers:

1.  **Domain Layer (`src/domain/`)**
    *   **Responsibilities:** Contains the core business logic, entities, aggregates, value objects, and domain events. This layer is the heart of the application and is kept independent of infrastructure concerns. It defines *what* the system does in terms of its business capabilities.
    *   **Key Patterns:** Event Sourcing, Domain-Driven Design (DDD) aggregates.
    *   **Technologies:** Pure Rust, focusing on business rules.

2.  **Application Layer (`src/application/`)**
    *   **Responsibilities:** Orchestrates the use cases of the application. It handles commands, coordinates with the domain layer to execute business logic, and manages transactions. It acts as an intermediary between the presentation/web layer and the domain layer. It may also be responsible for authorizing requests and triggering side effects like publishing events to a message broker (after they are committed by the domain layer).
    *   **Key Components:** Command Handlers, Application Services, potentially Query Services (if not using separate read models extensively).
    *   **Technologies:** Rust.

3.  **Infrastructure Layer (`src/infrastructure/`)**
    *   **Responsibilities:** Implements the technical concerns that support the other layers. This includes data persistence (event store, read model repositories), message queueing (Kafka integration), caching (Redis integration), external service integrations, security implementations (authentication, authorization), logging, metrics, and configuration management.
    *   **Key Components:** Event Store implementation (e.g., using PostgreSQL), Kafka producers/consumers, Redis client integrations, database connection management (SQLx), configuration services.
    *   **Technologies:** Rust, PostgreSQL (via SQLx), Kafka (via `rdkafka`), Redis (via `redis-rs`), Serde (for serialization/deserialization).

4.  **Web Layer / Presentation Layer (`src/web/` or `src/main.rs` for API setup)**
    *   **Responsibilities:** Exposes the application's functionality to the outside world, typically via an HTTP API (REST or gRPC). It handles incoming requests, parses them, passes them to the application layer (usually as commands or queries), and formats the responses.
    *   **Key Components:** API route handlers, request/response models (DTOs), middleware for concerns like authentication, logging, and error handling.
    *   **Technologies:** Rust web frameworks (e.g., Actix Web, Axum, Rocket), JSON for data interchange.

## Core Principles

*   **Separation of Concerns:** Each layer has distinct responsibilities, reducing coupling and improving maintainability.
*   **Dependency Rule:** Dependencies generally flow inwards: Web Layer -> Application Layer -> Domain Layer. The Domain Layer should have no knowledge of the outer layers. The Infrastructure Layer provides implementations for interfaces defined typically by the Application or Domain layers.
*   **Event-Driven:** The system relies heavily on events for state changes and communication between components, particularly through Kafka.
*   **CQRS (Command Query Responsibility Segregation):** While not explicitly detailed here, the structure supports CQRS by separating command processing (leading to state changes and events) from query processing (reading from potentially optimized read models).
*   **Resiliency and Scalability:** The choice of technologies like Kafka and Rust, along with patterns like event sourcing, aims to build a resilient and scalable system.

## Technology Stack Summary

*   **Language:** Rust
*   **Database (Event Store / Projections):** PostgreSQL (interacted with via SQLx)
*   **Event Streaming:** Apache Kafka
*   **Caching:** Redis
*   **Containerization:** Docker
*   **Build System:** Cargo (for Rust), CMake (potentially for C/C++ dependencies like `librdkafka`)

This overview provides a foundational understanding. More detailed documentation for specific aspects like Event Sourcing, CQRS, and Event Streaming integration can be found in their respective documents.
