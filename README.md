# Banking ES (Event Sourcing) Project

A sample banking application demonstrating Event Sourcing principles with Rust.

## Overview

This project implements basic banking functionalities such as account creation, deposits, and withdrawals using an event-sourced approach. It showcases how to build a system where all changes to application state are stored as a sequence of events.

## Architecture & Services

The system is composed of several key parts:

*   **Domain Model:** Contains the core business logic and event definitions.
*   **Application Layer:** Orchestrates operations, handles commands, and uses services.
*   **Infrastructure Layer:** Manages data persistence (event store, projections) and external service integrations.
*   **Web API:** Exposes functionalities through an HTTP API (Axum).

### Key Dependencies & Services:

*   **PostgreSQL:** Used as the primary data store for events and projections.
*   **Redis:** Integrated for performance enhancement and specific functionalities:
    *   **Account Data Caching:** Acts as a read-through cache (L1 cache) for frequently accessed account data to reduce database load.
    *   **Event Batching:** Events are temporarily queued in Redis lists before being flushed to the PostgreSQL event store by a background worker. This can help in smoothing out write loads.
    *   **Command De-duplication:** Used to prevent processing of duplicate commands within a certain time window, ensuring idempotency for critical operations.

## Running the Project

### Prerequisites

*   Docker and Docker Compose
*   Rust toolchain (if building/running locally without Docker for the application)

### Using Docker Compose (Recommended for local development)

1.  **Environment Variables:**
    *   Ensure you have a `.env` file based on `.env.example` (if provided) or create one.
    *   The application requires `DATABASE_URL` for PostgreSQL and `REDIS_URL` for Redis.
    *   Default for Docker Compose:
        *   `DATABASE_URL=postgresql://postgres:password@postgres:5432/banking_es`
        *   `REDIS_URL=redis://redis:6379` (The `redis` hostname refers to the Redis service in `docker-compose.yml`)

2.  **Start Services:**
    ```bash
    docker-compose up --build
    ```
    This command will build the application container and start all necessary services, including PostgreSQL and Redis, as defined in `docker-compose.yml`.

3.  The application will typically be available at `http://localhost:3000`.

### Local Development (without Docker for the application)

If you run the application directly using `cargo run`:

1.  Ensure PostgreSQL and Redis instances are running and accessible.
2.  Set the `DATABASE_URL` and `REDIS_URL` environment variables to point to your instances. For example:
    ```bash
    export DATABASE_URL="postgresql://user:pass@host:port/dbname"
    export REDIS_URL="redis://your_redis_host:6379"
    cargo run
    ```

## API Endpoints

(Details about API endpoints would go here - e.g., create account, deposit, withdraw, get account)

---

This is a simplified README. A real project would include more details on API documentation, testing, contributing, etc.
