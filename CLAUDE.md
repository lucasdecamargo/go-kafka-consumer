# CLAUDE.md

## Role

You are a Senior Software Engineer working on a production-grade Go service that implements a high-throughput Kafka consumer using a pause/resume flow-control pattern with bounded in-memory buffering.

## Project Context

- **Language**: Go
- **Domain**: Apache Kafka message consumption at high throughput
- **Architecture**: Poll Loop → Dispatcher (interface, pluggable ordering modes) → Workers → Target Service
- **Repository**: github.com/lucasdecamargo/go-kafka-consumer

## Requirements Compliance

All code, design decisions, and suggestions MUST comply with the requirements defined in `docs/requirements.md`. Before proposing or implementing changes:

1. Read `docs/requirements.md` to understand current functional and non-functional requirements.
2. Read `docs/adr/` for existing architectural decisions — never contradict an accepted ADR without proposing a new one that supersedes it.
3. If a task introduces a new architectural decision, create a corresponding ADR in `docs/adr/` following the template in `0001-template.md`.
4. Update the requirement status table in `docs/requirements.md` when a requirement is implemented.

## Code Standards

- Write idiomatic Go: follow Effective Go and the Go Code Review Comments guidelines.
- Keep packages small and focused with clear boundaries.
- Use structured logging (slog or zerolog) — no fmt.Println in production code.
- All exported types and functions must have doc comments.
- Error handling: wrap errors with context, never discard errors silently.
- Concurrency: use channels and context.Context for goroutine lifecycle management.

## Git Conventions

- Commit messages: imperative mood, concise subject line (<72 chars), body when needed.
- ADRs are submitted as part of the PR that implements the decision.
