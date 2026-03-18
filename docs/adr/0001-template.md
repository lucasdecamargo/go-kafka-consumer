# ADR-0001: ADR Template

## Status

Accepted

## Context

We need a lightweight way to record architectural decisions made in this project so that future contributors understand **why** choices were made, not just what the code does.

We adopt the Architecture Decision Record (ADR) format popularized by Michael Nygard.

## Decision

Each ADR is a numbered Markdown file in `docs/adr/` following this structure:

```
# ADR-NNNN: Title

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-XXXX

## Context
What is the issue or force motivating this decision?

## Decision
What is the change that we're proposing and/or doing?

## Consequences
What becomes easier or harder as a result of this decision?
```

**Conventions:**
- Files are named `NNNN-short-title.md` (e.g., `0002-use-badger-for-buffer.md`).
- ADRs are immutable once accepted. If a decision is reversed, a new ADR supersedes the old one.
- ADRs are submitted as part of the PR that implements the decision.

## Consequences

- Every significant architectural choice will have a traceable rationale.
- New contributors can read the ADR log to understand the project's evolution.
- Minor implementation details should NOT get ADRs — only decisions that affect the system's structure, dependencies, or guarantees.
