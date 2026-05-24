# AGENTS.md — Development Principles for ma-dlna

## Development Environment

- **Go development is done via Docker.** Do not install golang on the host system.
- **Use a persistent Dev Container.** Start one long-running container and execute commands inside it via `docker exec`. Do not create a new container for each operation.
- Start the dev container: `docker run -d --name ma-dlna-dev --network host -v $(pwd):/app -w /app ma-dlna-dev`
- Run Go commands inside: `docker exec ma-dlna-dev go ...`
- Shell into the container: `docker exec -it ma-dlna-dev bash`
- The dev image has Go only (no ffmpeg). For full testing with ffmpeg, build the production image.

## Progress Tracking

- Keep progress records so any Agent can pick up the work mid-stream.
- After completing each milestone/slice, update the task list and commit changes before moving on.

## Documentation Discipline

- **Progressive disclosure** — only document what's needed, when it's needed.
- Avoid bloated, outdated, or useless documentation files.
- The canonical spec lives in `docs/pre-PRD.md`. Implementation decisions should reference it.
- No README or extra .md files unless explicitly requested.

## Code Conventions

- Go standard project layout: `cmd/` for entry points, `internal/` for private packages.
- Logging via `log/slog`.
- Configuration via YAML file, loaded at startup.
- No comments unless the WHY is non-obvious. Code should be self-documenting.
