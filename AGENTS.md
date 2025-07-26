# Agent Instructions

- When building, testing, or running commands, use `make` targets provided in the Makefile. Common targets are `make clean build`, `make clean test`, and `make lint`.
- If any required tool is missing, inspect `.devcontainer/Dockerfile` and `.devcontainer/devcontainer.json` to see how the tool is installed. Attempt to replicate those installation steps using `apt-get` or the provided scripts before running the project commands.
- The devcontainer defines the expected environment; mirror it as closely as possible when preparing the workspace.
