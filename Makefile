# Makefile for SBT Project
# -----------------------------

# Configuration
SBT_OPTS := -Xmx2G -XX:+UseG1GC

# Build task
.PHONY: build
build:
	@echo 'building project...'
	@echo 'cleaning...'
	@sbt clean
	@echo 'checking formatting...'
	@sbt scalafmtCheckAll
	@echo 'compiling...'
	@sbt compile
	@echo 'running tests...'
	@sbt test
	@echo 'done building project'

# Code formatting task
.PHONY: format
format:
	@sbt scalafmtAll

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build         - Clean, compile and test the project"
	@echo "  format        - Format the code"
	@echo "  help          - Show this help message"