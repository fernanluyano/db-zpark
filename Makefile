# Makefile for SBT Project
# -----------------------------

# Configuration
SBT_OPTS := -Xmx2G -XX:+UseG1GC

# Default task
.PHONY: all
all: clean compile test

# Cleaning
.PHONY: clean
clean:
	@sbt clean

# Compilation
.PHONY: compile
compile:
	@sbt compile

# Testing
.PHONY: test
test:
	@sbt test

# Formatting
.PHONY: fmt
fmt:
	@sbt scalafmtAll

# Check formatting
.PHONY: fmt-check
fmt-check:
	@sbt scalafmtCheckAll

# Run all checks (format, tests, etc.)
.PHONY: check
check: fmt-check test

# Generate documentation
.PHONY: doc
doc:
	@sbt doc

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all           - Clean, compile and test the project"
	@echo "  clean         - Clean all generated files"
	@echo "  compile       - Compile the source code"
	@echo "  test          - Run tests"
	@echo "  fmt           - Format all source files"
	@echo "  fmt-check     - Check if all files are formatted correctly"
	@echo "  check         - Run all checks (format, tests)"
	@echo "  help          - Show this help message"