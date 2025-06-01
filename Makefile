# Makefile for duck-shard.sh testing

.PHONY: test test-verbose test-specific clean install-deps help check-deps

# Default target
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  test-specific - Run specific test (use PATTERN=<pattern>)"
	@echo "  check-deps    - Check if dependencies are installed"
	@echo "  install-deps  - Install testing dependencies (macOS/Ubuntu)"
	@echo "  clean         - Clean up test artifacts"
	@echo ""
	@echo "Examples:"
	@echo "  make test"
	@echo "  make test-specific PATTERN='single.*conversion'"
	@echo "  make test-verbose"

# Check if required dependencies are installed
check-deps:
	@echo "Checking dependencies..."
	@command -v bats >/dev/null 2>&1 || { echo "❌ bats not installed"; exit 1; }
	@command -v duckdb >/dev/null 2>&1 || { echo "❌ duckdb not installed"; exit 1; }
	@echo "✅ All dependencies available"

# Run all tests
test: check-deps
	@echo "🧪 Running all tests..."
	cd tests && bats test.bats

# Run tests with verbose output
test-verbose: check-deps
	@echo "🧪 Running tests with verbose output..."
	cd tests && bats -p test.bats

# Run specific tests based on pattern
test-specific: check-deps
	@echo "🧪 Running tests matching pattern: $(PATTERN)"
	@if [ -z "$(PATTERN)" ]; then \
		echo "❌ Please specify PATTERN=<pattern>"; \
		echo "Example: make test-specific PATTERN='single.*conversion'"; \
		exit 1; \
	fi
	cd tests && bats -f "$(PATTERN)" test.bats

# Install dependencies on macOS (Homebrew) or Ubuntu/Debian
install-deps:
	@echo "🔧 Installing dependencies..."
	@if command -v brew >/dev/null 2>&1; then \
		echo "📦 Installing via Homebrew..."; \
		brew install bats-core duckdb; \
	elif command -v apt-get >/dev/null 2>&1; then \
		echo "📦 Installing via apt-get..."; \
		sudo apt-get update; \
		sudo apt-get install -y bats; \
		echo "📦 Installing DuckDB..."; \
		wget -q https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip; \
		unzip -q duckdb_cli-linux-amd64.zip; \
		sudo mv duckdb /usr/local/bin/; \
		rm duckdb_cli-linux-amd64.zip; \
	else \
		echo "❌ Unsupported package manager. Please install bats and duckdb manually."; \
		exit 1; \
	fi
	@echo "✅ Dependencies installed"

# Clean up test artifacts
clean:
	@echo "🧹 Cleaning up test artifacts..."
	@find . -name "*.tmp" -delete 2>/dev/null || true
	@find . -name "test_temp_*" -type d -exec rm -rf {} + 2>/dev/null || true
	@find ./tmp -type f ! -name '.gitkeep' -exec rm -f {} + 2>/dev/null || true
	@echo "✅ Cleanup complete"

# Quick test for CI/CD
ci-test: check-deps
	@echo "🚀 Running CI tests..."
	cd tests && bats --formatter junit test.bats

# Generate test coverage report (if you have shellcheck)
lint:
	@echo "🔍 Linting shell scripts..."
	@if command -v shellcheck >/dev/null 2>&1; then \
		shellcheck parquet-to.sh; \
		echo "✅ Linting complete"; \
	else \
		echo "⚠️  shellcheck not installed, skipping lint"; \
	fi

# Run tests with timing information
test-timing: check-deps
	@echo "⏱️  Running tests with timing..."
	cd tests && bats -T test.bats

# Run tests and generate junit XML output
test-junit: check-deps
	@echo "📊 Running tests with JUnit output..."
	cd tests && bats --formatter junit test.bats > test_results.xml
	@echo "✅ Results saved to tests/test_results.xml"
