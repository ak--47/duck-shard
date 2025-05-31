bash# From project root
make test

# Or directly
cd tests && bats test.bats

# Run specific tests
make test-specific PATTERN="event.*conversion"

# Verbose output
make test-verbose