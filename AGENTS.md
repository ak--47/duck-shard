# CLAUDE.md - Development Guidelines

This file contains information for Claude Code when working on this project.

## Project Overview

**duck-shard** is a high-performance ETL tool that combines DuckDB, jq, and curl for fast data processing. It's a bash script that can convert between formats (Parquet, CSV, TSV, NDJSON, XML, all with gzip/compression support), apply SQL transformations, stream results to APIs, and run analytical queries with `--count` and custom SQL.

## Key Commands for Development

### Testing
```bash
# Run basic functionality tests
bats tests/test.bats

# Test with sample data
./duck-shard.sh tests/testData/parquet -s all.csv -f csv
./duck-shard.sh tests/testData/tsv -s all.tsv -f tsv

# Test compressed file support
./duck-shard.sh data.csv.gz --preview 5
./duck-shard.sh data.tsv.gz -f parquet -o ./out/

# Test analytical mode and count
./duck-shard.sh tests/testData/parquet/part-1.parquet --count
./duck-shard.sh tests/testData/ndjson --count --fast-mode

# Test compressed output
./duck-shard.sh tests/testData/csv -f parquet --compressed -o ./out/

# Test fast mode for NDJSON splitting
./duck-shard.sh tests/testData/ndjson/part-1.ndjson --fast-mode -f ndjson --rows 1000 -o ./out/

# Start UI for testing
npm run dev  # or npx duck-shard --ui
```

### Building & Deployment

```bash
# Test locally
make check-deps

# Lint and typecheck (run these before committing)
npm run lint      # If applicable
npm run typecheck # If applicable

# Test npm package locally
npm run cli --help
```

### npm Distribution

The project is distributed via npm for easy installation:

Users install via: `npx duck-shard` or `npm install -g duck-shard`

No complex build processes or dependency management needed - npm handles everything.

## Architecture

- **Core**: `duck-shard.sh` - Main bash script with all functionality
- **Web UI**: `server.mjs` + `ui/` directory - Node.js server with WebSocket support
- **Testing**: `tests/` directory with Bats tests
- **Cloud Support**: Built-in GCS and S3 integration via DuckDB

## Dependencies

- **Required**: DuckDB (for data processing)
- **Optional**: Node.js (for `--ui` web interface)
- **Optional**: jq (for `--jq` JSON transformations)

## Key Features to Preserve

1. **Performance**: Parallel processing, streaming architecture, fast mode for NDJSON (25-50% faster)
2. **Simplicity**: Single bash script, no complex dependencies
3. **Flexibility**: Supports multiple formats, cloud storage, API streaming, compressed output
4. **UI**: Web interface with real-time progress via WebSockets
5. **Analytical Mode**: Two modes - ETL (with --format) and Analytical (no --format, displays + saves CSV)
   - `--count` shortcut for quick row counting
   - Fast mode works with analytical queries for ultra-fast JSONL counting

## Common Tasks

### Adding New Features
- Modify `duck-shard.sh`
- Update help text and examples
- Add tests in `tests/test.bats`
- Update README.md examples

### Releasing
- Update version in package.json: `npm version patch|minor|major`
- Push: `git push origin main && git push --tags`
- Publish to npm: `npm publish`

## Testing Data
- Sample files in `tests/testData/` (parquet, csv, tsv, ndjson, xml)
- TSV test data available in `tests/testData/tsv/`
- Use `--preview` mode for quick testing
- Test both single files and directories

## Important Notes
- Always test with `--verbose` flag during development
- The script must remain cross-platform (macOS/Linux)
- Maintain backward compatibility for existing command-line options
- Web UI requires npm dependencies but core script should work standalone