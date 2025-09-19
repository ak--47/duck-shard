#!/usr/bin/env node

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { checkDependenciesOrExit } from '../lib/check-dependencies.mjs';
import { showBanner, showVersion } from '../lib/banner.mjs';
import chalk from 'chalk';

// ESM __dirname shim
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function main() {
    const args = process.argv.slice(2);

    // Handle help and version flags
    if (args.includes('--help') || args.includes('-h')) {
        showHelp();
        return;
    }

    if (args.includes('--version') || args.includes('-v')) {
        const packagePath = join(__dirname, '..', 'package.json');
        const { default: pkg } = await import(packagePath, { with: { type: 'json' } });
        showVersion(pkg.version);
        return;
    }

    // Check if user wants to start the UI server
    if (args.includes('--ui') || args.includes('--server') || args.length === 0) {
        await startUIServer();
        return;
    }

    // For CLI usage, just run the duck-shard.sh script directly
    await runCLI(args);
}

async function startUIServer() {
    showBanner();

    console.log(chalk.blue('🔍 Checking system dependencies...\n'));

    // Check dependencies (will exit if required ones are missing)
    await checkDependenciesOrExit();

    console.log(chalk.green('✅ All dependencies found!'));
    console.log(chalk.blue('\n🚀 Starting web server...\n'));

    // Import and start the server
    const serverPath = join(__dirname, '..', 'server.mjs');
    await import(serverPath);
}

async function runCLI(args) {
    console.log(chalk.blue('🦆 Duck Shard CLI Mode'));

    // Check dependencies silently (will exit if required ones are missing)
    await checkDependenciesOrExit(true);

    // Import spawn and run the shell script
    const { spawn } = await import('child_process');
    const scriptPath = join(__dirname, '..', 'duck-shard.sh');

    const proc = spawn('bash', [scriptPath, ...args], {
        stdio: 'inherit',
        cwd: join(__dirname, '..')
    });

    proc.on('close', (code) => {
        process.exit(code);
    });

    proc.on('error', (error) => {
        console.error(chalk.red('Error running duck-shard:'), error.message);
        process.exit(1);
    });
}

function showHelp() {
    showBanner();

    console.log(chalk.white.bold('USAGE:'));
    console.log('  npx duck-shard [options]                 # Start web UI');
    console.log('  npx duck-shard --ui                      # Start web UI explicitly');
    console.log('  npx duck-shard <input> [cli-options]     # Run CLI mode');
    console.log('  npx duck-shard --help                    # Show this help');
    console.log('  npx duck-shard --version                 # Show version\n');

    console.log(chalk.white.bold('WEB UI MODE:'));
    console.log('  npx duck-shard');
    console.log('  npx duck-shard --ui');
    console.log('  npx duck-shard --server\n');

    console.log(chalk.white.bold('CLI MODE EXAMPLES:'));
    console.log('  npx duck-shard ./data.parquet --format csv --output ./out/');
    console.log('  npx duck-shard gs://bucket/data/ --format ndjson --sql ./transform.sql');
    console.log('  npx duck-shard ./data.json --jq ".user_id = (.user_id | tonumber)" --preview 10\n');

    console.log(chalk.white.bold('DEPENDENCIES:'));
    console.log('  • DuckDB (required) - Data processing engine');
    console.log('  • curl (required) - HTTP requests and cloud storage');
    console.log('  • jq (optional) - Advanced JSON transformations\n');

    console.log(chalk.gray('For full CLI documentation, see: https://github.com/ak--47/duck-shard'));
}

// Run the main function
main().catch((error) => {
    console.error(chalk.red('Error:'), error.message);
    process.exit(1);
});