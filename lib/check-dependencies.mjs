import { spawn } from 'child_process';
import chalk from 'chalk';

/**
 * Check if a command exists and optionally verify its version
 * @param {string} command - Command to check
 * @param {string[]} args - Arguments to pass for version check
 * @param {string} name - Human readable name for the dependency
 * @returns {Promise<{success: boolean, version?: string, error?: string}>}
 */
async function checkCommand(command, args = ['--version'], name = command) {
    return new Promise((resolve) => {
        const proc = spawn(command, args, { stdio: 'pipe' });

        let output = '';
        let error = '';

        proc.stdout.on('data', (data) => {
            output += data.toString();
        });

        proc.stderr.on('data', (data) => {
            error += data.toString();
        });

        proc.on('close', (code) => {
            if (code === 0) {
                // Extract version from output (first line, first version-like string)
                const versionMatch = (output || error).match(/\d+\.\d+(\.\d+)?/);
                const version = versionMatch ? versionMatch[0] : 'unknown';
                resolve({ success: true, version });
            } else {
                resolve({
                    success: false,
                    error: `Command '${command}' not found or failed to run`
                });
            }
        });

        proc.on('error', (err) => {
            resolve({
                success: false,
                error: `Command '${command}' not found: ${err.message}`
            });
        });

        // Timeout after 5 seconds
        setTimeout(() => {
            proc.kill();
            resolve({
                success: false,
                error: `Command '${command}' timed out`
            });
        }, 5000);
    });
}

/**
 * Check all required dependencies for duck-shard
 * @param {boolean} silent - If true, don't print status messages
 * @returns {Promise<{allPresent: boolean, results: Object}>}
 */
export async function checkDependencies(silent = false) {
    const dependencies = [
        { command: 'duckdb', args: ['--version'], name: 'DuckDB', required: true },
        { command: 'jq', args: ['--version'], name: 'jq', required: false },
        { command: 'curl', args: ['--version'], name: 'curl', required: true }
    ];

    if (!silent) {
        console.log(chalk.blue('🔍 Checking system dependencies...'));
    }

    const results = {};
    let allRequired = true;

    for (const dep of dependencies) {
        if (!silent) {
            process.stdout.write(`  ${dep.name}: `);
        }

        const result = await checkCommand(dep.command, dep.args, dep.name);
        results[dep.command] = result;

        if (!silent) {
            if (result.success) {
                console.log(chalk.green(`✓ v${result.version}`));
            } else {
                const icon = dep.required ? '✗' : '⚠️';
                const color = dep.required ? chalk.red : chalk.yellow;
                console.log(color(`${icon} ${result.error}`));

                if (dep.required) {
                    allRequired = false;
                }
            }
        } else {
            if (!result.success && dep.required) {
                allRequired = false;
            }
        }
    }

    return {
        allPresent: allRequired,
        results
    };
}

/**
 * Print installation instructions for missing dependencies
 * @param {Object} results - Results from checkDependencies
 */
export function printInstallationInstructions(results) {
    const missing = [];
    const optional = [];

    if (!results.duckdb?.success) missing.push('duckdb');
    if (!results.curl?.success) missing.push('curl');
    if (!results.jq?.success) optional.push('jq');

    if (missing.length === 0 && optional.length === 0) {
        return;
    }

    console.log('\n' + chalk.yellow('📦 Installation Instructions:'));

    if (missing.length > 0) {
        console.log(chalk.red('\nRequired dependencies:'));
        missing.forEach(dep => {
            console.log(chalk.white(`  ${dep}:`));
            switch (dep) {
                case 'duckdb':
                    console.log('    macOS: brew install duckdb');
                    console.log('    Linux: wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip');
                    console.log('    Windows: winget install DuckDB.cli');
                    break;
                case 'curl':
                    console.log('    macOS: brew install curl (usually pre-installed)');
                    console.log('    Linux: apt-get install curl / yum install curl');
                    console.log('    Windows: Usually pre-installed with Windows 10+');
                    break;
            }
        });
    }

    if (optional.length > 0) {
        console.log(chalk.yellow('\nOptional dependencies (for advanced JSON processing):'));
        optional.forEach(dep => {
            console.log(chalk.white(`  ${dep}:`));
            switch (dep) {
                case 'jq':
                    console.log('    macOS: brew install jq');
                    console.log('    Linux: apt-get install jq / yum install jq');
                    console.log('    Windows: winget install jqlang.jq');
                    break;
            }
        });
    }

    console.log(chalk.blue('\n💡 After installing dependencies, run the command again.'));
}

/**
 * Check dependencies and exit if required ones are missing
 * @param {boolean} silent - If true, don't print status messages
 */
export async function checkDependenciesOrExit(silent = false) {
    const { allPresent, results } = await checkDependencies(silent);

    if (!allPresent) {
        if (!silent) {
            printInstallationInstructions(results);
            console.log(chalk.red('\n❌ Missing required dependencies. Please install them and try again.'));
        }
        process.exit(1);
    }

    return results;
}