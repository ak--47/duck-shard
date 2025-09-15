import chalk from 'chalk';

/**
 * Display a beautiful CLI banner with ASCII art and signature
 */
export function showBanner() {
    const banner = `
${chalk.cyan('╔══════════════════════════════════════════════════════════════════╗')}
${chalk.cyan('║')}                                                                  ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.yellow.bold('🦆 DUCK SHARD')} ${chalk.gray('- Universal Data Pipeline')}                    ${chalk.cyan('║')}
${chalk.cyan('║')}                                                                  ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.white('DuckDB')} ${chalk.gray('+')} ${chalk.white('jq')} ${chalk.gray('+')} ${chalk.white('curl')} ${chalk.gray('= Insane performance on modest hardware')}     ${chalk.cyan('║')}
${chalk.cyan('║')}                                                                  ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.green('•')} Convert between formats ${chalk.gray('(Parquet, CSV, NDJSON)')}            ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.green('•')} Apply SQL transformations ${chalk.gray('(with DuckDB power)')}              ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.green('•')} Stream to APIs ${chalk.gray('(parallel processing)')}                      ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.green('•')} Web UI for visual processing                               ${chalk.cyan('║')}
${chalk.cyan('║')}                                                                  ${chalk.cyan('║')}
${chalk.cyan('║')}   ${chalk.magenta.bold('Built with ❤️  by AK')} ${chalk.gray('(https://github.com/ak--47)')}            ${chalk.cyan('║')}
${chalk.cyan('║')}                                                                  ${chalk.cyan('║')}
${chalk.cyan('╚══════════════════════════════════════════════════════════════════╝')}
`;

    console.log(banner);
}

/**
 * Display a minimal banner for server mode
 */
export function showMiniBanner() {
    console.log(chalk.cyan('🦆 ') + chalk.yellow.bold('Duck Shard') + chalk.gray(' - Universal Data Pipeline'));
    console.log(chalk.gray('Built with ❤️  by AK\n'));
}

/**
 * Display version and help information
 */
export function showVersion(version) {
    console.log(chalk.cyan('🦆 ') + chalk.yellow.bold('Duck Shard') + chalk.gray(` v${version}`));
    console.log(chalk.gray('Universal Data Pipeline - Built with ❤️  by AK'));
}