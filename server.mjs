import express from 'express';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

// ESM __dirname shim
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ==== ENV CHECK (warn if not set but don't fail) ==== //
const OPTIONAL_ENV_VARS = [
    'GCS_KEY_ID',
    'GCS_SECRET',
    'S3_KEY_ID',
    'S3_SECRET'
];

const missingVars = OPTIONAL_ENV_VARS.filter(k => !process.env[k]);
if (missingVars.length > 0) {
    console.warn('\nWARNING: Missing optional environment variables (cloud storage may not work):');
    missingVars.forEach(v => console.warn('  - ' + v));
    console.warn('These can be provided via request body or set as env vars.\n');
}


const app = express();
const port = process.env.PORT || 8080;

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use((req, res, next) => {
    // Add request ID for tracking
    req.id = Math.random().toString(36).substr(2, 9);
    console.log(`[${req.id}] ${req.method} ${req.path}`);
    next();
});

// Set timeouts for Cloud Run
const CLOUD_RUN_TIMEOUT = 850; // seconds (Cloud Run max is 900, leave some buffer)

// CLI param mapping
function buildArgs(params) {
    const args = [];
    if (!params.input_path) throw new Error("input_path required");
    args.push(params.input_path);
    if (params.max_parallel_jobs) args.push(String(params.max_parallel_jobs));
    if (params.format)        args.push('--format', params.format);
    if (params.single_file)   args.push('--single-file', params.single_file);
    if (params.cols)          args.push('--cols', params.cols);
    if (params.dedupe)        args.push('--dedupe');
    if (params.output)        args.push('--output', params.output);
    if (params.rows)          args.push('--rows', String(params.rows));
    if (params.sql)           args.push('--sql', params.sql);
    if (params.gcs_key)       args.push('--gcs-key', params.gcs_key);
    if (params.gcs_secret)    args.push('--gcs-secret', params.gcs_secret);
    if (params.s3_key)        args.push('--s3-key', params.s3_key);
    if (params.s3_secret)     args.push('--s3-secret', params.s3_secret);
    if (params.url)           args.push('--url', params.url);
    if (params.header) {
        const headers = Array.isArray(params.header) ? params.header : [params.header];
        headers.forEach(h => args.push('--header', h));
    }
    if (params.log)           args.push('--log');
    if (params.jq)            args.push('--jq', params.jq);
    if (params.preview)       args.push('--preview', String(params.preview));
    if (params.verbose)       args.push('--verbose');
    return args;
}

app.post('/run', async (req, res) => {
    const startTime = Date.now();
    let args;
    
    try {
        args = buildArgs(req.body);
        console.log(`[${req.id}] Running duck-shard with args:`, args.join(' '));
    } catch (e) {
        console.error(`[${req.id}] Invalid arguments:`, e.message);
        return res.status(400).json({ 
            error: e.message,
            request_id: req.id 
        });
    }

    const scriptPath = path.join(__dirname, 'duck-shard.sh');
    
    // Build environment variables (prefer request body over env vars)
    const envVars = { 
        ...process.env,
        ...(req.body.gcs_key && { GCS_KEY_ID: req.body.gcs_key }),
        ...(req.body.gcs_secret && { GCS_SECRET: req.body.gcs_secret }),
        ...(req.body.s3_key && { S3_KEY_ID: req.body.s3_key }),
        ...(req.body.s3_secret && { S3_SECRET: req.body.s3_secret })
    };

    // Set up timeout handling
    const timeoutHandle = setTimeout(() => {
        console.warn(`[${req.id}] Request timeout after ${CLOUD_RUN_TIMEOUT}s`);
        if (!res.headersSent) {
            res.status(408).json({
                status: 'timeout',
                error: `Request exceeded ${CLOUD_RUN_TIMEOUT}s timeout`,
                request_id: req.id,
                duration: Date.now() - startTime
            });
        }
        proc.kill('SIGTERM');
    }, CLOUD_RUN_TIMEOUT * 1000);

    const proc = spawn('bash', [scriptPath, ...args], { 
        env: envVars,
        cwd: __dirname
    });

    let logs = '';
    let errorLogs = '';
    
    proc.stdout.on('data', data => { 
        const output = data.toString();
        logs += output;
        // Stream large outputs to prevent memory issues
        if (logs.length > 1000000) { // 1MB
            console.log(`[${req.id}] Large output detected, truncating logs`);
            logs = logs.slice(-500000) + '\n... (truncated) ...\n';
        }
    });
    
    proc.stderr.on('data', data => { 
        const output = data.toString();
        errorLogs += output;
        console.error(`[${req.id}] STDERR:`, output);
    });

    proc.on('error', (error) => {
        clearTimeout(timeoutHandle);
        console.error(`[${req.id}] Process error:`, error);
        if (!res.headersSent) {
            res.status(500).json({
                status: 'error',
                error: `Process failed to start: ${error.message}`,
                request_id: req.id,
                duration: Date.now() - startTime
            });
        }
    });

    proc.on('close', (code, signal) => {
        clearTimeout(timeoutHandle);
        const duration = Date.now() - startTime;
        
        console.log(`[${req.id}] Process completed with code ${code}, signal ${signal}, duration ${duration}ms`);
        
        if (!res.headersSent) {
            const response = {
                status: code === 0 ? 'success' : 'error',
                code,
                signal,
                logs: logs || 'No output',
                error_logs: errorLogs || null,
                request_id: req.id,
                duration,
                timestamp: new Date().toISOString()
            };
            
            res.json(response);
        }
    });
});

// Health check endpoint for Cloud Run
app.get('/health', (_, res) => {
    res.status(200).json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        version: process.env.npm_package_version || '1.0.0'
    });
});

// Root endpoint
app.get('/', (_, res) => {
    res.json({
        service: 'Duck Shard API',
        status: 'running',
        version: process.env.npm_package_version || '1.0.0',
        endpoints: {
            'POST /run': 'Execute duck-shard with JSON parameters',
            'GET /health': 'Health check endpoint',
            'GET /': 'This endpoint'
        },
        example_request: {
            input_path: 'gs://bucket/data.parquet',
            format: 'ndjson',
            output: 'gs://bucket/output/',
            jq: '.user_id = (.user_id | tonumber)',
            preview: 10
        }
    });
});

// Graceful shutdown handling
process.on('SIGTERM', () => {
    console.log('SIGTERM received, shutting down gracefully');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('SIGINT received, shutting down gracefully');
    process.exit(0);
});

app.listen(port, '0.0.0.0', () => {
    console.log(`🦆 Duck Shard API running on port ${port}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Health check: http://localhost:${port}/health`);
});
