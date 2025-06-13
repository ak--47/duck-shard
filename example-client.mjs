#!/usr/bin/env node
/**
 * Example client for Duck Shard API
 * Usage: node example-client.mjs [API_URL]
 */

const API_URL = process.argv[2] || 'http://localhost:8080';

async function testAPI() {
    console.log('🦆 Duck Shard API Client Test');
    console.log(`Testing API at: ${API_URL}`);
    console.log('='*50);

    try {
        // Test health check
        console.log('\n1. Testing health check...');
        const healthResponse = await fetch(`${API_URL}/health`);
        const health = await healthResponse.json();
        console.log('✅ Health check:', health);

        // Test API info
        console.log('\n2. Testing API info...');
        const infoResponse = await fetch(`${API_URL}/`);
        const info = await infoResponse.json();
        console.log('✅ API info:', info);

        // Test preview with local data (replace with your actual data)
        console.log('\n3. Testing preview mode...');
        const previewRequest = {
            input_path: 'tests/testData/csv/part-1.csv', // Replace with your data
            format: 'ndjson',
            preview: 3,
            jq: '.user_id'
        };

        console.log('Request:', JSON.stringify(previewRequest, null, 2));
        
        const previewResponse = await fetch(`${API_URL}/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(previewRequest)
        });

        const previewResult = await previewResponse.json();
        console.log('✅ Preview result:', previewResult);

        // Test with cloud storage (comment out if you don't have cloud storage set up)
        /*
        console.log('\n4. Testing cloud storage...');
        const cloudRequest = {
            input_path: 'gs://your-bucket/data.parquet',
            format: 'ndjson',
            output: 'gs://your-bucket/output/',
            jq: 'select(.event == "purchase") | {user: .user_id, amount: .revenue}',
            gcs_key: process.env.GCS_KEY_ID,
            gcs_secret: process.env.GCS_SECRET
        };

        const cloudResponse = await fetch(`${API_URL}/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cloudRequest)
        });

        const cloudResult = await cloudResponse.json();
        console.log('✅ Cloud storage result:', cloudResult);
        */

    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    }
}

// Check if fetch is available (Node.js 18+)
if (typeof fetch === 'undefined') {
    console.error('❌ This script requires Node.js 18+ with built-in fetch support');
    console.error('Alternatively, install node-fetch: npm install node-fetch');
    process.exit(1);
}

testAPI().then(() => {
    console.log('\n✅ All tests completed successfully!');
}).catch(error => {
    console.error('\n❌ Test failed:', error);
    process.exit(1);
});