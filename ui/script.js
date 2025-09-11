// Duck Shard UI Application
class DuckShardUI {
	constructor() {
		this.websocket = null;
		this.currentJobId = null;
		this.initializeUI();
		this.setupEventListeners();
	}

	// WebSocket connection methods
	connectWebSocket(jobId) {
		try {
			const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
			const wsUrl = `${protocol}//${window.location.host}`;
			
			this.websocket = new WebSocket(wsUrl);
			this.currentJobId = jobId;
			
			this.websocket.onopen = () => {
				// Register this connection with the job
				this.websocket.send(JSON.stringify({
					type: 'register-job',
					jobId: jobId
				}));
			};
			
			this.websocket.onmessage = (event) => {
				try {
					const data = JSON.parse(event.data);
					this.handleWebSocketMessage(data);
				} catch (error) {
					console.error('Failed to parse WebSocket message:', error);
				}
			};
			
			this.websocket.onerror = (error) => {
				console.error('WebSocket error:', error);
			};
			
			this.websocket.onclose = () => {
				this.websocket = null;
				this.currentJobId = null;
			};
			
		} catch (error) {
			console.error('Failed to connect WebSocket:', error);
		}
	}
	
	disconnectWebSocket() {
		if (this.websocket) {
			this.websocket.close();
			this.websocket = null;
			this.currentJobId = null;
		}
	}
	
	handleWebSocketMessage(data) {
		if (data.jobId !== this.currentJobId) {
			return; // Ignore messages for other jobs
		}
		
		switch (data.type) {
			case 'job-registered':
				break;
				
			case 'progress':
				this.updateProgressDisplay(data.data);
				break;
				
			case 'job-complete':
				this.hideLoading();
				this.showResults(data.result, false);
				this.disconnectWebSocket();
				break;
				
			case 'job-error':
				console.error('Job failed:', data.error);
				this.hideLoading();
				this.showError(`Processing failed: ${data.error}`);
				this.disconnectWebSocket();
				break;
				
			default:
				// Unknown message type - ignore silently
		}
	}
	
	updateProgressDisplay(progressData) {
		// Update the loading message with real-time progress
		const loadingDetails = document.querySelector('.loading-details');
		if (loadingDetails && progressData) {
			const { processed, requests, throughput, memory } = progressData;
			
			const formatNumber = (num) => {
				if (typeof num === 'number') {
					return num.toLocaleString();
				}
				return num || '0';
			};
			
			const formatBytes = (bytes) => {
				if (!bytes || bytes === 0) return '0 B';
				const k = 1024;
				const sizes = ['B', 'KB', 'MB', 'GB'];
				const i = Math.floor(Math.log(bytes) / Math.log(k));
				return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
			};
			
			loadingDetails.innerHTML = `
				<div class="progress-stats">
					${processed ? `
					<div class="stat-item">
						<span class="stat-label">Processed:</span>
						<span class="stat-value">${formatNumber(processed)}</span>
					</div>` : ''}
					${requests ? `
					<div class="stat-item">
						<span class="stat-label">Files:</span>
						<span class="stat-value">${formatNumber(requests)}</span>
					</div>` : ''}
					${throughput ? `
					<div class="stat-item">
						<span class="stat-label">Rows/sec:</span>
						<span class="stat-value">${throughput}</span>
					</div>` : ''}
					${memory ? `
					<div class="stat-item">
						<span class="stat-label">Memory:</span>
						<span class="stat-value">${formatBytes(memory)}</span>
					</div>` : ''}
				</div>
			`;
		}
	}

	initializeUI() {
		// Set default values
		this.updateCLICommand();
	}

	setupEventListeners() {
		// Input source toggle
		const inputSourceRadios = document.querySelectorAll('input[name="inputSource"]');
		inputSourceRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleInputSource.bind(this));
		});

		// Output mode toggle
		const outputModeRadios = document.querySelectorAll('input[name="outputMode"]');
		outputModeRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleOutputMode.bind(this));
		});

		// Form submission
		const form = document.getElementById('duckShardForm');
		form.addEventListener('submit', (e) => {
			e.preventDefault();
			this.submitJob(false);
		});

		// Test run button
		const testRunBtn = document.getElementById('test-run-btn');
		testRunBtn.addEventListener('click', () => {
			this.submitJob(true);
		});

		// Preview data button
		const previewBtn = document.getElementById('preview-data-btn');
		previewBtn.addEventListener('click', () => {
			this.previewData();
		});

		// CLI command copy button
		const copyCliBtn = document.getElementById('copy-cli');
		copyCliBtn.addEventListener('click', this.copyCLICommand.bind(this));

		// Add header button for API mode
		const addHeaderBtn = document.getElementById('add-header-btn');
		addHeaderBtn.addEventListener('click', this.addHeaderRow.bind(this));

		// Update CLI command when form changes
		form.addEventListener('input', this.updateCLICommand.bind(this));
		form.addEventListener('change', this.updateCLICommand.bind(this));

		// Single file checkbox logic
		const singleFileCheckbox = document.getElementById('singleFile');
		const rowsPerFileInput = document.getElementById('rowsPerFile');
		singleFileCheckbox.addEventListener('change', () => {
			rowsPerFileInput.disabled = singleFileCheckbox.checked;
			if (singleFileCheckbox.checked) {
				rowsPerFileInput.value = '';
			}
		});

		// Clean up WebSocket connection when page is unloaded
		window.addEventListener('beforeunload', () => {
			this.disconnectWebSocket();
		});

		// Dev key button for quick form filling
		const devKeyBtn = document.getElementById('dev-key-btn');
		if (devKeyBtn) {
			devKeyBtn.addEventListener('click', this.fillDevValues.bind(this));
		}

		// Initialize first header row event listeners
		this.setupHeaderRowEvents();
	}

	fillDevValues() {
		// Fill with Google Cloud Storage example values for quick testing
		
		// Select GCS input source
		document.querySelector('input[name="inputSource"][value="gcs"]').checked = true;
		this.toggleInputSource();
		
		// Set GCS path to public dataset
		document.getElementById('gcsPath').value = 'gs://mixpanel-import-public-data/example-dnd-events.json';
		document.getElementById('outputFormat').value = 'ndjson';
		document.getElementById('outputPath').value = './output/';
		
		// Select API mode for demo
		document.querySelector('input[name="outputMode"][value="api"]').checked = true;
		this.toggleOutputMode();
		
		document.getElementById('apiUrl').value = 'https://httpbin.org/post';
		document.getElementById('batchSize').value = '500';
		
		// Add an example header
		const headerRows = document.querySelectorAll('.header-row');
		if (headerRows.length > 0) {
			headerRows[0].querySelector('.header-name').value = 'Authorization';
			headerRows[0].querySelector('.header-value').value = 'Bearer test-token';
		}

		this.updateCLICommand();
		console.log('Dev values filled with GCS public dataset');
	}

	toggleInputSource() {
		const inputSource = document.querySelector('input[name="inputSource"]:checked').value;
		const localInput = document.getElementById('local-input');
		const gcsInput = document.getElementById('gcs-input');
		const s3Input = document.getElementById('s3-input');
		const cloudCredentialsSection = document.getElementById('cloud-credentials-section');
		const gcsCredentials = document.getElementById('gcs-credentials');
		const s3Credentials = document.getElementById('s3-credentials');

		// Hide all input sections first
		localInput.style.display = 'none';
		gcsInput.style.display = 'none';
		s3Input.style.display = 'none';
		cloudCredentialsSection.style.display = 'none';
		gcsCredentials.style.display = 'none';
		s3Credentials.style.display = 'none';

		// Show the selected input section
		if (inputSource === 'local') {
			localInput.style.display = 'block';
		} else if (inputSource === 'gcs') {
			gcsInput.style.display = 'block';
			cloudCredentialsSection.style.display = 'block';
			gcsCredentials.style.display = 'block';
		} else if (inputSource === 's3') {
			s3Input.style.display = 'block';
			cloudCredentialsSection.style.display = 'block';
			s3Credentials.style.display = 'block';
		}

		this.updateCLICommand();
	}

	toggleOutputMode() {
		const outputMode = document.querySelector('input[name="outputMode"]:checked').value;
		const fileOutput = document.getElementById('file-output');
		const apiOutput = document.getElementById('api-output');
		const previewOutput = document.getElementById('preview-output');

		// Hide all output sections first
		fileOutput.style.display = 'none';
		apiOutput.style.display = 'none';
		previewOutput.style.display = 'none';

		// Show the selected output section
		if (outputMode === 'file') {
			fileOutput.style.display = 'block';
		} else if (outputMode === 'api') {
			apiOutput.style.display = 'block';
		} else if (outputMode === 'preview') {
			previewOutput.style.display = 'block';
		}

		this.updateCLICommand();
	}

	addHeaderRow() {
		const container = document.getElementById('headers-container');
		const headerRow = document.createElement('div');
		headerRow.className = 'header-row';
		headerRow.innerHTML = `
			<input type="text" placeholder="Header name" class="header-name">
			<input type="text" placeholder="Header value" class="header-value">
			<button type="button" class="remove-header-btn">✕</button>
		`;
		container.appendChild(headerRow);
		this.setupHeaderRowEvents();
	}

	setupHeaderRowEvents() {
		// Add event listeners to all remove buttons
		document.querySelectorAll('.remove-header-btn').forEach(btn => {
			btn.replaceWith(btn.cloneNode(true)); // Remove existing listeners
		});
		
		document.querySelectorAll('.remove-header-btn').forEach(btn => {
			btn.addEventListener('click', (e) => {
				const headerRow = e.target.closest('.header-row');
				if (document.querySelectorAll('.header-row').length > 1) {
					headerRow.remove();
				} else {
					// Clear the last row instead of removing it
					headerRow.querySelector('.header-name').value = '';
					headerRow.querySelector('.header-value').value = '';
				}
				this.updateCLICommand();
			});
		});

		// Add change listeners to header inputs
		document.querySelectorAll('.header-name, .header-value').forEach(input => {
			input.addEventListener('input', this.updateCLICommand.bind(this));
		});
	}

	async previewData() {
		try {
			// Validation
			const inputSource = document.querySelector('input[name="inputSource"]:checked').value;
			const inputPath = this.getInputPath();

			if (!inputPath) {
				this.showError('Please specify an input path to preview.');
				return;
			}

			// Show loading state
			const previewBtn = document.getElementById('preview-data-btn');
			const originalText = previewBtn.innerHTML;
			previewBtn.innerHTML = '<span class="btn-icon">⏳</span> Loading...';
			previewBtn.disabled = true;

			// Collect form data for preview
			const formData = this.collectFormData();
			formData.preview = '10'; // Always preview 10 rows

			// Call the run endpoint with preview mode
			const response = await fetch('/run', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(formData)
			});

			const result = await response.json();

			if (result.status !== 'success') {
				throw new Error(result.error || result.error_logs || 'Preview failed');
			}

			// Extract data from stdout
			const logs = result.logs || '';
			this.displayPreviewData(logs);

			// Reset button
			previewBtn.innerHTML = originalText;
			previewBtn.disabled = false;

		} catch (error) {
			console.error('Preview error:', error);
			this.showError('Preview failed: ' + error.message);

			// Reset button
			const previewBtn = document.getElementById('preview-data-btn');
			previewBtn.innerHTML = '<span class="btn-icon">👁️</span> Preview Data';
			previewBtn.disabled = false;
		}
	}

	displayPreviewData(logs) {
		const previewSection = document.getElementById('data-preview');
		const previewContent = document.getElementById('preview-content');

		// Try to extract JSON data from logs
		let previewData = 'No preview data available';
		
		// Look for JSON lines in the output
		const lines = logs.split('\n');
		const jsonLines = lines.filter(line => {
			try {
				JSON.parse(line.trim());
				return true;
			} catch {
				return false;
			}
		});

		if (jsonLines.length > 0) {
			// Show the JSON data nicely formatted
			const jsonData = jsonLines.slice(0, 5).map(line => JSON.parse(line.trim()));
			previewData = JSON.stringify(jsonData, null, 2);
		} else if (logs.trim()) {
			// Fallback to showing raw logs
			previewData = logs;
		}

		previewSection.style.display = 'block';
		previewContent.innerHTML = `<pre><code>${this.escapeHtml(previewData)}</code></pre>`;
	}

	async submitJob(isTest = false) {
		try {
			// Validation
			const inputPath = this.getInputPath();
			if (!inputPath) {
				this.showError('Please specify an input path.');
				return;
			}

			const outputMode = document.querySelector('input[name="outputMode"]:checked').value;
			if (outputMode === 'file') {
				const outputPath = document.getElementById('outputPath').value.trim();
				if (!outputPath) {
					this.showError('Please specify an output path for file mode.');
					return;
				}
			} else if (outputMode === 'api') {
				const apiUrl = document.getElementById('apiUrl').value.trim();
				if (!apiUrl) {
					this.showError('Please specify an API URL for streaming mode.');
					return;
				}
			}

			// Clear any previous results
			this.clearResults();

			// Show loading
			this.showLoading(
				isTest ? 'Running Test...' : 'Processing Data...',
				isTest ? 'Processing sample data to test configuration' : 'Processing your data with duck-shard'
			);

			// Collect form data
			const formData = this.collectFormData();
			if (isTest) {
				formData.preview = '10'; // Test with 10 rows
			}

			// Submit to run endpoint
			const response = await fetch('/run', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(formData)
			});

			const result = await response.json();

			if (result.status === 'success') {
				this.hideLoading();
				this.showResults(result, isTest);
			} else {
				this.hideLoading();
				this.showError(`${isTest ? 'Test' : 'Processing'} failed: ${result.error || result.error_logs}`);
			}

		} catch (error) {
			this.hideLoading();
			this.showError(`Network error: ${error.message}`);
		}
	}

	collectFormData() {
		const inputSource = document.querySelector('input[name="inputSource"]:checked').value;
		const outputMode = document.querySelector('input[name="outputMode"]:checked').value;

		const formData = {
			input_path: this.getInputPath()
		};

		// Add max parallel jobs if specified
		const maxParallelJobs = document.getElementById('maxParallelJobs').value;
		if (maxParallelJobs) {
			formData.max_parallel_jobs = parseInt(maxParallelJobs);
		}

		// Output configuration
		if (outputMode === 'file') {
			formData.format = document.getElementById('outputFormat').value;
			formData.output = document.getElementById('outputPath').value;
			
			const rowsPerFile = document.getElementById('rowsPerFile').value;
			if (rowsPerFile && !document.getElementById('singleFile').checked) {
				formData.rows = parseInt(rowsPerFile);
			}
			
			if (document.getElementById('singleFile').checked) {
				formData.single_file = true;
			}
		} else if (outputMode === 'api') {
			formData.url = document.getElementById('apiUrl').value;
			formData.format = 'ndjson'; // API streaming uses NDJSON
			
			const batchSize = document.getElementById('batchSize').value;
			if (batchSize) {
				formData.rows = parseInt(batchSize);
			}
			
			if (document.getElementById('logResponses').checked) {
				formData.log = true;
			}
			
			// Collect headers
			const headers = this.collectHeaders();
			if (headers.length > 0) {
				formData.header = headers;
			}
		}

		// Cloud credentials
		if (inputSource === 'gcs') {
			const gcsKey = document.getElementById('gcsKey').value;
			const gcsSecret = document.getElementById('gcsSecret').value;
			if (gcsKey) formData.gcs_key = gcsKey;
			if (gcsSecret) formData.gcs_secret = gcsSecret;
		} else if (inputSource === 's3') {
			const s3Key = document.getElementById('s3Key').value;
			const s3Secret = document.getElementById('s3Secret').value;
			if (s3Key) formData.s3_key = s3Key;
			if (s3Secret) formData.s3_secret = s3Secret;
		}

		// Processing options
		const sqlFile = document.getElementById('sqlFile').value;
		if (sqlFile) formData.sql = sqlFile;

		const jqExpression = document.getElementById('jqExpression').value;
		if (jqExpression) formData.jq = jqExpression;

		const selectColumns = document.getElementById('selectColumns').value;
		if (selectColumns) formData.cols = selectColumns;

		if (document.getElementById('dedupe').checked) {
			formData.dedupe = true;
		}

		if (document.getElementById('verbose').checked) {
			formData.verbose = true;
		}

		// File organization
		const prefix = document.getElementById('prefix').value;
		if (prefix) formData.prefix = prefix;

		const suffix = document.getElementById('suffix').value;
		if (suffix) formData.suffix = suffix;

		return formData;
	}

	getInputPath() {
		const inputSource = document.querySelector('input[name="inputSource"]:checked').value;
		
		if (inputSource === 'local') {
			return document.getElementById('localPath').value.trim();
		} else if (inputSource === 'gcs') {
			return document.getElementById('gcsPath').value.trim();
		} else if (inputSource === 's3') {
			return document.getElementById('s3Path').value.trim();
		}
		
		return '';
	}

	collectHeaders() {
		const headers = [];
		document.querySelectorAll('.header-row').forEach(row => {
			const name = row.querySelector('.header-name').value.trim();
			const value = row.querySelector('.header-value').value.trim();
			if (name && value) {
				headers.push(`${name}: ${value}`);
			}
		});
		return headers;
	}

	updateCLICommand() {
		const cliElement = document.getElementById('cli-command');
		
		try {
			const inputPath = this.getInputPath();
			if (!inputPath) {
				cliElement.textContent = 'Configure your options above to generate CLI command...';
				cliElement.classList.add('empty');
				return;
			}

			let command = `./duck-shard.sh "${inputPath}"`;

			// Add max parallel jobs
			const maxParallelJobs = document.getElementById('maxParallelJobs').value;
			if (maxParallelJobs) {
				command += ` ${maxParallelJobs}`;
			}

			const outputMode = document.querySelector('input[name="outputMode"]:checked').value;

			if (outputMode === 'file') {
				const format = document.getElementById('outputFormat').value;
				command += ` --format ${format}`;

				const outputPath = document.getElementById('outputPath').value.trim();
				if (outputPath) {
					command += ` --output "${outputPath}"`;
				}

				const rowsPerFile = document.getElementById('rowsPerFile').value;
				if (rowsPerFile && !document.getElementById('singleFile').checked) {
					command += ` --rows ${rowsPerFile}`;
				}

				if (document.getElementById('singleFile').checked) {
					command += ` --single-file`;
				}
			} else if (outputMode === 'api') {
				const apiUrl = document.getElementById('apiUrl').value.trim();
				if (apiUrl) {
					command += ` --url "${apiUrl}"`;
				}

				const batchSize = document.getElementById('batchSize').value;
				if (batchSize) {
					command += ` --rows ${batchSize}`;
				}

				if (document.getElementById('logResponses').checked) {
					command += ' --log';
				}

				// Add headers
				const headers = this.collectHeaders();
				headers.forEach(header => {
					command += ` --header "${header}"`;
				});
			} else if (outputMode === 'preview') {
				const previewRows = document.getElementById('previewRows').value;
				command += ` --preview ${previewRows}`;
			}

			// Cloud credentials
			const inputSource = document.querySelector('input[name="inputSource"]:checked').value;
			if (inputSource === 'gcs') {
				const gcsKey = document.getElementById('gcsKey').value;
				const gcsSecret = document.getElementById('gcsSecret').value;
				if (gcsKey) command += ` --gcs-key "${gcsKey}"`;
				if (gcsSecret) command += ` --gcs-secret "[HIDDEN]"`;
			} else if (inputSource === 's3') {
				const s3Key = document.getElementById('s3Key').value;
				const s3Secret = document.getElementById('s3Secret').value;
				if (s3Key) command += ` --s3-key "${s3Key}"`;
				if (s3Secret) command += ` --s3-secret "[HIDDEN]"`;
			}

			// Processing options
			const sqlFile = document.getElementById('sqlFile').value;
			if (sqlFile) command += ` --sql "${sqlFile}"`;

			const jqExpression = document.getElementById('jqExpression').value;
			if (jqExpression) command += ` --jq '${jqExpression}'`;

			const selectColumns = document.getElementById('selectColumns').value;
			if (selectColumns) command += ` --cols '${selectColumns}'`;

			if (document.getElementById('dedupe').checked) {
				command += ' --dedupe';
			}

			if (document.getElementById('verbose').checked) {
				command += ' --verbose';
			}

			// File organization
			const prefix = document.getElementById('prefix').value;
			if (prefix) command += ` --prefix "${prefix}"`;

			const suffix = document.getElementById('suffix').value;
			if (suffix) command += ` --suffix "${suffix}"`;

			cliElement.textContent = command;
			cliElement.classList.remove('empty');

		} catch (error) {
			console.warn('Error generating CLI command:', error);
			cliElement.textContent = 'Error generating CLI command...';
			cliElement.classList.add('empty');
		}
	}

	copyCLICommand() {
		const cliElement = document.getElementById('cli-command');
		const command = cliElement.textContent;

		if (command.includes('Configure') || command.includes('Error')) {
			this.showError('Please configure your processing options first.');
			return;
		}

		// Copy to clipboard
		navigator.clipboard.writeText(command).then(() => {
			// Show success feedback
			const copyBtn = document.getElementById('copy-cli');
			const originalText = copyBtn.innerHTML;
			copyBtn.innerHTML = '<span class="btn-icon">✓</span> Copied!';
			copyBtn.style.background = 'var(--success)';

			setTimeout(() => {
				copyBtn.innerHTML = originalText;
				copyBtn.style.background = '';
			}, 2000);
		}).catch(_err => {
			this.showError('Could not copy to clipboard. Please select and copy manually.');
		});
	}

	showLoading(title, message) {
		document.getElementById('loading-title').textContent = title;
		document.getElementById('loading-message').textContent = message;
		document.getElementById('loading').style.display = 'flex';
	}

	hideLoading() {
		document.getElementById('loading').style.display = 'none';
	}

	clearResults() {
		const resultsSection = document.getElementById('results');
		const resultsData = document.getElementById('results-data');
		
		if (resultsSection) {
			resultsSection.style.display = 'none';
		}
		if (resultsData) {
			resultsData.innerHTML = '';
		}
	}

	showResults(result, isTest) {
		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = isTest ? 'Test Results' : 'Processing Complete!';

		// Display the logs/output
		const output = result.logs || result.error_logs || 'No output available';
		resultsData.innerHTML = `<pre><code>${this.escapeHtml(output)}</code></pre>`;
		
		resultsSection.style.display = 'block';
		resultsSection.scrollIntoView({ behavior: 'smooth' });
	}

	showError(message) {
		// Remove existing error messages
		const existingErrors = document.querySelectorAll('.error');
		existingErrors.forEach(error => error.remove());

		// Create new error message
		const errorDiv = document.createElement('div');
		errorDiv.className = 'error';
		errorDiv.textContent = message;

		// Insert after header
		const header = document.querySelector('.header');
		header.parentNode.insertBefore(errorDiv, header.nextSibling);

		// Scroll to error
		errorDiv.scrollIntoView({ behavior: 'smooth' });

		// Auto-remove after 5 seconds
		setTimeout(() => {
			if (errorDiv.parentNode) {
				errorDiv.remove();
			}
		}, 5000);
	}

	escapeHtml(text) {
		const div = document.createElement('div');
		div.textContent = text;
		return div.innerHTML;
	}
}

// Global function for collapsible sections
function toggleSection(sectionId) {
	const section = document.getElementById(sectionId);
	const header = section?.previousElementSibling || section?.parentElement?.querySelector('.section-header');
	const toggleIcon = header?.querySelector('.toggle-icon');
	
	if (!section) return;
	
	const isVisible = section.style.display !== 'none';
	section.style.display = isVisible ? 'none' : 'block';
	
	if (toggleIcon) {
		toggleIcon.textContent = isVisible ? '▼' : '▲';
	}
	
	if (header) {
		header.setAttribute('aria-expanded', !isVisible);
	}
}

// Initialize the application
let app;
document.addEventListener('DOMContentLoaded', () => {
	app = new DuckShardUI();
	// Make app globally available
	window.app = app;
});