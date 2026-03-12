/**
 * ingestor_api_client.js
 * ======================
 * JavaScript client for IntelliCredit Ingestor API
 * 
 * Usage in home2.html:
 * 1. Include this file in <script> tag
 * 2. Call IngestorAPI.uploadDocuments() when user clicks "Launch Analysis"
 * 3. Update UI with returned document data
 */

const IngestorAPI = {
    // API base URL - change this to your server address
    baseURL: 'http://localhost:8000/api/ingest',
    
    /**
     * Upload multiple documents and get back processed results
     * 
     * @param {FileList} files - Files from <input type="file" multiple>
     * @param {string} caseId - Case ID (from Entity Identification form)
     * @param {string} userEmail - Optional user email
     * @returns {Promise<Object>} Batch processing result
     */
    async uploadDocuments(files, caseId, userEmail = null) {
        try {
            console.log(`[API] Uploading ${files.length} documents for case ${caseId}...`);
            
            // Create FormData for multipart upload
            const formData = new FormData();
            formData.append('case_id', caseId);
            if (userEmail) {
                formData.append('user_email', userEmail);
            }
            
            // Add all files
            for (let i = 0; i < files.length; i++) {
                formData.append('files', files[i]);
            }
            
            // POST to batch endpoint
            const response = await fetch(`${this.baseURL}/batch`, {
                method: 'POST',
                body: formData,
                // Don't set Content-Type - browser will set it with boundary
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Upload failed');
            }
            
            const result = await response.json();
            console.log(`[API] ✓ Batch processed: ${result.total_documents} documents`);
            console.log(`[API]   - Ready: ${result.ready}`);
            console.log(`[API]   - Pending: ${result.pending}`);
            console.log(`[API]   - Low Confidence: ${result.low_confidence}`);
            
            return result;
            
        } catch (error) {
            console.error('[API] Upload failed:', error);
            throw error;
        }
    },
    
    /**
     * Upload a single document
     * 
     * @param {File} file - Single file
     * @param {string} caseId - Case ID
     * @param {string} userEmail - Optional user email
     * @returns {Promise<Object>} Document processing result
     */
    async uploadSingleDocument(file, caseId, userEmail = null) {
        try {
            console.log(`[API] Uploading ${file.name}...`);
            
            const formData = new FormData();
            formData.append('file', file);
            formData.append('case_id', caseId);
            if (userEmail) {
                formData.append('user_email', userEmail);
            }
            
            const response = await fetch(`${this.baseURL}/upload`, {
                method: 'POST',
                body: formData,
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Upload failed');
            }
            
            const result = await response.json();
            console.log(`[API] ✓ Document processed: ${result.classified_type} (${(result.confidence * 100).toFixed(0)}%)`);
            
            return result;
            
        } catch (error) {
            console.error('[API] Single upload failed:', error);
            throw error;
        }
    },
    
    /**
     * Get processing status for a case
     * 
     * @param {string} caseId - Case ID
     * @returns {Promise<Object>} Status with all documents
     */
    async getCaseStatus(caseId) {
        try {
            const response = await fetch(`${this.baseURL}/status/${caseId}`);
            
            if (!response.ok) {
                throw new Error('Failed to get case status');
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('[API] Get status failed:', error);
            throw error;
        }
    },
    
    /**
     * Approve or deny a document classification
     * 
     * @param {string} documentId - Document ID
     * @param {string} action - "approve" or "deny"
     * @param {string} correctedType - If denied, the corrected document type
     * @param {string} userEmail - User email
     * @returns {Promise<Object>} Validation result
     */
    async validateDocument(documentId, action, correctedType = null, userEmail = null) {
        try {
            const body = {
                document_id: documentId,
                action: action,
                corrected_type: correctedType,
                user_email: userEmail,
            };
            
            const response = await fetch(`${this.baseURL}/validate`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(body),
            });
            
            if (!response.ok) {
                throw new Error('Validation failed');
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('[API] Validation failed:', error);
            throw error;
        }
    },
    
    /**
     * Edit schema fields for a document
     * 
     * @param {string} documentId - Document ID
     * @param {Object} fieldEdits - Key-value pairs of field edits
     * @param {string} userEmail - User email
     * @returns {Promise<Object>} Edit result
     */
    async editSchema(documentId, fieldEdits, userEmail = null) {
        try {
            const body = {
                document_id: documentId,
                field_edits: fieldEdits,
                user_email: userEmail,
            };
            
            const response = await fetch(`${this.baseURL}/schema/edit`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(body),
            });
            
            if (!response.ok) {
                throw new Error('Schema edit failed');
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('[API] Schema edit failed:', error);
            throw error;
        }
    },
    
    /**
     * Get key findings for a case
     * 
     * @param {string} caseId - Case ID
     * @param {number} limit - Max findings to return
     * @returns {Promise<Object>} Findings data
     */
    async getFindings(caseId, limit = 25) {
        try {
            const response = await fetch(`${this.baseURL}/findings/${caseId}?limit=${limit}`);
            
            if (!response.ok) {
                throw new Error('Failed to get findings');
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('[API] Get findings failed:', error);
            throw error;
        }
    },
};

/**
 * UI Helper Functions for Ingestor Page
 */
const IngestorUI = {
    /**
     * Render document table from API response
     * 
     * @param {Array} documents - Array of DocumentResponse objects
     * @param {string} containerId - ID of container element
     */
    renderDocumentTable(documents, containerId = 'ingestorDocTable') {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container #${containerId} not found`);
            return;
        }
        
        // Clear existing content
        container.innerHTML = '';
        
        // Render each document row
        documents.forEach(doc => {
            const row = document.createElement('div');
            row.className = 'ingestor-doc-row';
            row.innerHTML = `
                <div class="doc-col doc-type">
                    <div class="doc-icon">${this.getFileTypeIcon(doc.filename)}</div>
                    <div class="doc-info">
                        <div class="doc-name">${doc.filename}</div>
                        <div class="doc-meta">${doc.pages} pages · ${doc.file_size_mb.toFixed(1)} MB</div>
                    </div>
                </div>
                
                <div class="doc-col doc-classified">
                    <div class="doc-type-badge" data-type="${doc.classified_type}">
                        ${this.getDocTypeLabel(doc.classified_type)}
                    </div>
                </div>
                
                <div class="doc-col doc-confidence">
                    <div class="confidence-bar">
                        <div class="confidence-fill" style="width: ${doc.confidence * 100}%"></div>
                    </div>
                    <div class="confidence-text">${(doc.confidence * 100).toFixed(0)}%</div>
                </div>
                
                <div class="doc-col doc-flags">
                    <span class="flag-badge ${doc.flags > 0 ? 'flag-active' : ''}">${doc.flags}</span>
                </div>
                
                <div class="doc-col doc-validation">
                    <button class="btn-validate ${doc.human_validation === 'APPROVE' ? 'approved' : 'pending'}"
                            data-doc-id="${doc.document_id}"
                            onclick="handleValidation('${doc.document_id}', 'approve')">
                        ${doc.human_validation === 'APPROVE' ? '✓ APPROVED' : '✓ APPROVE'}
                    </button>
                    ${doc.human_validation === 'PENDING' ? `
                    <button class="btn-deny" 
                            data-doc-id="${doc.document_id}"
                            onclick="handleValidation('${doc.document_id}', 'deny')">
                        ✗ DENY
                    </button>
                    ` : ''}
                </div>
                
                <div class="doc-col doc-status">
                    <span class="status-badge status-${doc.status.toLowerCase()}">
                        ${doc.status}
                    </span>
                </div>
                
                <div class="doc-col doc-schema">
                    <button class="btn-edit-schema" 
                            data-doc-id="${doc.document_id}"
                            onclick="openSchemaEditor('${doc.document_id}')">
                        EDIT ✎
                    </button>
                </div>
            `;
            
            container.appendChild(row);
        });
    },
    
    /**
     * Render key findings sidebar
     * 
     * @param {Array} findings - Array of finding objects
     * @param {string} containerId - ID of container element
     */
    renderFindings(findings, containerId = 'findingsContainer') {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        container.innerHTML = '';
        
        findings.forEach(finding => {
            const item = document.createElement('div');
            item.className = 'finding-item';
            item.innerHTML = `
                <div class="finding-header">
                    <span class="finding-source">${finding.source}</span>
                    <span class="badge badge-${finding.severity.toLowerCase()}">${finding.severity}</span>
                </div>
                <div class="finding-text">${finding.text}</div>
            `;
            container.appendChild(item);
        });
    },
    
    /**
     * Get file type icon (PDF, XLS, CSV, etc.)
     */
    getFileTypeIcon(filename) {
        const ext = filename.split('.').pop().toLowerCase();
        const icons = {
            pdf: 'PDF',
            xlsx: 'XLS',
            xls: 'XLS',
            csv: 'CSV',
            json: 'JSON',
            jpg: 'JPG',
            png: 'PNG',
        };
        return icons[ext] || 'DOC';
    },
    
    /**
     * Get human-readable document type label
     */
    getDocTypeLabel(docType) {
        const labels = {
            'ALM': 'ALM Statement',
            'SHAREHOLDING': 'Shareholding',
            'BORROWING_PROFILE': 'Borrowing Profile',
            'ANNUAL_REPORT': 'Annual Report',
            'PORTFOLIO_CUTS': 'Portfolio Cuts',
            'GST_FILING': 'GST Filings',
            'BANK_STATEMENT': 'Bank Statements',
            'ITR': 'ITR',
            'BOARD_MINUTES': 'Board Minutes',
            'RATING_REPORT': 'Rating Report',
            'SANCTION_LETTER': 'Sanction Letter',
            'UNKNOWN': 'Unknown',
        };
        return labels[docType] || docType;
    },
};

// ═══════════════════════════════════════════════════════════════════
// EVENT HANDLERS FOR FRONTEND
// ═══════════════════════════════════════════════════════════════════

/**
 * Handle "Launch Analysis" button click
 * This integrates with the User Onboarding flow in home2.html
 */
async function launchAnalysis() {
    try {
        // Get case ID from Entity Identification form
        const caseId = document.getElementById('entityCIN')?.value || 'CS2024-' + Date.now();
        
        // Get uploaded files
        const fileInput = document.getElementById('fileUpload');
        if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
            alert('Please upload at least one document');
            return;
        }
        
        // Show loading screen immediately (no popup)
        startProcessing();
        
        // Upload and process documents
        const result = await IngestorAPI.uploadDocuments(fileInput.files, caseId);
        
        // Store result in global state
        window.ingestorData = result;
        
        // Wait for processing animation to complete
        // The startProcessing() function will call enableDemoUI() when done
        // which transitions to the dashboard
        
        // Once on dashboard, populate the ingestor page
        setTimeout(() => {
            // Render document table
            IngestorUI.renderDocumentTable(result.documents);
            
            // Collect all findings
            const allFindings = [];
            result.documents.forEach(doc => {
                allFindings.push(...doc.key_findings);
            });
            
            // Render findings
            IngestorUI.renderFindings(allFindings);
            
            console.log('[✓] Ingestor page populated with API data');
        }, 500);
        
    } catch (error) {
        console.error('[✗] Launch Analysis failed:', error);
        alert(`Processing failed: ${error.message}`);
        
        // Hide loading screen
        const overlay = document.getElementById('procOverlay');
        if (overlay) overlay.classList.remove('active');
    }
}

/**
 * Handle document validation (approve/deny)
 */
async function handleValidation(documentId, action) {
    try {
        await IngestorAPI.validateDocument(documentId, action);
        
        // Update UI
        const btn = document.querySelector(`[data-doc-id="${documentId}"]`);
        if (btn && action === 'approve') {
            btn.classList.add('approved');
            btn.textContent = '✓ APPROVED';
        }
        
        console.log(`[✓] Document ${documentId} ${action}d`);
        
    } catch (error) {
        console.error('[✗] Validation failed:', error);
        alert(`Validation failed: ${error.message}`);
    }
}

/**
 * Open schema editor modal
 */
function openSchemaEditor(documentId) {
    // Find document in stored data
    const doc = window.ingestorData?.documents.find(d => d.document_id === documentId);
    if (!doc) {
        console.error('Document not found:', documentId);
        return;
    }
    
    // Build schema editor UI (simplified example)
    const editorHTML = `
        <div class="schema-editor-modal" id="schemaEditorModal">
            <div class="schema-editor-content">
                <div class="schema-editor-header">
                    <h3>Edit Schema: ${doc.filename}</h3>
                    <button onclick="closeSchemaEditor()">✕</button>
                </div>
                <div class="schema-editor-body">
                    <table class="schema-table">
                        <thead>
                            <tr>
                                <th>Field</th>
                                <th>Type</th>
                                <th>Value</th>
                            </tr>
                        </thead>
                        <tbody id="schemaFieldsTable">
                            ${doc.schema_fields.map(field => `
                                <tr>
                                    <td>${field.display_name}</td>
                                    <td>${field.data_type}</td>
                                    <td>
                                        <input type="text" 
                                               class="schema-field-input"
                                               data-field="${field.field_name}"
                                               value="${doc.extracted_data[field.field_name] || ''}"
                                               ${field.editable ? '' : 'readonly'}>
                                    </td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
                <div class="schema-editor-footer">
                    <button class="btn-primary" onclick="saveSchemaEdits('${documentId}')">Save Changes</button>
                    <button class="btn-secondary" onclick="closeSchemaEditor()">Cancel</button>
                </div>
            </div>
        </div>
    `;
    
    // Add to DOM
    document.body.insertAdjacentHTML('beforeend', editorHTML);
}

/**
 * Close schema editor
 */
function closeSchemaEditor() {
    const modal = document.getElementById('schemaEditorModal');
    if (modal) modal.remove();
}

/**
 * Save schema edits
 */
async function saveSchemaEdits(documentId) {
    try {
        // Collect edited values
        const inputs = document.querySelectorAll('.schema-field-input');
        const edits = {};
        
        inputs.forEach(input => {
            const fieldName = input.dataset.field;
            const value = input.value;
            edits[fieldName] = value;
        });
        
        // Save via API
        await IngestorAPI.editSchema(documentId, edits);
        
        // Close modal
        closeSchemaEditor();
        
        console.log(`[✓] Schema updated for ${documentId}`);
        alert('Schema updated successfully');
        
    } catch (error) {
        console.error('[✗] Schema save failed:', error);
        alert(`Save failed: ${error.message}`);
    }
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { IngestorAPI, IngestorUI };
}
