// Override the broken loadAllocationMatrix function
window.addEventListener('DOMContentLoaded', function() {
    window.loadAllocationMatrix = async function() {
        const response = await fetch('/visualizer/allocation-matrix');
        const data = await response.json();
        
        const container = document.getElementById('allocation-matrix-container');
        
        if (!data.allocations || Object.keys(data.allocations).length === 0) {
            container.innerHTML = '<div class="loading">No allocation data available</div>';
            return;
        }
        
        let html = `
            <div class="matrix-header" style="margin-bottom: 1rem;">
                <h3>Shard Allocation Matrix</h3>
                <div class="matrix-legend" style="margin-top: 0.5rem; padding: 0.5rem; background: #f5f5f5; border-radius: 4px;">
                    <strong>Legend:</strong> 
                    <span style="margin-left: 1rem;"><strong>p</strong> = Primary (Ingest)</span>
                    <span style="margin-left: 1rem;"><strong>r</strong> = Replica (Search)</span>
                </div>
            </div>
        `;
        
        for (const [indexName, nodeAllocations] of Object.entries(data.allocations)) {
            html += `
                <div class="index-allocation-section" style="margin-bottom: 2rem; border: 1px solid #ddd; border-radius: 8px; overflow: hidden;">
                    <div class="index-header" style="background: #667eea; color: white; padding: 1rem;">
                        <h4 style="margin: 0;">${indexName}</h4>
                    </div>
                    <div style="padding: 1rem;">
                        <table class="matrix-table" style="width: 100%; border-collapse: collapse;">
                            <thead>
                                <tr style="background: #f8f9fa;">
                                    <th style="border: 1px solid #ddd; padding: 0.5rem; text-align: left;">Node</th>
                                    <th style="border: 1px solid #ddd; padding: 0.5rem; text-align: left;">Shards</th>
                                </tr>
                            </thead>
                            <tbody>
            `;
            
            for (const [nodeName, shards] of Object.entries(nodeAllocations)) {
                html += `<tr><td style="border: 1px solid #ddd; padding: 0.5rem; font-weight: bold;">${nodeName}</td><td style="border: 1px solid #ddd; padding: 0.5rem;">`;
                shards.forEach(shard => {
                    const isPrimary = shard.endsWith('p');
                    const shardNum = shard.slice(0, -1);
                    const color = isPrimary ? '#28a745' : '#007bff';
                    const label = isPrimary ? 'Primary' : 'Replica';
                    html += `<span style="display: inline-block; margin: 0.25rem; padding: 0.25rem 0.5rem; background: ${color}; color: white; border-radius: 3px; font-size: 0.9rem;">Shard ${shardNum} (${label})</span>`;
                });
                html += `</td></tr>`;
            }
            
            html += `</tbody></table></div></div>`;
        }
        
        container.innerHTML = html;
    };
});
