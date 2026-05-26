// === Theme Toggle ===
const themeToggle = document.getElementById('theme-toggle');
const html = document.documentElement;

themeToggle.addEventListener('click', () => {
    const current = html.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
});

// Restore saved theme
const saved = localStorage.getItem('theme');
if (saved) html.setAttribute('data-theme', saved);

// === Search ===
const searchInput = document.getElementById('search-input');
const suggestionsDiv = document.getElementById('suggestions');
let debounceTimer;

searchInput.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const q = searchInput.value.trim();
    if (q.length < 1) { suggestionsDiv.classList.add('hidden'); return; }
    debounceTimer = setTimeout(() => fetchSuggestions(q), 250);
});

async function fetchSuggestions(q) {
    const resp = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
    const tickers = await resp.json();
    if (tickers.length === 0) { suggestionsDiv.classList.add('hidden'); return; }

    suggestionsDiv.innerHTML = tickers.map(t =>
        `<div class="suggestion-item" data-ticker="${t}">${t}</div>`
    ).join('');
    suggestionsDiv.classList.remove('hidden');

    suggestionsDiv.querySelectorAll('.suggestion-item').forEach(el => {
        el.addEventListener('click', () => {
            searchInput.value = el.dataset.ticker;
            suggestionsDiv.classList.add('hidden');
            loadTicker(el.dataset.ticker);
        });
    });
}

// Close suggestions on outside click
document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-container')) suggestionsDiv.classList.add('hidden');
});

// Enter key
searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        suggestionsDiv.classList.add('hidden');
        loadTicker(searchInput.value.trim().toUpperCase());
    }
});

// === Load Ticker Data ===
const TABLES = ['income_stmt', 'cash_flow', 'balance_sheet', 'financials'];
const COLORS = ['#818cf8', '#34d399', '#fbbf24', '#f87171', '#60a5fa', '#a78bfa', '#fb923c', '#2dd4bf'];
let charts = {};

async function loadTicker(ticker) {
    if (!ticker) return;
    document.getElementById('ticker-title').textContent = ticker;
    document.getElementById('results').classList.remove('hidden');

    const resp = await fetch(`/api/financial_data/${encodeURIComponent(ticker)}`);
    const data = await resp.json();

    for (const table of TABLES) {
        for (const freq of ['annual', 'quarterly']) {
            renderChart(table, freq, data[table][freq]);
            renderTable(table, freq, data[table][freq]);
        }
    }
}

function renderChart(table, freq, rows) {
    const canvasId = `chart-${table}-${freq}`;
    const canvas = document.getElementById(canvasId);

    // Destroy existing chart
    if (charts[canvasId]) { charts[canvasId].destroy(); }

    if (!rows || rows.length === 0) {
        charts[canvasId] = new Chart(canvas, {
            type: 'bar', data: { labels: [], datasets: [] },
            options: { plugins: { title: { display: true, text: 'No data available' } } }
        });
        return;
    }

    // Pivot: group by metric, x-axis = report_date
    const metrics = [...new Set(rows.map(r => r.metric))];
    const dates = [...new Set(rows.map(r => r.report_date))].sort();

    // Limit to top 5 metrics by absolute total value for readability
    const metricTotals = metrics.map(m => ({
        metric: m,
        total: rows.filter(r => r.metric === m).reduce((s, r) => s + Math.abs(r.value || 0), 0)
    }));
    metricTotals.sort((a, b) => b.total - a.total);
    const topMetrics = metricTotals.slice(0, 5).map(m => m.metric);

    const datasets = topMetrics.map((metric, i) => {
        const values = dates.map(d => {
            const row = rows.find(r => r.metric === metric && r.report_date === d);
            return row ? row.value : null;
        });
        return {
            label: metric.replace(/^(annual|quarterly)/, ''),
            data: values,
            borderColor: COLORS[i % COLORS.length],
            backgroundColor: COLORS[i % COLORS.length] + '33',
            tension: 0.3,
            fill: false,
        };
    });

    const isDark = html.getAttribute('data-theme') === 'dark';
    const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
    const textColor = isDark ? '#a0a0b0' : '#555';

    charts[canvasId] = new Chart(canvas, {
        type: 'line',
        data: { labels: dates, datasets },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'bottom', labels: { color: textColor, boxWidth: 12, font: { size: 10 } } },
            },
            scales: {
                x: { ticks: { color: textColor, font: { size: 9 } }, grid: { color: gridColor } },
                y: { ticks: { color: textColor, font: { size: 9 }, callback: (v) => formatValue(v) }, grid: { color: gridColor } },
            }
        }
    });
}

function formatValue(v) {
    if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1) + 'B';
    if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K';
    return v?.toFixed?.(2) ?? v;
}

function renderTable(table, freq, rows) {
    const container = document.getElementById(`table-${table}-${freq}`);
    if (!rows || rows.length === 0) {
        container.innerHTML = '<p style="color:var(--text-secondary);padding:1rem;">No data available</p>';
        return;
    }

    // Pivot: metrics as rows, dates as columns
    const metrics = [...new Set(rows.map(r => r.metric))];
    const dates = [...new Set(rows.map(r => r.report_date))].sort();

    let html_str = '<table><thead><tr><th>Metric</th>';
    dates.forEach(d => { html_str += `<th>${d}</th>`; });
    html_str += '</tr></thead><tbody>';

    metrics.forEach(metric => {
        html_str += `<tr><td>${metric.replace(/^(annual|quarterly)/, '')}</td>`;
        dates.forEach(d => {
            const row = rows.find(r => r.metric === metric && r.report_date === d);
            const val = row ? formatValue(row.value) : '-';
            html_str += `<td>${val}</td>`;
        });
        html_str += '</tr>';
    });

    html_str += '</tbody></table>';
    container.innerHTML = html_str;
}

// === Frequency Tabs ===
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        document.querySelectorAll('.freq-content').forEach(c => c.classList.remove('active'));
        document.getElementById(`${btn.dataset.freq}-content`).classList.add('active');
    });
});
