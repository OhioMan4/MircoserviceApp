/* ===========================================================
   Observability Lab — Web UI Application Logic
   =========================================================== */

const API = '/api';
const TRAFFIC_API = '/traffic';
const NOTIFICATIONS_API = '/notifications';
const INVENTORY_API = '/inventory';

// ============================================================
// Tab Navigation
// ============================================================
const navBtns = document.querySelectorAll('.nav-btn');
const tabs    = document.querySelectorAll('.tab-content');
const title   = document.getElementById('page-title');

const tabTitles = {
    dashboard: 'Dashboard',
    products: 'Products',
    orders: 'Orders',
    loadtest: 'Load Test',
    events: 'Events',
};

function switchTab(tabId) {
    navBtns.forEach(b => b.classList.toggle('active', b.dataset.tab === tabId));
    tabs.forEach(t => t.classList.toggle('active', t.id === `tab-${tabId}`));
    title.textContent = tabTitles[tabId] || tabId;

    if (tabId === 'products') loadProducts();
    if (tabId === 'orders')   { loadProducts(true); loadOrders(); }
    if (tabId === 'dashboard') refreshDashboard();
    if (tabId === 'loadtest') loadTrafficStatus();
    if (tabId === 'events') loadEventsTab();
}

navBtns.forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));

// ============================================================
// Toast Notifications
// ============================================================
function toast(msg, type = 'info') {
    const c = document.getElementById('toast-container');
    const icons = { success: '✅', error: '❌', info: 'ℹ️' };
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.innerHTML = `<span>${icons[type] || ''}</span><span>${msg}</span>`;
    c.appendChild(t);
    setTimeout(() => t.remove(), 4000);
}

// ============================================================
// Activity Feed
// ============================================================
function addFeedItem(icon, msg) {
    const feed = document.getElementById('activity-feed');
    const empty = feed.querySelector('.feed-empty');
    if (empty) empty.remove();

    const now = new Date().toLocaleTimeString();
    const item = document.createElement('div');
    item.className = 'feed-item';
    item.innerHTML = `
        <span class="feed-icon">${icon}</span>
        <div class="feed-body">
            <div class="feed-msg">${msg}</div>
            <div class="feed-time">${now}</div>
        </div>`;
    feed.prepend(item);

    // Keep max 50 items
    while (feed.children.length > 50) feed.lastElementChild.remove();
}

document.getElementById('btn-clear-feed').addEventListener('click', () => {
    document.getElementById('activity-feed').innerHTML =
        '<div class="feed-empty">No activity yet.</div>';
});

// ============================================================
// API Helpers
// ============================================================
async function api(path, options = {}) {
    try {
        const resp = await fetch(`${API}${path}`, options);
        const data = await resp.json();
        return { ok: resp.ok, status: resp.status, data };
    } catch (err) {
        return { ok: false, status: 0, data: { error: err.message } };
    }
}

async function trafficApi(path, options = {}) {
    try {
        const resp = await fetch(`${TRAFFIC_API}${path}`, options);
        const data = await resp.json();
        return { ok: resp.ok, status: resp.status, data };
    } catch (err) {
        return { ok: false, status: 0, data: { error: err.message } };
    }
}

async function notifApi(path, options = {}) {
    try {
        const resp = await fetch(`${NOTIFICATIONS_API}${path}`, options);
        const data = await resp.json();
        return { ok: resp.ok, status: resp.status, data };
    } catch (err) {
        return { ok: false, status: 0, data: { error: err.message } };
    }
}

async function invApi(path, options = {}) {
    try {
        const resp = await fetch(`${INVENTORY_API}${path}`, options);
        const data = await resp.json();
        return { ok: resp.ok, status: resp.status, data };
    } catch (err) {
        return { ok: false, status: 0, data: { error: err.message } };
    }
}

// ============================================================
// Dashboard — Health Checks
// ============================================================
async function checkHealth() {
    // API Gateway health (through nginx proxy)
    const gw = await api('/health');
    setHealthCard('health-api-gateway', gw.ok ? 'healthy' : 'down',
                  gw.ok ? 'Healthy' : 'Down');

    // Order service health (returns db + cache status)
    if (gw.ok && gw.data) {
        const osHealth = await api('/products');
        if (osHealth.ok) {
            setHealthCard('health-order-service', 'healthy', 'Healthy');
            const source = osHealth.data.source || 'unknown';
            setHealthCard('health-db', 'healthy', 'Connected');
            setHealthCard('health-cache', source === 'cache' ? 'healthy' : 'healthy',
                          source === 'cache' ? 'Hit' : 'Populated');
        } else {
            setHealthCard('health-order-service', 'down', 'Error');
            setHealthCard('health-db', 'degraded', 'Unknown');
            setHealthCard('health-cache', 'degraded', 'Unknown');
        }
    } else {
        setHealthCard('health-order-service', 'degraded', 'Unknown');
        setHealthCard('health-db', 'degraded', 'Unknown');
        setHealthCard('health-cache', 'degraded', 'Unknown');
    }

    // Notification Worker health
    const notifHealth = await notifApi('/health');
    setHealthCard('health-notification',
                  notifHealth.ok ? 'healthy' : 'down',
                  notifHealth.ok ? 'Healthy' : 'Down');

    // Inventory Worker health
    const invHealth = await invApi('/health');
    setHealthCard('health-inventory',
                  invHealth.ok ? 'healthy' : 'down',
                  invHealth.ok ? 'Healthy' : 'Down');
}

function setHealthCard(id, status, text) {
    const card = document.getElementById(id);
    card.className = `health-card ${status}`;
    const statusEl = card.querySelector('.health-status');
    statusEl.className = `health-status ${status}`;
    statusEl.textContent = text;
}

// ============================================================
// Dashboard — Stats
// ============================================================
async function loadStats() {
    const [prodResp, orderResp] = await Promise.all([
        api('/products'),
        api('/orders?limit=50'),
    ]);

    if (prodResp.ok) {
        const products = prodResp.data.products || [];
        document.getElementById('stat-products').textContent = products.length;
        document.getElementById('stat-cache-source').textContent =
            (prodResp.data.source || '—').toUpperCase();
    }

    if (orderResp.ok) {
        const orders = orderResp.data.orders || [];
        document.getElementById('stat-orders').textContent = orderResp.data.count || 0;
        if (orders.length > 0) {
            document.getElementById('stat-last-order').textContent =
                orders[0].order_id || '—';
        }
    }
}

async function refreshDashboard() {
    await Promise.all([checkHealth(), loadStats()]);
}

// ============================================================
// Products
// ============================================================
let cachedProducts = [];

async function loadProducts(selectOnly = false) {
    const resp = await api('/products');
    if (!resp.ok) {
        if (!selectOnly) toast('Failed to load products', 'error');
        return;
    }

    cachedProducts = resp.data.products || [];
    const source = resp.data.source || 'unknown';

    if (!selectOnly) {
        renderProductsTable(cachedProducts);
        const badge = document.getElementById('products-source');
        badge.textContent = `Source: ${source}`;
        badge.className = `source-badge ${source}`;
        addFeedItem('📦', `Loaded ${cachedProducts.length} products (${source})`);
    }

    // Also populate the order form select
    populateProductSelect(cachedProducts);
}

function renderProductsTable(products) {
    const tbody = document.getElementById('products-tbody');
    if (products.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading-cell">No products found</td></tr>';
        return;
    }

    tbody.innerHTML = products.map(p => `
        <tr>
            <td style="font-family:'JetBrains Mono',monospace; color:var(--text-muted)">#${p.id}</td>
            <td style="font-weight:500; color:var(--text-primary)">${escHtml(p.name)}</td>
            <td>${escHtml(p.category || '—')}</td>
            <td class="price">$${p.price.toFixed(2)}</td>
            <td class="stock ${p.stock < 10 ? 'low' : 'ok'}">${p.stock}</td>
            <td><button class="buy-btn" onclick="quickBuy(${p.id})">Buy</button></td>
        </tr>
    `).join('');
}

function populateProductSelect(products) {
    const select = document.getElementById('order-product');
    const current = select.value;
    select.innerHTML = '<option value="">Select a product...</option>';
    products.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.id;
        opt.textContent = `${p.name} — $${p.price.toFixed(2)} (stock: ${p.stock})`;
        select.appendChild(opt);
    });
    if (current) select.value = current;
}

document.getElementById('btn-reload-products').addEventListener('click', () => loadProducts());

// ============================================================
// Orders
// ============================================================
async function loadOrders() {
    const resp = await api('/orders?limit=50');
    if (!resp.ok) {
        toast('Failed to load orders', 'error');
        return;
    }

    const orders = resp.data.orders || [];
    renderOrdersTable(orders);
}

function renderOrdersTable(orders) {
    const tbody = document.getElementById('orders-tbody');
    if (orders.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="loading-cell">No orders yet</td></tr>';
        return;
    }

    tbody.innerHTML = orders.map(o => {
        const time = new Date(o.created_at).toLocaleString();
        return `
        <tr>
            <td style="font-family:'JetBrains Mono',monospace; color:var(--accent)">${escHtml(o.order_id)}</td>
            <td style="font-weight:500">${escHtml(o.product_name)}</td>
            <td style="text-align:center">${o.quantity}</td>
            <td class="price">$${o.total_amount.toFixed(2)}</td>
            <td><span class="status-badge ${o.status}">${o.status}</span></td>
            <td style="font-family:'JetBrains Mono',monospace; font-size:0.78rem; color:var(--text-muted)">${escHtml(o.payment_txn_id || '—')}</td>
            <td style="font-size:0.8rem; color:var(--text-muted)">${time}</td>
        </tr>`;
    }).join('');
}

document.getElementById('btn-reload-orders').addEventListener('click', () => loadOrders());

// ============================================================
// Create Order
// ============================================================
document.getElementById('order-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const productId = parseInt(document.getElementById('order-product').value);
    const quantity  = parseInt(document.getElementById('order-quantity').value);

    if (!productId) { toast('Please select a product', 'error'); return; }

    const btn = document.getElementById('btn-submit-order');
    btn.disabled = true;
    btn.textContent = '⏳ Processing...';

    const resp = await api('/order', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ product_id: productId, quantity }),
    });

    btn.disabled = false;
    btn.innerHTML = '🛒 Place Order';

    const resultDiv = document.getElementById('order-result');
    resultDiv.classList.remove('hidden', 'success', 'error');

    if (resp.ok) {
        const order = resp.data.order || resp.data;
        resultDiv.classList.add('success');
        resultDiv.innerHTML = `
            <strong>✅ Order created!</strong><br>
            Order ID: <code>${escHtml(order.order_id)}</code> &nbsp;|&nbsp;
            Product: ${escHtml(order.product)} &nbsp;|&nbsp;
            Amount: $${order.total_amount?.toFixed(2)} &nbsp;|&nbsp;
            Status: ${order.status}`;
        toast(`Order ${order.order_id} created`, 'success');
        addFeedItem('🛒', `Order <strong>${escHtml(order.order_id)}</strong> — ${escHtml(order.product)} x${quantity} — $${order.total_amount?.toFixed(2)}`);
        loadOrders();
        loadProducts(true); // refresh stock
    } else {
        const msg = resp.data.message || resp.data.error || 'Unknown error';
        resultDiv.classList.add('error');
        resultDiv.innerHTML = `<strong>❌ Order failed:</strong> ${escHtml(msg)}`;
        toast('Order failed: ' + msg, 'error');
        addFeedItem('❌', `Order failed: ${escHtml(msg)}`);
    }
});

// Quick-buy from products table
window.quickBuy = function(productId) {
    switchTab('orders');
    document.getElementById('order-product').value = productId;
    document.getElementById('order-quantity').value = 1;
    document.getElementById('order-quantity').focus();
};

// ============================================================
// Quick Actions (Dashboard)
// ============================================================
document.getElementById('btn-quick-order').addEventListener('click', async () => {
    addFeedItem('🎲', 'Creating random order...');
    const resp = await api('/order', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            product_id: Math.floor(Math.random() * 5) + 1,
            quantity: Math.floor(Math.random() * 3) + 1,
        }),
    });

    if (resp.ok) {
        const order = resp.data.order || resp.data;
        toast(`Random order ${order.order_id} created`, 'success');
        addFeedItem('✅', `Random order <strong>${escHtml(order.order_id)}</strong> — ${escHtml(order.product)} — $${order.total_amount?.toFixed(2)}`);
        loadStats();
    } else {
        toast('Random order failed', 'error');
        addFeedItem('❌', `Random order failed: ${escHtml(resp.data.message || resp.data.error || '')}`);
    }
});

document.getElementById('btn-quick-products').addEventListener('click', async () => {
    await loadProducts();
    loadStats();
    toast('Products refreshed', 'info');
});

document.getElementById('btn-quick-health').addEventListener('click', async () => {
    addFeedItem('💚', 'Running health check...');
    await checkHealth();
    toast('Health check complete', 'info');
    addFeedItem('✅', 'Health check completed');
});

document.getElementById('btn-refresh').addEventListener('click', () => {
    const active = document.querySelector('.nav-btn.active')?.dataset.tab;
    if (active === 'dashboard') refreshDashboard();
    if (active === 'products')  loadProducts();
    if (active === 'orders')    { loadProducts(true); loadOrders(); }
    if (active === 'loadtest')  loadTrafficStatus();
    if (active === 'events')    loadEventsTab();
    toast('Refreshed', 'info');
});

// ============================================================
// Load Test — Traffic Generator Control
// ============================================================
const SCENARIO_DESCS = {
    normal:       'Balanced user activity — browsing, ordering, viewing history',
    flash_sale:   'High-volume ordering — simulates promotion/sale event',
    browse_heavy: 'Mostly browsing with occasional orders — window shoppers',
    health_check: 'Continuous health checks — monitors service availability',
    event_driven: 'Orders + verify Kafka event processing (notifications + inventory)',
};

const SCENARIO_DEFAULTS = {
    normal: 2, flash_sale: 10, browse_heavy: 5, health_check: 3, event_driven: 3,
};

let trafficPollingInterval = null;

// Scenario selector → update description + default rate
document.getElementById('lt-scenario').addEventListener('change', (e) => {
    const scenario = e.target.value;
    document.getElementById('lt-scenario-desc').textContent =
        SCENARIO_DESCS[scenario] || '';
    // Set default rate for scenario
    const rate = SCENARIO_DEFAULTS[scenario] || 2;
    document.getElementById('lt-rate').value = rate;
    document.getElementById('lt-rate-value').textContent = rate;
});

// Slider value display
document.getElementById('lt-rate').addEventListener('input', (e) => {
    document.getElementById('lt-rate-value').textContent = e.target.value;
});
document.getElementById('lt-duration').addEventListener('input', (e) => {
    document.getElementById('lt-duration-value').textContent = e.target.value + 's';
});

// Load traffic status
async function loadTrafficStatus() {
    const resp = await trafficApi('/status');
    if (!resp.ok) {
        updateTrafficUI(false, null);
        return;
    }
    const d = resp.data;
    updateTrafficUI(d.running, d);
}

function updateTrafficUI(running, data) {
    const badge = document.getElementById('lt-status-badge');
    const statusText = document.getElementById('lt-status-text');
    const startBtn = document.getElementById('btn-lt-start');
    const stopBtn  = document.getElementById('btn-lt-stop');
    const scenarioSelect = document.getElementById('lt-scenario');
    const rateSlider = document.getElementById('lt-rate');
    const durationSlider = document.getElementById('lt-duration');
    const progressWrap = document.getElementById('lt-progress-wrap');

    if (running) {
        badge.className = 'lt-status-badge running';
        statusText.textContent = 'Running';
        startBtn.disabled = true;
        startBtn.innerHTML = '<span>⏳</span> Running...';
        stopBtn.disabled = false;
        scenarioSelect.disabled = true;
        rateSlider.disabled = true;
        durationSlider.disabled = true;
        progressWrap.style.display = 'flex';

        if (data) {
            // Update stats
            document.getElementById('lt-stat-total').textContent = data.stats.total;
            document.getElementById('lt-stat-success').textContent = data.stats.success;
            document.getElementById('lt-stat-errors').textContent = data.stats.errors;
            document.getElementById('lt-stat-elapsed').textContent = data.stats.elapsed + 's';

            // Update progress
            const pct = Math.min(100, ((data.stats.elapsed / data.duration) * 100));
            document.getElementById('lt-progress-fill').style.width = pct + '%';
            document.getElementById('lt-progress-text').textContent = Math.round(pct) + '%';

            // Show current config
            scenarioSelect.value = data.scenario;
            rateSlider.value = data.rate;
            document.getElementById('lt-rate-value').textContent = data.rate;
            durationSlider.value = data.duration;
            document.getElementById('lt-duration-value').textContent = data.duration + 's';
        }

        // Start polling if not already
        if (!trafficPollingInterval) {
            trafficPollingInterval = setInterval(loadTrafficStatus, 2000);
        }
    } else {
        badge.className = 'lt-status-badge idle';
        statusText.textContent = 'Idle';
        startBtn.disabled = false;
        startBtn.innerHTML = '<span>▶</span> Start Traffic';
        stopBtn.disabled = true;
        scenarioSelect.disabled = false;
        rateSlider.disabled = false;
        durationSlider.disabled = false;

        // Stop polling
        if (trafficPollingInterval) {
            clearInterval(trafficPollingInterval);
            trafficPollingInterval = null;
        }

        // Keep last stats visible, hide progress if no data
        if (data && data.stats.total === 0) {
            progressWrap.style.display = 'none';
        }
    }
}

// Start Traffic
document.getElementById('btn-lt-start').addEventListener('click', async () => {
    const scenario = document.getElementById('lt-scenario').value;
    const rate = parseInt(document.getElementById('lt-rate').value);
    const duration = parseInt(document.getElementById('lt-duration').value);

    const startBtn = document.getElementById('btn-lt-start');
    startBtn.disabled = true;
    startBtn.innerHTML = '<span>⏳</span> Starting...';

    // Reset stats UI
    document.getElementById('lt-stat-total').textContent = '0';
    document.getElementById('lt-stat-success').textContent = '0';
    document.getElementById('lt-stat-errors').textContent = '0';
    document.getElementById('lt-stat-elapsed').textContent = '0s';
    document.getElementById('lt-progress-fill').style.width = '0%';
    document.getElementById('lt-progress-text').textContent = '0%';
    document.getElementById('lt-progress-wrap').style.display = 'flex';

    const resp = await trafficApi('/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scenario, rate, duration }),
    });

    if (resp.ok) {
        toast(`Traffic started: ${resp.data.message}`, 'success');
        addFeedItem('⚡', `Load test started: <strong>${scenario}</strong> @ ${rate} req/s for ${duration}s`);
        loadTrafficStatus();
    } else {
        toast('Failed to start: ' + (resp.data.error || 'Unknown error'), 'error');
        startBtn.disabled = false;
        startBtn.innerHTML = '<span>▶</span> Start Traffic';
    }
});

// Stop Traffic
document.getElementById('btn-lt-stop').addEventListener('click', async () => {
    const stopBtn = document.getElementById('btn-lt-stop');
    stopBtn.disabled = true;
    stopBtn.innerHTML = '<span>⏳</span> Stopping...';

    const resp = await trafficApi('/stop', { method: 'POST' });

    if (resp.ok) {
        toast('Traffic stopped', 'info');
        addFeedItem('🛑', `Load test stopped. Total: ${resp.data.stats?.total || 0} requests`);
    } else {
        toast('Failed to stop', 'error');
    }

    // Refresh status after a brief delay
    setTimeout(loadTrafficStatus, 500);
});

// ============================================================
// Utility
// ============================================================
function escHtml(str) {
    if (!str) return '';
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ============================================================
// Auto-refresh & Init
// ============================================================
(async function init() {
    await refreshDashboard();
    await loadProducts();
})();

// Auto-refresh every 30s
setInterval(() => {
    const active = document.querySelector('.nav-btn.active')?.dataset.tab;
    if (active === 'dashboard') refreshDashboard();
    if (active === 'events') loadEventsTab();
}, 30000);

// ============================================================
// Events Tab — Kafka Workers Dashboard
// ============================================================
async function loadEventsTab() {
    await Promise.all([loadWorkerStatuses(), loadNotifications(), loadInventoryLog()]);
}

async function loadWorkerStatuses() {
    // Notification Worker
    const nResp = await notifApi('/status');
    if (nResp.ok) {
        const d = nResp.data;
        document.getElementById('evt-notif-badge').textContent = d.status || 'running';
        document.getElementById('evt-notif-badge').className =
            `events-worker-badge ${d.status === 'running' ? 'running' : 'stopped'}`;
        document.getElementById('evt-notif-processed').textContent = d.events_processed ?? '—';
        document.getElementById('evt-notif-errors').textContent = d.errors ?? '—';
        document.getElementById('evt-notif-uptime').textContent = formatUptime(d.uptime_seconds);
        document.getElementById('evt-notification-card').className = 'events-worker-card healthy';
    } else {
        document.getElementById('evt-notif-badge').textContent = 'down';
        document.getElementById('evt-notif-badge').className = 'events-worker-badge down';
        document.getElementById('evt-notification-card').className = 'events-worker-card down';
    }

    // Inventory Worker
    const iResp = await invApi('/status');
    if (iResp.ok) {
        const d = iResp.data;
        document.getElementById('evt-inv-badge').textContent = d.status || 'running';
        document.getElementById('evt-inv-badge').className =
            `events-worker-badge ${d.status === 'running' ? 'running' : 'stopped'}`;
        document.getElementById('evt-inv-processed').textContent = d.events_processed ?? '—';
        document.getElementById('evt-inv-errors').textContent = d.errors ?? '—';
        document.getElementById('evt-inv-uptime').textContent = formatUptime(d.uptime_seconds);
        document.getElementById('evt-inventory-card').className = 'events-worker-card healthy';
    } else {
        document.getElementById('evt-inv-badge').textContent = 'down';
        document.getElementById('evt-inv-badge').className = 'events-worker-badge down';
        document.getElementById('evt-inventory-card').className = 'events-worker-card down';
    }
}

function formatUptime(seconds) {
    if (!seconds && seconds !== 0) return '—';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return `${Math.round(seconds / 3600)}h`;
}

async function loadNotifications() {
    const resp = await notifApi('/notifications?limit=30');
    const tbody = document.getElementById('notifications-tbody');
    if (!resp.ok) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading-cell">Failed to load notifications</td></tr>';
        return;
    }
    const items = resp.data.notifications || resp.data || [];
    if (items.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading-cell">No notifications yet</td></tr>';
        return;
    }
    tbody.innerHTML = items.map(n => {
        const time = n.created_at ? new Date(n.created_at).toLocaleString() : '—';
        const eventIcon = {
            'order.created': '📨',
            'order.payment_completed': '✅',
            'order.payment_failed': '❌',
        }[n.event_type] || '📩';
        return `
        <tr>
            <td><span class="evt-type-badge">${eventIcon} ${escHtml(n.event_type)}</span></td>
            <td style="font-family:'JetBrains Mono',monospace; color:var(--accent)">${escHtml(n.order_id || '—')}</td>
            <td>${escHtml(n.channel || n.notification_type || '—')}</td>
            <td><span class="status-badge ${n.status === 'sent' ? 'completed' : n.status}">${escHtml(n.status || '—')}</span></td>
            <td style="font-size:0.8rem; color:var(--text-muted)">${time}</td>
        </tr>`;
    }).join('');
}

async function loadInventoryLog() {
    const resp = await invApi('/inventory/log?limit=30');
    const tbody = document.getElementById('inventory-log-tbody');
    if (!resp.ok) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading-cell">Failed to load inventory log</td></tr>';
        return;
    }
    const items = resp.data.log || resp.data || [];
    if (items.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading-cell">No inventory changes yet</td></tr>';
        return;
    }
    tbody.innerHTML = items.map(e => {
        const time = e.created_at ? new Date(e.created_at).toLocaleString() : '—';
        const actionIcon = e.action === 'reserve' ? '📥' : (e.action === 'release' ? '📤' : '🔄');
        const qtyClass = e.quantity_change < 0 ? 'stock low' : 'stock ok';
        return `
        <tr>
            <td><span class="evt-type-badge">${actionIcon} ${escHtml(e.action || '—')}</span></td>
            <td style="text-align:center">#${e.product_id || '—'}</td>
            <td class="${qtyClass}" style="text-align:center">${e.quantity_change > 0 ? '+' : ''}${e.quantity_change ?? '—'}</td>
            <td style="font-family:'JetBrains Mono',monospace; color:var(--accent); font-size:0.78rem">${escHtml(e.order_id || '—')}</td>
            <td style="font-size:0.8rem; color:var(--text-muted)">${time}</td>
        </tr>`;
    }).join('');
}

// Events tab refresh button
document.getElementById('btn-reload-events')?.addEventListener('click', () => loadEventsTab());
