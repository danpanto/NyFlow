import { filterService } from '../services/FilterService.js';
import { vendorService } from '../services/VendorService.js';
import { dataRangeservice } from '../services/DataRangeService.js';

const template = document.createElement('template');
template.innerHTML = `
    <style>
        :host {
            display: flex;
            flex-direction: column;
            width: 300px;
            background-color: var(--app-bg, #ffffff);
            border-radius: 16px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.18);
            font-family: system-ui, sans-serif;
            border: 1px solid var(--map-bg, #e5e7eb);
            /* Scrolling handled by the global CSS rule on the host element */
        }

        .section {
            padding: 16px 20px;
            display: flex;
            flex-direction: column;
            gap: 10px;
            flex-shrink: 0;
        }

        .section-divider {
            height: 1px;
            background-color: var(--app-text-secondary, #e5e7eb);
            margin: 0 20px;
            flex-shrink: 0;
        }

        h3 {
            margin: 0 0 2px 0;
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--app-text-title, #111827);
        }

        .header {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 12px;
        }

        .clear-btn {
            background: transparent;
            border: none;
            color: #94a3b8;
            cursor: pointer;
            font-size: 1.1rem;
            line-height: 1;
            padding: 2px 6px;
            padding-left: 8px;
            border-radius: 6px;
            transition: all 0.2s;
            visibility: hidden; 
            opacity: 0;
        }

        .clear-btn.visible {
            visibility: visible;
            opacity: 1;
        }

        .clear-btn:hover {
            color: #ef4444;
            background: #fee2e2;
        }

        /* ── Vendor grid ─────────────────────────────────────── */
        .vendor-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 6px;
        }

        .vbtn {
            background: var(--app-bg-secondary, #e5e7eb);
            border: 1px solid var(--map-bg, #d4dadc);
            color: var(--app-text, #1f2937);
            padding: 8px 6px;
            border-radius: 8px;
            font-size: 0.78rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.18s;
            text-align: center;
        }
        .vbtn:hover { border-color: var(--app-text-accent, #2563eb); }
        .vbtn.active {
            background: var(--app-text-accent, #2563eb);
            color: #fff;
            border-color: var(--app-text-accent, #2563eb);
        }
        .vendor-grid .vbtn:last-child:nth-child(odd) { grid-column: 1 / -1; }

        /* ── Days grid ───────────────────────────────────────── */
        .days-grid {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 4px;
        }
        .days-grid .vbtn {
            padding: 8px 0;
            font-size: 0.75rem;
        }

        /* ── Date range ──────────────────────────────────────── */
        .range-btns {
            display: flex;
            gap: 3px;
        }
        .rbtn {
            flex: 1;
            background: var(--app-bg-secondary, #e5e7eb);
            border: 1px solid var(--map-bg, #d4dadc);
            color: var(--app-text, #1f2937);
            padding: 5px 0;
            border-radius: 6px;
            font-size: 0.7rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.18s;
            text-align: center;
        }
        .rbtn:hover { border-color: var(--app-text-accent, #2563eb); }
        .rbtn.active {
            background: var(--app-text-accent, #2563eb);
            color: #fff;
            border-color: var(--app-text-accent, #2563eb);
        }

        .input-group {
            display: flex;
            align-items: center;
            gap: 8px;
            background: var(--app-bg-secondary, #f3f4f6);
            padding: 6px 10px;
            border-radius: 8px;
            border: 1px solid var(--map-bg, #d4dadc);
        }
        .input-group label {
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--app-text-title, #111827);
            min-width: 32px;
        }
        input[type="date"] {
            flex: 1;
            border: 1px solid var(--map-bg, #d4dadc);
            background: var(--app-bg, #ffffff);
            color: var(--app-text, #1f2937);
            font-family: inherit;
            font-size: 0.85rem;
            padding: 3px 6px;
            border-radius: 4px;
            outline: none;
            min-width: 0;
        }

        /* ── Hour dial ───────────────────────────────────────── */
        .dial-wrapper {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 6px;
            padding-bottom: 4px;
            flex-shrink: 0;
        }

        .dial-hint {
            font-size: 0.68rem;
            opacity: 0.5;
            color: var(--app-text, #1f2937);
            letter-spacing: 0.04em;
        }

        .dial-canvas-wrap {
            position: relative;
            width: 200px;
            height: 200px;
            cursor: grab;
            flex-shrink: 0;
        }
        .dial-canvas-wrap:active { cursor: grabbing; }

        canvas#dial-canvas {
            width: 200px;
            height: 200px;
            display: block;
            touch-action: none;
        }

        .dial-center-text {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            pointer-events: none;
            text-align: center;
            line-height: 1.1;
        }
        .dial-hour-num {
            font-size: 2.1rem;
            font-weight: 800;
            color: var(--app-text-title, #111827);
        }
        .dial-center-sub {
            font-size: 0.6rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            opacity: 0.45;
            color: var(--app-text-title, #111827);
        }
    </style>

    <!-- ── Vendor filter ─────────────────────────────────── -->
    <div class="section">
        <div class="header">
            <h3>Service vendor filter</h3>
            <button id="clear-vendors-btn" class="clear-btn" title="Clear selection">✕</button>
        </div>
        <div class="vendor-grid" id="vendor-grid">
            <div class="vbtn" style="opacity:.4;pointer-events:none;grid-column:1/-1;text-align:center">Loading…</div>
        </div>
    </div>

    <div class="section-divider"></div>

    <!-- ── Date range ────────────────────────────────────── -->
    <div class="section">
        <h3>Date Range</h3>
        <div class="range-btns" id="range-btns">
            <button class="rbtn" data-unit="h">1h</button>
            <button class="rbtn" data-unit="d">1d</button>
            <button class="rbtn" data-unit="w">1w</button>
            <button class="rbtn" data-unit="m">1m</button>
            <button class="rbtn" data-unit="y">1y</button>
            <button class="rbtn" data-unit="all">All</button>
        </div>
        <div class="input-group">
            <label>Start</label>
            <input type="date" id="start-date">
        </div>
        <div class="input-group">
            <label>End</label>
            <input type="date" id="end-date">
        </div>
    </div>

    <div class="section-divider"></div>

        <!-- ── Day of Week filter ────────────────────────────── -->
    <div class="section">
        <div class="header">
            <h3>Day of week filter</h3>
            <button id="clear-days-btn" class="clear-btn" title="Clear selection">✕</button>
        </div>
        <div class="days-grid" id="days-grid">
            <button class="vbtn" data-id="1">L</button>
            <button class="vbtn" data-id="2">M</button>
            <button class="vbtn" data-id="3">X</button>
            <button class="vbtn" data-id="4">J</button>
            <button class="vbtn" data-id="5">V</button>
            <button class="vbtn" data-id="6">S</button>
            <button class="vbtn" data-id="7">D</button>
        </div>
    </div>

    <div class="section-divider"></div>

    <!-- ── Hour dial ──────────────────────────────────────── -->
    <div class="section">
        <h3>Hour of Day</h3>
        <div class="dial-wrapper">
            <div class="dial-canvas-wrap" id="dial-wrap">
                <canvas id="dial-canvas" width="400" height="400"></canvas>
                <div class="dial-center-text">
                    <div class="dial-hour-num" id="dial-hour-display">00</div>
                    <div class="dial-center-sub">hour</div>
                </div>
            </div>
            <div class="dial-hint">Scroll or drag to change hour</div>
        </div>
    </div>
`;

export class DemandHourlyControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));

        this._hour = 0;
        this._minDate = null;
        this._maxDate = null;
        this._onHourChange = null;

        // Element references
        this.vendorGrid   = this.shadowRoot.getElementById('vendor-grid');
        this.clearVendorsBtn = this.shadowRoot.getElementById('clear-vendors-btn');
        this.daysGrid     = this.shadowRoot.getElementById('days-grid');
        this.clearDaysBtn = this.shadowRoot.getElementById('clear-days-btn');
        this.startInput   = this.shadowRoot.getElementById('start-date');
        this.endInput     = this.shadowRoot.getElementById('end-date');
        this.rangeBtns    = this.shadowRoot.querySelectorAll('.rbtn');
        this.dialWrap     = this.shadowRoot.getElementById('dial-wrap');
        this.dialCanvas   = this.shadowRoot.getElementById('dial-canvas');
        this.dialDisplay  = this.shadowRoot.getElementById('dial-hour-display');

        this._dragging     = false;
        this._lastAngle    = null;
        this._accumulator  = 0;

        this._unsubDate    = null;
        this._unsubVendors = null;
        this._unsubDays    = null;
    }

    // Public API
    get hour() { return this._hour; }
    set onHourChange(fn) { this._onHourChange = fn; }

    connectedCallback() {
        if (!this._initialized) {
            this._initVendors();
            this._initDays();
            this._initDates();
            this._initDial();
            this._initialized = true;
        } else {
            this._syncVendors(filterService.vendors);
            this._syncDays(filterService.daysOfWeek);
            this._onExternalDateChange(filterService.dateRange);
            this._drawDial();
        }

        this._unsubDate    = filterService.addListener('date',    (r) => this._onExternalDateChange(r));
        this._unsubVendors = filterService.addListener('vendors', (v) => this._syncVendors(v));
        this._unsubDays    = filterService.addListener('days',    (d) => this._syncDays(d));
    }

    disconnectedCallback() {
        if (this._unsubDate)    { this._unsubDate();    this._unsubDate    = null; }
        if (this._unsubVendors) { this._unsubVendors(); this._unsubVendors = null; }
        if (this._unsubDays)    { this._unsubDays();    this._unsubDays    = null; }
    }

    // ── Vendors ──────────────────────────────────────────────────────────────
    async _initVendors() {
        try {
            const vendors = await vendorService.load();
            this.vendorGrid.innerHTML = '';
            Object.entries(vendors).forEach(([key, value]) => {
                const btn = document.createElement('button');
                btn.className = 'vbtn';
                btn.dataset.id = key;
                btn.textContent = value;
                this.vendorGrid.appendChild(btn);
            });
            this._syncVendors(filterService.vendors);
        } catch (e) {
            console.error('DemandHourlyControlPanel: vendor error', e);
        }

        this.vendorGrid.addEventListener('click', (e) => {
            const b = e.target.closest('.vbtn');
            if (b) filterService.selectVendor(b.dataset.id);
        });

        this.clearVendorsBtn.addEventListener('click', () => {
            filterService.selectVendor(null);
        });
    }

    _syncVendors(vendors) {
        this.vendorGrid.querySelectorAll('.vbtn').forEach(b => {
            b.classList.toggle('active', !!(vendors && vendors.has(b.dataset.id)));
        });

        if (vendors && vendors.size > 0) {
            this.clearVendorsBtn.classList.add('visible');
        } else {
            this.clearVendorsBtn.classList.remove('visible');
        }
    }

    // ── Days of Week ─────────────────────────────────────────────────────────
    _initDays() {
        this.daysGrid.addEventListener('click', (e) => {
            const b = e.target.closest('.vbtn');
            if (b) filterService.selectDayOfWeek(b.dataset.id);
        });

        this.clearDaysBtn.addEventListener('click', () => {
            filterService.selectDayOfWeek(null);
        });

        this._syncDays(filterService.daysOfWeek);
    }

    _syncDays(days) {
        this.daysGrid.querySelectorAll('.vbtn').forEach(b => {
            b.classList.toggle('active', !!(days && days.has(Number(b.dataset.id))));
        });

        if (days && days.size > 0) {
            this.clearDaysBtn.classList.add('visible');
        } else {
            this.clearDaysBtn.classList.remove('visible');
        }
    }

    // ── Dates ─────────────────────────────────────────────────────────────────
    async _initDates() {
        try {
            const range = await dataRangeservice.load();
            this._minDate = range.min;
            this._maxDate = range.max;
            // Push initial full range
            filterService.selectDateRange({ min: this._minDate, max: this._maxDate });
            this._markRange('all');
        } catch (e) {
            console.error('DemandHourlyControlPanel: date error', e);
        }

        this.startInput.addEventListener('change', () => this._handleManualDate());
        this.endInput.addEventListener('change',   () => this._handleManualDate());

        this.rangeBtns.forEach(b => {
            b.addEventListener('click', (e) => this._applyRange(e.target.dataset.unit));
        });
    }

    _onExternalDateChange(range) {
        if (!range) return;
        this.startInput.value = this._toDateStr(range.min);
        this.endInput.value   = this._toDateStr(range.max);
    }

    _handleManualDate() {
        this._markRange(null);
        const s = new Date(this.startInput.value + 'T00:00:00');
        const e = new Date(this.endInput.value   + 'T23:59:59');
        if (!isNaN(s) && !isNaN(e)) filterService.selectDateRange({ min: s, max: e });
    }

    _applyRange(unit) {
        this._markRange(unit);
        let { min: s, max: e } = filterService.dateRange || { min: this._minDate, max: this._maxDate };
        if (!s) return;

        if (unit === 'all') {
            s = new Date(this._minDate);
            e = new Date(this._maxDate);
        } else {
            s = new Date(s);
            e = this._shift(new Date(s), 1, unit);
        }
        filterService.selectDateRange({ min: s, max: e });
    }

    _shift(d, n, unit) {
        if (unit === 'h') d.setHours(d.getHours() + n);
        else if (unit === 'd') d.setDate(d.getDate() + n);
        else if (unit === 'w') d.setDate(d.getDate() + n * 7);
        else if (unit === 'm') d.setMonth(d.getMonth() + n);
        else if (unit === 'y') d.setFullYear(d.getFullYear() + n);
        return d;
    }

    _markRange(unit) {
        this.rangeBtns.forEach(b => b.classList.toggle('active', b.dataset.unit === unit));
    }

    _toDateStr(d) {
        const dt = d instanceof Date ? d : new Date(d);
        const y  = dt.getFullYear();
        const m  = String(dt.getMonth() + 1).padStart(2, '0');
        const dy = String(dt.getDate()).padStart(2, '0');
        return `${y}-${m}-${dy}`;
    }

    // ── Dial ──────────────────────────────────────────────────────────────────
    _initDial() {
        this._drawDial();

        this.dialWrap.addEventListener('wheel', (e) => {
            e.preventDefault();
            this._setHour((this._hour + (e.deltaY > 0 ? -1 : 1) + 24) % 24);
        }, { passive: false });

        this.dialWrap.addEventListener('mousedown', (e) => {
            this._dragging = true;
            this._lastAngle = this._angleFrom(e.clientX, e.clientY);
            this._accumulator = 0;
        });
        window.addEventListener('mousemove', (e) => {
            if (this._dragging) this._drag(e.clientX, e.clientY);
        });
        window.addEventListener('mouseup', () => {
            this._dragging = false; this._lastAngle = null;
        });

        this.dialWrap.addEventListener('touchstart', (e) => {
            if (e.touches.length !== 1) return;
            this._dragging = true;
            this._lastAngle = this._angleFrom(e.touches[0].clientX, e.touches[0].clientY);
            this._accumulator = 0;
        }, { passive: true });
        this.dialWrap.addEventListener('touchmove', (e) => {
            if (!this._dragging || e.touches.length !== 1) return;
            e.preventDefault();
            this._drag(e.touches[0].clientX, e.touches[0].clientY);
        }, { passive: false });
        this.dialWrap.addEventListener('touchend', () => {
            this._dragging = false; this._lastAngle = null;
        });
    }

    _angleFrom(cx, cy) {
        const r = this.dialCanvas.getBoundingClientRect();
        return Math.atan2(cy - (r.top + r.height / 2), cx - (r.left + r.width / 2));
    }

    _drag(clientX, clientY) {
        const angle = this._angleFrom(clientX, clientY);
        if (this._lastAngle === null) { this._lastAngle = angle; return; }
        let delta = angle - this._lastAngle;
        if (delta >  Math.PI) delta -= 2 * Math.PI;
        if (delta < -Math.PI) delta += 2 * Math.PI;
        this._lastAngle = angle;
        this._accumulator += delta;

        const step = (2 * Math.PI) / 24;
        const steps = Math.round(this._accumulator / step);
        if (steps !== 0) {
            this._accumulator -= steps * step;
            this._setHour((this._hour + steps + 24) % 24);
        }
    }

    _setHour(h) {
        this._hour = h;
        this.dialDisplay.textContent = String(h).padStart(2, '0');
        this._drawDial();
        if (this._onHourChange) this._onHourChange(h);
    }

    _drawDial() {
        const canvas = this.dialCanvas;
        const ctx = canvas.getContext('2d');
        const W = canvas.width, H = canvas.height;
        const cx = W / 2, cy = H / 2;
        const R  = Math.min(W, H) / 2 - 12;

        ctx.clearRect(0, 0, W, H);

        // Read CSS vars from :root (shadow DOM can't easily read host vars in canvas)
        const style       = getComputedStyle(document.documentElement);
        const textColor   = style.getPropertyValue('--app-text').trim()         || '#1f2937';
        const bgSecondary = style.getPropertyValue('--app-bg-secondary').trim() || '#e5e7eb';
        const accent      = style.getPropertyValue('--app-text-accent').trim()  || '#2563eb';

        const START = -Math.PI / 2; // 12 o'clock

        // Track ring
        ctx.beginPath();
        ctx.arc(cx, cy, R, 0, Math.PI * 2);
        ctx.strokeStyle = bgSecondary;
        ctx.lineWidth   = 20;
        ctx.stroke();

        // Filled arc for current hour
        if (this._hour > 0) {
            const endAngle = START + (this._hour / 24) * Math.PI * 2;
            ctx.beginPath();
            ctx.arc(cx, cy, R, START, endAngle);
            ctx.strokeStyle = accent;
            ctx.lineWidth   = 20;
            ctx.lineCap     = 'round';
            ctx.stroke();
        }

        // Tick marks + major labels
        for (let h = 0; h < 24; h++) {
            const angle  = START + (h / 24) * Math.PI * 2;
            const isMaj  = h % 6 === 0;
            const inner  = isMaj ? R - 24 : R - 16;

            ctx.beginPath();
            ctx.moveTo(cx + Math.cos(angle) * inner, cy + Math.sin(angle) * inner);
            ctx.lineTo(cx + Math.cos(angle) * (R - 10), cy + Math.sin(angle) * (R - 10));
            ctx.strokeStyle = isMaj ? textColor : bgSecondary;
            ctx.lineWidth   = isMaj ? 3 : 1.5;
            ctx.lineCap     = 'round';
            ctx.stroke();

            if (isMaj) {
                const lr = R - 42;
                ctx.font         = `bold ${W * 0.065}px system-ui`;
                ctx.fillStyle    = textColor;
                ctx.textAlign    = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(String(h).padStart(2, '0'), cx + Math.cos(angle) * lr, cy + Math.sin(angle) * lr);
            }
        }

        // Needle
        const needleAngle = START + (this._hour / 24) * Math.PI * 2;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(cx + Math.cos(needleAngle) * (R - 26), cy + Math.sin(needleAngle) * (R - 26));
        ctx.strokeStyle = accent;
        ctx.lineWidth   = 3.5;
        ctx.lineCap     = 'round';
        ctx.stroke();

        // Center dot
        ctx.beginPath();
        ctx.arc(cx, cy, 7, 0, Math.PI * 2);
        ctx.fillStyle = accent;
        ctx.fill();
    }
}

customElements.define('demand-hourly-control-panel', DemandHourlyControlPanel);
