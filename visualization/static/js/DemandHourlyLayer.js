import { BaseLayer } from "./BaseLayer.js";
import { ZoneController } from "./ZoneController.js";
import { filterService } from "./services/FilterService.js";
import { zoneData } from "./services/ZoneDataService.js";
import { LRUCache } from "./LRUCache.js";
import "./components/DemandHourlyControlPanel.js";

// ─── Categorical color palette ─────────────────────────────────────────────────
// Inspired by the warm-to-hot palette used by the AGG DATA FROM TLC layers
const DEMAND_COLORS = {
    null:   { fill: '#111111', border: 'transparent', opacity: 0.50 }, // no data
    low:    { fill: '#7c3aed', border: 'white',       opacity: 0.65 }, // purple
    medium: { fill: '#f97316', border: 'white',       opacity: 0.70 }, // orange
    high:   { fill: '#eab308', border: 'white',       opacity: 0.80 }, // yellow
};

// ─── Controller ────────────────────────────────────────────────────────────────
class DemandHourlyController extends ZoneController {
    constructor(backend) {
        super(backend);
        this.currentData   = null;
        this.normalOpacity = 0.65;
        this.hoverOpacity  = 0.90;
        this.selectOpacity = 1.00;
        this._init();
    }

    async _init() {
        await zoneData.load();
        if (this._visible) this.backend.refresh();
    }

    update(data) {
        super.update(data);
        if (data.query !== undefined) {
            this.currentData = data.query;
            this.backend.refresh();
        }
    }

    _colorFor(id) {
        if (!this.currentData || this.currentData[id] === undefined) return DEMAND_COLORS.null;
        return DEMAND_COLORS[this.currentData[id]] ?? DEMAND_COLORS.null;
    }

    getStyle(id) {
        const c = this._colorFor(id);
        const isSelected = filterService.isSelectedZone(id);
        return {
            color:       c.border,
            fillColor:   c.fill,
            fillOpacity: isSelected ? this.selectOpacity : c.opacity,
            weight:      1,
        };
    }

    onHover(e, id) {
        if (filterService.isSelectedZone(id)) return;
        e.target.setStyle({ fillOpacity: this.hoverOpacity });
    }

    onUnhover(e, id) {
        if (filterService.isSelectedZone(id)) return;
        e.target.setStyle(this.getStyle(id));
    }

    onClick(e, id) {
        if (filterService.layer === 'routes') return;
        const isMulti = e.originalEvent.ctrlKey || e.originalEvent.metaKey;
        const isSel   = filterService.isSelectedZone(id);
        filterService.selectZone(id, isMulti ? !isSel : true, !isMulti);
        this.backend.refresh();
    }
}

// ─── Layer ─────────────────────────────────────────────────────────────────────
export class DemandHourlyLayer extends BaseLayer {
    constructor(mapManager, backend) {
        super();

        this.name         = 'demand_hourly';
        this.mapManager   = mapManager;
        this.baseAppName  = 'NyFlow';
        this.data         = null;

        // LRU cache — key: JSON.stringify(payload), value: response data object
        this.cache = new LRUCache(100);

        // Zone controller
        this.mapController = new DemandHourlyController(backend);
        mapManager.addLayer(this.name, this.mapController);

        // Control panel — a single web component with vendor + dates + dial
        this.controlPanel = document.createElement('demand-hourly-control-panel');

        // Hover zone info card + categorical legend wrapper
        this.zoneInfoDiv = document.createElement('zone-info');
        Object.assign(this.zoneInfoDiv.style, { position: 'relative', inset: 'auto' });

        this.uiWrapper = document.createElement('div');
        Object.assign(this.uiWrapper.style, {
            position:   'absolute',
            bottom:     '20px',
            right:      '20px',
            display:    'flex',
            gap:        '20px',
            alignItems: 'flex-end',
            zIndex:     '1',
        });
        this.uiWrapper.appendChild(this.zoneInfoDiv);
        this.uiWrapper.appendChild(this._buildLegend());

        // Subscriptions
        this._unsubZones  = null;
        this._unsubDate   = null;
        this._unsubVendor = null;
    }

    // ── Categorical legend ────────────────────────────────────────────────────
    _buildLegend() {
        const legend = document.createElement('div');
        Object.assign(legend.style, {
            background:   'var(--app-bg, #fff)',
            borderRadius: '12px',
            boxShadow:    '0 4px 16px rgba(0,0,0,0.15)',
            padding:      '12px 16px',
            fontFamily:   'system-ui, sans-serif',
            fontSize:     '0.8rem',
            border:       '1px solid var(--map-bg, #e5e7eb)',
            minWidth:     '130px',
        });

        const title = document.createElement('div');
        Object.assign(title.style, {
            fontWeight:    '700',
            fontSize:      '0.68rem',
            textTransform: 'uppercase',
            letterSpacing: '0.08em',
            opacity:       '0.55',
            marginBottom:  '8px',
            color:         'var(--app-text-title, #111)',
        });
        title.textContent = 'Demand Level';
        legend.appendChild(title);

        [               
            { label: 'High',    color: DEMAND_COLORS.high.fill   },
            { label: 'Medium',  color: DEMAND_COLORS.medium.fill },
            { label: 'Low',     color: DEMAND_COLORS.low.fill    },
            { label: 'No data', color: DEMAND_COLORS.null.fill   },
        ].forEach(({ label, color }) => {
            const row    = document.createElement('div');
            const swatch = document.createElement('span');
            const lbl    = document.createElement('span');

            Object.assign(row.style,    { display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '5px' });
            Object.assign(swatch.style, { display: 'inline-block', width: '14px', height: '14px', borderRadius: '3px', background: color, flexShrink: '0' });
            lbl.style.color = 'var(--app-text, #1f2937)';
            lbl.textContent = label;

            row.appendChild(swatch);
            row.appendChild(lbl);
            legend.appendChild(row);
        });

        return legend;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    bind() {
        document.body.appendChild(this.controlPanel);
        document.body.appendChild(this.uiWrapper);

        document.title = `${this.baseAppName} • Demand Hourly`;
        const titleEl = document.querySelector('.layer-title-text');
        if (titleEl) titleEl.textContent = '• Demand Hourly';

        this.mapManager.toggleLayer(this.name, true);

        // Wire up hour dial callback
        this.controlPanel.onHourChange = () => this._fetchData();

        this._unsubZones  = filterService.addListener('zones',   () => this._updateZoneInfo(filterService.lastZone));
        this._unsubDate   = filterService.addListener('date',    () => this._fetchData());
        this._unsubVendor = filterService.addListener('vendors', () => this._fetchData());

        // Initial fetch once date range is set (small delay to let controlPanel init run)
        setTimeout(() => this._fetchData(), 50);
    }

    unbind() {
        this.controlPanel.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.name, false);

        const titleEl = document.querySelector('.layer-title-text');
        if (titleEl) titleEl.textContent = '';
        document.title = this.baseAppName;

        if (this._unsubZones)  { this._unsubZones();  this._unsubZones  = null; }
        if (this._unsubDate)   { this._unsubDate();   this._unsubDate   = null; }
        if (this._unsubVendor) { this._unsubVendor(); this._unsubVendor = null; }
    }

    // ── Data fetching (with LRU cache) ────────────────────────────────────────
    async _fetchData() {
        const dateRange = filterService.dateRange;
        if (!dateRange?.min || !dateRange?.max) return;

        const fmt = (d) => (d instanceof Date ? d : new Date(d)).toISOString().split('.')[0];

        const payload = {
            vendors: Array.from(filterService.vendors ?? []),
            date:    { min: fmt(dateRange.min), max: fmt(dateRange.max) },
            hour:    this.controlPanel.hour,
        };

        const cacheKey = JSON.stringify(payload);

        // 1. Try cache first
        const cached = this.cache.get(cacheKey);
        if (cached) {
            this._applyData(cached);
            return;
        }

        // 2. Fetch from API
        try {
            const res  = await fetch('/api/hourly-demand-classification', {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify(payload),
            });
            const json = await res.json();

            if (json.status === 'ok') {
                this.cache.set(cacheKey, json.data);
                this._applyData(json.data);
            } else {
                console.warn('DemandHourlyLayer: API returned error', json);
            }
        } catch (e) {
            console.error('DemandHourlyLayer: fetch failed', e);
        }
    }

    _applyData(data) {
        this.data = data;
        this.mapController.update({ query: data });
        this._updateZoneInfo(filterService.lastZone);
    }

    // ── Zone info hover card ──────────────────────────────────────────────────
    _updateZoneInfo(zone) {
        if (!zone || !this.data) { this.zoneInfoDiv.visible = false; return; }
        const cls = this.data[zone];
        if (cls === undefined)   { this.zoneInfoDiv.visible = false; return; }

        this.zoneInfoDiv.visible  = true;
        this.zoneInfoDiv.heading  = zoneData.getName(zone);
        this.zoneInfoDiv.data     = {
            'Zone ID': zone,
            'Demand':  cls ? cls.charAt(0).toUpperCase() + cls.slice(1) : '—',
        };
    }
}
