import { filterService } from '../services/FilterService.js';
import { vendorService } from '../services/VendorService.js'; 

const template = document.createElement('template');
template.innerHTML = `
    <style>
        :host {
            display: block;
            width: 100%;
            box-sizing: border-box;
            font-family: system-ui, sans-serif;
            color: var(--app-text, #1f2937);
        }
        
        /* Flex container to align the clear button and title */
        .header {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 16px;
        }

        h3 { 
            margin: 0; 
            font-size: 1rem; 
            color: var(--app-text-title, #111827); 
        }

        /* Subtle Clear Button */
        .clear-btn {
            background: transparent;
            border: none;
            color: #94a3b8; /* Subtle gray */
            cursor: pointer;
            font-size: 1.1rem;
            line-height: 1;
            padding: 2px 6px;
            border-radius: 6px;
            transition: all 0.2s;
            /* Use visibility instead of display so the title doesn't shift when it appears */
            visibility: hidden; 
            opacity: 0;
        }

        .clear-btn.visible {
            visibility: visible;
            opacity: 1;
        }

        .clear-btn:hover {
            color: #ef4444; /* Subtle red alert on hover */
            background: #fee2e2;
        }

        .section-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: bold;
            color: var(--app-text-title, #111827);
            opacity: 0.7;
            margin-bottom: 8px;
        }

        .layer-container {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }

        .btn {
            background: var(--app-bg-secondary, #e5e7eb);
            border: 1px solid var(--map-bg, #d4dadc);
            color: var(--app-text, #1f2937);
            padding: 10px 8px;
            border-radius: 8px;
            font-size: 0.85rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            text-align: center;
        }

        .btn:hover { border-color: var(--app-text-accent, #2563eb); }
        
        .btn.active {
            background: var(--app-text-accent, #2563eb);
            color: #ffffff;
            border-color: var(--app-text-accent, #2563eb);
        }

        .btn:last-child:nth-child(odd) {
            grid-column: 1 / -1;
        }

        .loading-text {
            font-size: 0.85rem;
            color: #64748b;
            grid-column: span 2;
            text-align: center;
            padding: 12px 0;
        }
    </style>

    <div class="header">
        <h3>Service vendor filter</h3>
        <button id="clear-btn" class="clear-btn" title="Clear selection">✕</button>
    </div>

    <div class="layer-container" id="grid">
        <div class="loading-text">Loading layers...</div>
    </div>
`;
export class VendorControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));
        
        this.grid = this.shadowRoot.getElementById('grid');
        this.clearBtn = this.shadowRoot.getElementById('clear-btn');
        this.unsubscribeFilter = null;
    }

    connectedCallback() {
        this.init();

        this.grid.addEventListener('click', (e) => {
            const btn = e.target.closest('.btn');
            if (!btn) return;

            const vendorId = btn.dataset.id;
            filterService.selectVendor(vendorId);
        });

        this.clearBtn.addEventListener('click', () => {
            filterService.selectVendor(null);
        });
        this.unsubscribeFilter = filterService.addListener("vendors", (vendors) => this.syncVisuals(vendors));
    }

    disconnectedCallback() {
        if (this.unsubscribeFilter) {
            this.unsubscribeFilter();
            this.unsubscribeFilter = null;
        }
    }

    async init() {
        try {
            const vendors = await vendorService.load(); 
            this.renderButtons(vendors);

            const vendorKeys = Object.keys(vendors);

            if (filterService.vendors !== null && filterService.vendors !== undefined) {
                this.syncVisuals(filterService.vendors);
            } 
             
            else if (vendorKeys.length > 0) {
                filterService.selectVendor(vendorKeys[0]);
            }

        } catch (error) {
            console.error("Failed to load vendors:", error);
            this.grid.innerHTML = `<div class="loading-text">Failed to load vendors.</div>`;
        }
    }

    renderButtons(vendors) {
        this.grid.innerHTML = '';
        Object.entries(vendors).forEach(([key, value]) => {
            const btn = document.createElement('button');
            btn.className = 'btn';
            btn.dataset.id = key;
            btn.textContent = value;
            this.grid.appendChild(btn);
        });
    }

    syncVisuals(vendors) {
        if(!vendors) return;

        const buttons = this.shadowRoot.querySelectorAll('.btn');

        if (vendors.size > 0) {
            this.clearBtn.classList.add('visible');
        } else {
            this.clearBtn.classList.remove('visible');
        }

        buttons.forEach(btn => {
            if (vendors && vendors.has(btn.dataset.id)) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }
}

customElements.define('vendor-control-panel', VendorControlPanel);
