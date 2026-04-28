import { filterService } from '../services/FilterService.js';

const MIN_YEAR = 2011;
const MAX_YEAR = 2024;

const template = document.createElement('template');
template.innerHTML = `
    <style>
        :host {
            display: flex;
            flex-direction: column;
            width: 300px;
            background-color: var(--app-bg, #ffffff);
            border-radius: 16px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.15);
            font-family: system-ui, sans-serif;
            overflow: hidden;
            border: 1px solid var(--map-bg, #e5e7eb);
        }
        .section {
            padding: 16px 20px;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        h3 { margin: 0 0 16px 0; font-size: 1rem; color: var(--app-text-title, #111827); }

        .slider-container {
            display: flex;
            flex-direction: column;
            gap: 16px;
            padding-bottom: 8px;
        }

        .year-label {
            text-align: center;
            font-weight: bold;
            color: var(--app-text-accent, #2563eb);
            font-size: 1.5rem;
            letter-spacing: 2px;
        }

        input[type="range"] {
            width: 100%;
            cursor: pointer;
            accent-color: var(--app-text-accent, #2563eb);
        }

        .year-range {
            display: flex;
            justify-content: space-between;
            font-size: 0.75rem;
            opacity: 0.6;
            color: var(--app-text, #374151);
        }
    </style>
    <div class="section">
        <h3>Year Selection</h3>
        <div class="slider-container">
            <div class="year-label" id="year-label">2024</div>
            <input type="range" id="year-slider" min="${MIN_YEAR}" max="${MAX_YEAR}" value="${MAX_YEAR}">
            <div class="year-range">
                <span>${MIN_YEAR}</span>
                <span>${MAX_YEAR}</span>
            </div>
        </div>
    </div>
`;

export class HouseIncomeControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));

        this.slider = this.shadowRoot.getElementById('year-slider');
        this.label = this.shadowRoot.getElementById('year-label');
    }

    connectedCallback() {
        this.slider.addEventListener('input', () => this.updateYear(true));
        this.slider.addEventListener('wheel', (e) => {
            e.preventDefault();
            if (e.deltaY < 0) {
                this.slider.value = Math.min(MAX_YEAR, parseInt(this.slider.value) + 1);
            } else {
                this.slider.value = Math.max(MIN_YEAR, parseInt(this.slider.value) - 1);
            }
            this.updateYear(true);
        });

        // Set initial value without triggering filter
        this.updateYear(false);
    }

    get selectedYear() {
        return parseInt(this.slider.value);
    }

    updateYear(triggerFilter) {
        const year = parseInt(this.slider.value);
        this.label.textContent = year;

        if (triggerFilter) {
            // Encode the year as a full-year date range for the filter service
            const startDate = new Date(`${year}-01-01T00:00:00`);
            const endDate   = new Date(`${year}-12-31T23:59:59`);
            filterService.selectDateRange({ min: startDate, max: endDate });
        }
    }
}

customElements.define('house-income-control-panel', HouseIncomeControlPanel);
