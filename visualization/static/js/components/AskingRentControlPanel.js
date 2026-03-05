import { filterService } from '../services/FilterService.js';

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
        
        .date-label {
            text-align: center;
            font-weight: bold;
            color: var(--app-text-accent, #2563eb);
            font-size: 1.25rem;
        }
        
        input[type="range"] {
            width: 100%;
            cursor: pointer;
            accent-color: var(--app-text-accent, #2563eb);
        }
    </style>
    <div class="section">
        <h3>Month Selection</h3>
        <div class="slider-container">
            <div class="date-label" id="date-label">Jan 2026</div>
            <input type="range" id="month-slider" min="0" max="192" value="192">
        </div>
    </div>
`;

export class AskingRentControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));

        this.slider = this.shadowRoot.getElementById('month-slider');
        this.label = this.shadowRoot.getElementById('date-label');
    }

    connectedCallback() {
        this.slider.addEventListener('input', () => this.updateDate(true));
        this.slider.addEventListener('wheel', (e) => {
            e.preventDefault();
            if (e.deltaY < 0) {
                this.slider.value = Math.min(192, parseInt(this.slider.value) + 1);
            } else {
                this.slider.value = Math.max(0, parseInt(this.slider.value) - 1);
            }
            this.updateDate(true);
        });

        // Let's set initial value
        this.updateDate(false);
    }

    updateDate(triggerFilter) {
        const monthsSince2010 = parseInt(this.slider.value);
        const year = 2010 + Math.floor(monthsSince2010 / 12);
        const month = (monthsSince2010 % 12) + 1;

        const monthStr = month.toString().padStart(2, '0');
        const displayDate = new Date(year, month - 1).toLocaleString('default', { month: 'short', year: 'numeric' });

        this.label.textContent = displayDate;

        if (triggerFilter) {
            const startStr = `${year}-${monthStr}-01T00:00:00`;
            const startDate = new Date(startStr);
            const endDate = new Date(year, month, 0, 23, 59, 59); // Last day of month

            filterService.selectDateRange({
                min: startDate,
                max: endDate
            });
        }
    }
}

customElements.define('asking-rent-control-panel', AskingRentControlPanel);
