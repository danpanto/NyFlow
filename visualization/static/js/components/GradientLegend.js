import { VARIABLE_CONFIG } from "../queryVariables.js";

export class GradientLegend extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });

        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    display: block;
                    background: var(--app-bg);
                    padding: 15px 25px 15px 15px; 
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                    font-family: sans-serif;
                    z-index: 1;
                    min-width: 100px;
                }
                .legend-title { 
                    font-weight: bold; 
                    margin-bottom: 25px; 
                    font-size: 16px;
                    color: var(--app-text-title);
                }
                .legend-body { 
                    display: flex; 
                    height: 300px; 
                    margin-bottom: 10px; 
                }
                .legend-bar { 
                    width: 20px; 
                    border-radius: 4px; 
                    border: 1px solid #ddd; 
                }
                .legend-markers {
                    position: relative; 
                    flex-grow: 1;
                    /* Increased gap to fit the tick lines */
                    margin-left: 18px; 
                }
                .legend-markers span {
                    position: absolute; 
                    left: 0;
                    /* Centers the text perfectly on the mathematical coordinate */
                    transform: translateY(50%); 
                    line-height: 1; 
                    font-size: 13px;
                    color: var(--app-text);
                    white-space: nowrap;
                    display: flex;
                    align-items: center;
                }
                /* Adds a tiny precision tick-mark connecting text to the bar */
                .legend-markers span::before {
                    content: '';
                    position: absolute;
                    left: -14px;
                    width: 8px;
                    height: 1px;
                    background-color: var(--app-text);
                    opacity: 0.3;
                }
            </style>            <div class="legend-title"></div>
            <div class="legend-body">
                <div class="legend-bar"></div>
                <div class="legend-markers"></div>
            </div>
        `;

        this.titleElement = this.shadowRoot.querySelector('.legend-title');
        this.barElement = this.shadowRoot.querySelector('.legend-bar');
        this.markersElement = this.shadowRoot.querySelector('.legend-markers');
    }

    static get observedAttributes() {
        return ['layer-id'];
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name === 'layer-id' && oldValue !== newValue) {
            const config = VARIABLE_CONFIG[newValue] || {};
            this.titleElement.textContent = config.shortName || newValue.replace(/_/g, ' ');
            this.currentLayerId = newValue;
        }
    }

    update(gradientInstance, min, max, absoluteMax = max) {
        this.barElement.style.background = gradientInstance.getCss(0);
        this.markersElement.innerHTML = '';

        const config = VARIABLE_CONFIG[this.currentLayerId] || {};
        const fallbackFormatter = new Intl.NumberFormat('en-US', { notation: "compact", maximumFractionDigits: 1 });
        const formatValue = config.formatter || ((val) => fallbackFormatter.format(val));

        const range = max - min;

        // Fallback for flat data (e.g., if all zones on the map have exactly the same value)
        if (range <= 0) {
            const span = document.createElement("span");
            let text = formatValue(min);
            if (config.units && config.units !== "USD") text += ` ${config.units}`;
            span.textContent = text;
            span.style.bottom = "50%";
            this.markersElement.appendChild(span);
            return;
        }

        const numMarkers = 6; 
        
        const roughStep = range / (numMarkers - 1);
        const magnitude = Math.pow(10, Math.floor(Math.log10(roughStep || 1)));
        const normalizedStep = roughStep / magnitude;

        // 2. Strictly use 1, 2, or 5 to ensure perfect 1-decimal boundaries (prevents 0.25, 0.75, etc.)
        let niceMultiplier;
        if (normalizedStep < 1.5) niceMultiplier = 1;
        else if (normalizedStep < 3.5) niceMultiplier = 2; 
        else if (normalizedStep < 7.5) niceMultiplier = 5;
        else niceMultiplier = 10;

        const tickStep = niceMultiplier * magnitude;

        // 3. ONLY generate nice ticks that fit within the bounds
        const firstTick = Math.ceil(min / tickStep) * tickStep;
        const lastTick = Math.floor(max / tickStep) * tickStep;

        const ticks = [];
        for (let i = lastTick; i >= firstTick; i -= tickStep) {
            const val = parseFloat(i.toPrecision(10)); 
            const position = ((val - min) / range) * 100;
            ticks.push({ value: val, position });
        }

        // 3. Render Ticks to DOM
        ticks.forEach((tick, index) => {
            const span = document.createElement("span");
            
            let formattedText = formatValue(tick.value);

            // Add '+' to the very highest tick if data outliers were clamped
            if (index === 0 && absoluteMax > max) {
                formattedText += "+";
            }

            if (config.units && config.units !== "USD") {
                formattedText += ` ${config.units}`;
            }

            span.textContent = formattedText;
            
            // This places the text exactly on the Y-axis according to its true data value
            span.style.bottom = `${tick.position}%`;
            
            this.markersElement.appendChild(span);
        });
    }    
}

customElements.define('gradient-legend', GradientLegend);
