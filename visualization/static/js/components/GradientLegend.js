export class GradientLegend extends HTMLElement {
    constructor() {
        super();
        // Attach Shadow DOM for encapsulation
        this.attachShadow({ mode: 'open' });

        this.formatter = new Intl.NumberFormat('en-US', { 
            notation: "compact", 
            maximumFractionDigits: 1 
        });

        // Initialize the internal structure
        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    display: block;
                    position: absolute;
                    bottom: 20px;
                    right: 20px;
                    background: var(--app-bg);
                    padding: 15px;
                    border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.2);
                    font-family: sans-serif;
                    z-index: 1;
                    min-width: 100px;
                }
                .legend-title { 
                    font-weight: bold; 
                    margin-bottom: 10px; 
                    font-size: 14px;
                    color: var(--app-text-title);
                    text-transform: capitalize;
                }
                .legend-body { 
                    display: flex; 
                    height: 300px; 
                }
                .legend-bar { 
                    width: 20px; 
                    border-radius: 4px; 
                    border: 1px solid #ddd; 
                }
                .legend-markers {
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                    margin-left: 10px;
                    font-size: 12px;
                    color: var(--app-text);
                }
            </style>
            <div class="legend-title"></div>
            <div class="legend-body">
                <div class="legend-bar"></div>
                <div class="legend-markers"></div>
            </div>
        `;

        // Cache DOM references
        this.titleElement = this.shadowRoot.querySelector('.legend-title');
        this.barElement = this.shadowRoot.querySelector('.legend-bar');
        this.markersElement = this.shadowRoot.querySelector('.legend-markers');
    }

    // Observe the 'title' attribute so we can update it directly from HTML
    static get observedAttributes() {
        return ['title'];
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name === 'title' && oldValue !== newValue) {
            // Replace underscores with spaces for cleaner display
            this.titleElement.textContent = newValue.replace(/_/g, ' ');
        }
    }

    update(gradientInstance, min, max, absoluteMax = max) {
        this.barElement.style.background = gradientInstance.getCss(0);

        this.markersElement.innerHTML = '';

        const numMarkers = gradientInstance.steps ? gradientInstance.steps + 1 : 5;
        
        for (let i = 0; i < numMarkers; i++) {
            // Calculate value from Top (max) to Bottom (min)
            const fraction = 1 - (i / (numMarkers - 1));
            const value = min + (max - min) * fraction;

            const span = document.createElement("span");
            let formattedText = this.formatter.format(value);

            // If this is the top marker (i === 0) and we clamped the outliers
            if (i === 0 && absoluteMax > max) {
                formattedText += "+";
            }

            span.textContent = formattedText;
            this.markersElement.appendChild(span);
        }
    }
}

// Register the custom element with the browser
customElements.define('gradient-legend', GradientLegend);
