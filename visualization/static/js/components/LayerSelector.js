import { filterService } from "../services/FilterService.js";

export class LayerSelector extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    connectedCallback() {
        // Add listeners
        this.addEventListener('click', (event) => {
            const clickedBtn = event.target.closest('button');
            if (!clickedBtn || clickedBtn.parentNode !== this) return;

            this.selectLayer(clickedBtn.getAttribute('layer'));
        });

        // Get and select default button
        const defaultLayer = this.getAttribute('selected');
        const allButtons = Array.from(this.querySelectorAll('button'));

        if (defaultLayer) {
            this.selectLayer(defaultLayer);
        } else if (allButtons.length > 0) {
            this.selectLayer(allButtons[0].getAttribute('layer'));
        }

        this.render();
    }

    render() {
        this.shadowRoot.innerHTML = `
            <style>
                /* The grid container inside the Shadow DOM */
                .layer-container {
                    display: grid;
                    grid-template-columns: repeat(3, 1fr);
                    gap: 10px;
                    justify-content: center;
                    max-width: 900px;
                    margin: 0 auto;
                }

                /* Target the buttons passed into the slot from your HTML */
                ::slotted(button) {
                    padding: 10px;
                    background: var(--app-bg);
                    cursor: pointer;
                    font-family: sans-serif;
                    font-size: 1.1rem;
                    font-weight: 800;
                    border: 0;
                    border-radius: 10px;
                    transition: background 0.2s, color 0.2s;
                    text-align: center;
                    color: var(--app-text);
                }

                ::slotted(button:hover) {
                    background: var(--app-bg-secondary);
                    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
                }

                ::slotted(button.active) {
                    background: var(--app-bg-secondary);
                    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
                }
            </style>
            
            <div class="layer-container">
                <slot></slot>
            </div>
        `;
    }


    // Reusable method to handle the visual update and event firing
    selectLayer(layerId) {
        const allButtons = Array.from(this.querySelectorAll('button'));
        const targetBtn = allButtons.find(btn => btn.getAttribute('layer') === layerId);

        if (!targetBtn) return;

        // Update visuals
        allButtons.forEach(btn => btn.classList.remove('active'));
        targetBtn.classList.add('active');
        filterService.selectLayer(layerId);
    }
}

customElements.define('layer-selector', LayerSelector);
