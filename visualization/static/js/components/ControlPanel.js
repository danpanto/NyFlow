import { TimeControlPanel } from "./TimeControlPanel.js";
import { VendorControlPanel } from "./VendorControlPanel.js";

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

        .section-divider {
            height: 1px;
            background-color: var(--app-text-secondary, #e5e7eb);
            margin: 0 20px; /* Leaves a nice gap on the edges */
        }

        /* Overrides for child components to make them blend in.
           We remove their internal padding and shadows so the 
           master panel handles the overall shape.
        */
        time-control-panel,
        layer-control-panel,
        settings-control-panel {
            width: 100%;
            display: block;
        }
    </style>

    <div class="section">
        <vendor-control-panel></vendor-control-panel>
    </div>

    <div class="section-divider"></div>

    <div class="section">
        <time-control-panel></time-control-panel>
    </div>
`;

export class ControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));
    }
}

customElements.define('control-panel', ControlPanel);
