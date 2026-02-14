import { zoneData } from "./services/ZoneDataService.js";

export class ZoneInfo extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this._zone = null;
    }

    get zone() {
        return this._zone;
    }

    set zone(value) {
        if(value === this._zone) return;
        this._zone = value;
        this.render();
    }

    render() {
        if(!this._zone) {
            this.shadowRoot.innerHTML = "";
            return;
        }

        const neighbourhood = zoneData.getBorough(this._zone);
        const name = zoneData.getName(this._zone)

        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    position: absolute;
                    top: 15px;
                    right: 15px;
                    z-index: 40;

                    display: block;
                    padding: 15px;
                    background-color: var(--app-bg);
                    border-radius: 10px;

                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); 
                    font-family: system-ui, sans-serif;
                }
                
                p {
                    margin: 5px 0;
                    color: var(--app-text);
                }
                
                span {
                    font-weight: bold;
                    color: var(--app-text);
                }

                h3 {
                    margin-top: 0;
                    text-align: center;
                    color: var(--app-text-title);
                }

            </style>
            
            <div>
                <h3>${name}</h3>
                <p><span>Zone ID:</span> ${this._zone}</p>
                <p><span>Neighbourhood:</span> ${neighbourhood}</p>
            </div>
        `;
    }
}

customElements.define('zone-info', ZoneInfo);
