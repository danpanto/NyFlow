export class ZoneInfo extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this._heading = "";
        this._data = null; 
        this._visible = true;
    }

    get heading() {
        return this._heading;
    }

    get visible() {
        return this._visible;
    }

    set visible(value) {
        const isVisible = !!value; 

        if(isVisible === this._visible) return;

        this._visible = isVisible;

        if (this._visible) {
            this.style.display = 'block';
        } else {
            this.style.display = 'none';
        }
    }

    set heading(value) {
        if(value === this._heading) return;
        this._heading = value;
        this.render();
    }

    get data() {
        return this._data;
    }

    set data(value) {
        if(value === this._data) return;
        this._data = value;
        this.render();
    }

    render() {
        if(!this._data && !this._heading) {
            this.shadowRoot.innerHTML = "";
            return;
        }

        let dataRows = "";
        if (this._data) {
            for (const [key, value] of Object.entries(this._data)) {
                dataRows += `<p><span>${key}:</span> ${value}</p>`;
            }
        }

        // Conditionally render the heading
        const headingElement = this._heading ? `<h3>${this._heading}</h3>` : "";

        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    position: absolute;
                    bottom: 20px;
                    right: 20px;
                    z-index: 1;

                    display: block;
                    padding: 15px;
                    background-color: var(--app-bg);
                    border-radius: 10px;

                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); 
                    font-family: system-ui, sans-serif;
                }
                
                p {
                    margin: 5px 0;
                    font-size: 14px;
                    color: var(--app-text);
                }
                
                span {
                    font-weight: bold;
                    color: var(--app-text);
                }

                h3 {
                    margin-top: 0;
                    font-size: 16px;
                    text-align: center;
                    color: var(--app-text-title);
                }
            </style>
            
            <div>
                ${headingElement}
                ${dataRows}
            </div>
        `;
    }
}

customElements.define('zone-info', ZoneInfo);
