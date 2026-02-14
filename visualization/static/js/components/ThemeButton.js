import { themeService } from "../services/ThemeService.js";

export class ThemeButton extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this._unsubscribe = null;
    }

    static get observedAttributes() { return ['theme']; }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name === 'theme' && oldValue !== newValue) {
            this.updateButtonState();
        }
    }

    get theme() { return this.getAttribute('theme') || 'light'; }
    set theme(val) { this.setAttribute('theme', val); }

    connectedCallback() {
        this.render(); 

        this.shadowRoot.querySelector('button')
            .addEventListener('click', () => themeService.toggle());

        // Listen for clicks
        this.shadowRoot.querySelector('button')
            .addEventListener('click', () => this.toggleTheme());

        // Add listener for theme changes
        this._unsubscribe = themeService.addListener((theme) => {
            this.theme = theme; 
        });
    }

    disconnectedCallback() {
        if (this._unsubscribe) this._unsubscribe();
    }

    updateButtonState() {
        const btn = this.shadowRoot.querySelector('button');
        if (btn) {
            btn.setAttribute('aria-label', `Switch to ${this.theme === 'light' ? 'dark' : 'light'} mode`);
        }
    }

    render() {
        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    display: inline-block;
                    /* Allow the parent to control size, or default to 40px */
                    --btn-size: var(--theme-btn-size, 50px);
                    --icon-size: calc(var(--btn-size) * 0.5);
                    --transition-speed: var(--theme-btn-transition, 0.5s);

                    /* Adaptive colors based on your global system */
                    --bg-color: var(--app-bg, #e5e7eb);
                    --hover-color: var(--app-bg-secondary, #e5e7eb);
                    --icon-color: var(--app-text, #1f2937);
                    --hover-bg: var(--app-bg, #f3f4f6);

                    width: var(--btn-size);
                    height: var(--btn-size);
                }

                button {
                    width: 100%;
                    height: 100%;
                    border-radius: 50%;
                    border: none;
                    cursor: pointer;
                    padding: 0;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    overflow: hidden; 
                    position: relative;
                    background-color: var(--bg-color);
                    color: var(--icon-color);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                    transition: 
                        background-color var(--transition-speed) ease, 
                        color var(--transition-speed) ease, 
                        transform 0.2s cubic-bezier(0.4, 0, 0.2, 1),
                        box-shadow 0.2s ease;
                }

                button:hover { 
                    background: var(--hover-color);
                }

                svg {
                    width: var(--icon-size);
                    height: var(--icon-size);
                    display: block;
                }

                .sun-core {
                    transition: transform var(--transition-speed) cubic-bezier(0.4, 0, 0.2, 1);
                    transform-origin: center center;
                }

                .rays {
                    transition: transform var(--transition-speed) ease, opacity var(--transition-speed) ease;
                    transform-origin: center center;
                    opacity: 1;
                }

                .shadow-overlay {
                    transition: transform var(--transition-speed) cubic-bezier(0.4, 0, 0.2, 1), fill var(--transition-speed) ease;
                    transform-origin: top right; 
                    fill: var(--bg-color);
                    transform: scale(0);
                }

                /* CSS automatically detects the attribute change on :host */
                :host([theme="dark"]) .sun-core { transform: scale(1.4); }
                :host([theme="dark"]) .rays { transform: scale(0) rotate(-45deg); opacity: 0; }
                :host([theme="dark"]) .shadow-overlay {
                    transform: scale(1);
                }


                button:hover .shadow-overlay { 
                    fill: var(--hover-color);
                }


            </style>

            <button type="button">
                <svg viewBox="0 0 24 24" fill="none" stroke="none">
                    <g class="rays" stroke="currentColor" stroke-width="2" stroke-linecap="round">
                        <line x1="12" y1="1" x2="12" y2="3" />
                        <line x1="12" y1="21" x2="12" y2="23" />
                        <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
                        <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
                        <line x1="1" y1="12" x2="3" y2="12" />
                        <line x1="21" y1="12" x2="23" y2="12" />
                        <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
                        <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
                    </g>
                    <circle class="sun-core" cx="12" cy="12" r="6" fill="currentColor" />
                    <circle class="shadow-overlay" cx="17" cy="8" r="7" />
                </svg>
            </button>
        `;
    }
}
customElements.define('theme-btn', ThemeButton);
