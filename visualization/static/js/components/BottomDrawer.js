export class BottomDrawer extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.isOpen = false;
    }

    connectedCallback() {
        this.render();
        this.addEventListeners();
    }

    static get observedAttributes() { return ['title', 'open']; }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name === 'title') {
            this._title = newValue;
            this.render();
            this.addEventListeners();
        }
    }

    addEventListeners() {
        const btn = this.shadowRoot.querySelector('.toggle-btn');
        const panel = this.shadowRoot.querySelector('.panel');
        const overlay = this.shadowRoot.querySelector('.overlay');
        const icon = this.shadowRoot.querySelector('.icon');

        const togglePanel = () => {
            this.isOpen = !this.isOpen;
            
            if (this.isOpen) {
                panel.classList.remove('collapsed');
                overlay.classList.add('visible');
                icon.style.transform = 'rotate(0deg)'; 
            } else {
                panel.classList.add('collapsed');
                overlay.classList.remove('visible');
                icon.style.transform = 'rotate(180deg)'; 
            }
        };

        btn.addEventListener('click', togglePanel);
        overlay.addEventListener('click', () => {
            if (this.isOpen) togglePanel();
        });

        if (this.isOpen) {
            panel.classList.remove('collapsed');
            overlay.classList.add('visible');
            icon.style.transform = 'rotate(0deg)'; 
        } else {
            panel.classList.add('collapsed');
            overlay.classList.remove('visible');
            icon.style.transform = 'rotate(180deg)'; 
        }
    }

    getHandleWidth() {
        const charWidth = 9; // Approximate px per character
        const padding = 90;  // Space for the curves and icon
        return Math.max(160, (this._title.length * charWidth) + padding);
    }

    render() {
        const handleWidth = this.getHandleWidth();
        const curveWidth = 40; // The 'sigmoid' transition width

        this.shadowRoot.innerHTML = `
            <style>

                :host {
                    position: absolute;
                    bottom: 0; left: 0; width: 100%;
                    z-index: 10;
                    pointer-events: none;
                }

                .overlay {
                    position: fixed; /* Cover the whole viewport */
                    top: 0; left: 0; width: 100vw; height: 100vh;
                    background-color: rgba(0, 0, 0, 0.3);
                    backdrop-filter: blur(2px);

                    opacity: 0;
                    visibility: hidden;
                    transition: opacity 0.3s ease, visibility 0.3s ease;
                    z-index: -1;
                    pointer-events: none;
                }

                .overlay.visible {
                    opacity: 1;
                    visibility: visible;
                    pointer-events: auto; /* Capture clicks when visible */
                }

                .panel {
                    background-color: var(--app-bg);
                    color: var(--app-text);
                    padding: 20px;
                    box-sizing: border-box;
                    transition: transform 0.3s cubic-bezier(0.4, 0.0, 0.2, 1);
                    transform: translateY(0);
                    pointer-events: auto;
                    min-height: 150px;
                    filter: drop-shadow(0 -3px 10px rgba(0,0,0,0.1));
                    position: relative;
                }

                .panel.collapsed {
                    transform: translateY(100%);
                }

                .toggle-btn {
                    position: absolute;
                    top: -34px; 
                    left: 50%;
                    transform: translateX(-50%);
                    width: ${handleWidth}px; /* Dynamic width */
                    height: 35px;
                    background: transparent;
                    border: none;
                    cursor: pointer;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                }

                .handle-shape {
                    position: absolute;
                    top: 0; left: 0;
                    width: 100%;
                    height: 100%;
                    fill: var(--app-bg);
                    /* Filter helps the shadow follow the curve */
                    filter: drop-shadow(0 -5px 4px rgba(0,0,0,0.1));
                    z-index: 11; 
                }

                .content-wrapper {
                    position: relative;
                    z-index: 12;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding-top: 4px;
                    color: var(--app-text);
                }

                .label {
                    font-family: sans-serif;
                    font-size: 0.9rem;
                    font-weight: 600;
                    color: var(--app-text);
                    user-select: none;
                    padding-left: 24px;
                    padding-bottom: 2px;
                    z-index: 12;
                }

                .icon {
                    width: 20px;
                    height: 20px;
                    fill: var(--app-text);
                    transition: transform 0.3s ease;
                    z-index: 12;
                }

            </style>

            <div class="overlay ${this.isOpen ? 'visible' : ''}"></div>

            <div class="panel ${this.isOpen ? '' : 'collapsed'}">
                <button class="toggle-btn" aria-label="Toggle Panel">
                    <svg class="handle-shape" preserveAspectRatio="none" viewBox="0 0 ${handleWidth} 35">
                        <path d="
                            M0,35 
                            C${curveWidth/2},35 ${curveWidth/2},0 ${curveWidth},0 
                            L${handleWidth - curveWidth},0 
                            C${handleWidth - curveWidth/2},0 ${handleWidth - curveWidth/2},35 ${handleWidth},35 
                            Z" />
                    </svg>

                    <div class="content-wrapper">
                        <span class="label">${this._title}</span>
                        <svg class="icon" style="transform: rotate(${this.isOpen ? '0deg' : '180deg'})" viewBox="0 0 24 24">
                            <path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z"/>
                        </svg>
                    </div>
                </button>

                <slot></slot>
            </div>
        `;
    }
}

customElements.define("bottom-drawer", BottomDrawer);
