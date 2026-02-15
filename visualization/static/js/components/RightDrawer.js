export class RightDrawer extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.isOpen = false;
        this._title = '';
    }

    connectedCallback() {
        this._title = this.getAttribute('title') || 'Drawer';
        this.render();
        this.addEventListeners();
    }

    static get observedAttributes() { return ['title', 'open']; }

    open() {
        if (this.isOpen) return; // 1. Guard: Stop if already open
        
        const panel = this.shadowRoot.querySelector('.panel');
        const icon = this.shadowRoot.querySelector('.icon');
        if (!panel || !icon) return;

        this.isOpen = true;
        panel.classList.remove('collapsed');
        icon.style.transform = 'rotate(0deg)';
        
        if (!this.hasAttribute('open')) {
            this.setAttribute('open', '');
        }
    }

    close() {
        if (!this.isOpen) return;
        
        const panel = this.shadowRoot.querySelector('.panel');
        const icon = this.shadowRoot.querySelector('.icon');
        if (!panel || !icon) return;

        this.isOpen = false;
        panel.classList.add('collapsed');
        icon.style.transform = 'rotate(180deg)';
        
        if (this.hasAttribute('open')) {
            this.removeAttribute('open');
        }
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (oldValue === newValue) return;

        if (name === 'title') {
            this._title = newValue;
            // FIX: Only update the label text, don't call render()
            const label = this.shadowRoot.querySelector('.label');
            if (label) {
                label.textContent = newValue;
            } else {
                // Fallback if called before first render
                this.render();
                this.addEventListeners();
            }
        }

        if (name === 'open') {
            const hasOpenAttr = newValue !== null;
            if (hasOpenAttr !== this.isOpen) {
                hasOpenAttr ? this.open() : this.close();
            }
        }
    }

    toggle() {
        this.isOpen ? this.close() : this.open();
    }

    addEventListeners() {
        const btn = this.shadowRoot.querySelector('.toggle-btn');
        btn.onclick = () => this.toggle();

        // Initialize state visually
        if (this.isOpen || this.hasAttribute('open')) {
            this.open();
        } else {
            this.close();
        }
    }

    getHandleHeight() {
        const charWidth = 9; 
        const padding = 90;  
        return Math.max(180, (this._title.length * charWidth) + padding);
    }

    render() {
        const handleHeight = this.getHandleHeight();
        const curveHeight = 40; 

        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    position: fixed;
                    top: 0; right: 0; height: 100vh;
                    z-index: 2;
                    pointer-events: none; 
                }

                .panel {
                    background-color: var(--app-bg, #1e1e1e);
                    color: var(--app-text, #fff);
                    width: var(--drawer-width, min(680px, 75vw));
                    height: 100%;
                    padding: 20px;
                    box-sizing: border-box;
                    transition: transform 0.3s cubic-bezier(0.4, 0.0, 0.2, 1);
                    transform: translateX(0);
                    pointer-events: auto; 
                    filter: drop-shadow(-5px 0 15px rgba(0,0,0,0.3)); 
                    position: absolute;
                    top: 0;
                    right: 0;
                }

                .panel.collapsed {
                    transform: translateX(100%);
                }

                .toggle-btn {
                    position: absolute;
                    left: -34px; 
                    top: 50%;
                    transform: translateY(-50%);
                    width: 35px;
                    height: ${handleHeight}px; 
                    background: transparent;
                    border: none;
                    cursor: pointer;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    pointer-events: auto;
                }

                .handle-shape {
                    position: absolute;
                    top: 0; left: 0;
                    width: 100%;
                    height: 100%;
                    fill: var(--app-bg, #1e1e1e);
                    filter: drop-shadow(-5px 0 4px rgba(0,0,0,0.1));
                    z-index: 11; 
                }

                .content-wrapper {
                    position: relative;
                    z-index: 12;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    gap: 8px;
                    color: var(--app-text, #fff);
                    width: ${handleHeight}px;
                    height: 35px;
                    transform: rotate(-90deg);
                }

                .label {
                    font-family: sans-serif;
                    font-size: 0.9rem;
                    font-weight: 600;
                    user-select: none;
                    white-space: nowrap;
                }

                .icon {
                    width: 20px;
                    height: 20px;
                    fill: currentColor;
                    transition: transform 0.3s ease;
                }
            </style>

            <div class="panel collapsed">
                <button class="toggle-btn" aria-label="Toggle Panel">
                    <svg class="handle-shape" preserveAspectRatio="none" viewBox="0 0 35 ${handleHeight}">
                        <path d="
                            M35,0 
                            C35,${curveHeight/2} 0,${curveHeight/2} 0,${curveHeight} 
                            L0,${handleHeight - curveHeight} 
                            C0,${handleHeight - curveHeight/2} 35,${handleHeight - curveHeight/2} 35,${handleHeight} 
                            Z" />
                    </svg>

                    <div class="content-wrapper">
                        <span class="label">${this._title}</span>
                        <svg class="icon" style="transform: rotate(180deg)" viewBox="0 0 24 24">
                            <path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z"/>
                        </svg>
                    </div>
                </button>

                <slot></slot>
            </div>
        `;
    }
}

customElements.define("right-drawer", RightDrawer);
