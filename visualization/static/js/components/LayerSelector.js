import { filterService } from "../services/FilterService.js";
import { VARIABLE_CONFIG } from "../queryVariables.js";

export class LayerSelector extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this._maxRows = 4;
        this._aggGroups = [];

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

        const groups = {}
        allButtons.forEach(btn => {
            const colName = btn.getAttribute('column') || "General";
            if (!groups[colName]) groups[colName] = [];
            groups[colName].push(btn);

            const layerId = btn.getAttribute('layer');
            if (layerId && VARIABLE_CONFIG[layerId]) {
                btn.textContent = VARIABLE_CONFIG[layerId].shortName;
            }
        });

        this._aggGroups = []; // Resetear
        Object.keys(groups).forEach((colName) => {
            if (groups[colName].length > this._maxRows) {
                this._aggGroups.push(colName);
            }
        });

        // renderizar
        this.render(groups);

        // asignar slots (Después del render para que existan los slots en el Shadow DOM)
        Object.keys(groups).forEach((colName, groupIndex) => {
            if (this._aggGroups.includes(colName)) {
                groups[colName].forEach((btn, btnIndex) => {
                    const colIndex = Math.floor(btnIndex / this._maxRows);
                    btn.setAttribute('slot', `slot-${groupIndex}-col-${colIndex}`);
                });
            } else {
                groups[colName].forEach(btn => {
                    btn.setAttribute('slot', `slot-${groupIndex}`);
                });
            }
        });

    }

    render(groups) {
        const groupNames = Object.keys(groups);

        const renderGroup = (colName) => {
            const groupIndex = groupNames.indexOf(colName);
            return `             
                <div class="group-section">
                    <div class="group-header">${colName}</div>
                    <div class="group-grid">
                        <slot name="slot-${groupIndex}"></slot>
                    </div>
                </div>   
            `;
        };

        const renderAggGroup = (colName) => {
            const groupIndex = groupNames.indexOf(colName);
            const buttonCount = groups[colName].length;
            const colCount = Math.ceil(buttonCount / this._maxRows);
            let slotsHtml = '';
            for (let i = 0; i < colCount; i++) {
                slotsHtml += `<div class="sub-column"><slot name="slot-${groupIndex}-col-${i}"></slot></div>`;
            }
            return `
            <div class="column">
                <div class="group-section">
                    <div class="group-header">${colName}</div>
                    <div class="agg-flex-container">
                        ${slotsHtml}
                    </div>
                </div>
            </div>
            `;
        };

        let finalHtml = "";
        let isInsideColumn = false;
        let currentNormCount = 0;

        groupNames.forEach((colName) => {
            const numElems = groups[colName].length;

            if (this._aggGroups.includes(colName)) {
                // Si veníamos de una columna normal, la cerramos
                if (isInsideColumn) { finalHtml += `</div>`; isInsideColumn = false; }
                finalHtml += renderAggGroup(colName);
                currentNormCount = 0;
            } else {
                // Lógica para agrupar secciones normales en columnas de max 4 elementos totales
                if (!isInsideColumn || (currentNormCount + numElems > this._maxRows)) {
                    if (isInsideColumn) finalHtml += `</div>`;
                    finalHtml += `<div class="column">`;
                    isInsideColumn = true;
                    currentNormCount = 0;
                }
                finalHtml += renderGroup(colName);
                currentNormCount += numElems;
            }
        });

        if (isInsideColumn) finalHtml += `</div>`;

        this.shadowRoot.innerHTML = `
            <style>
                :host {
                    display: block;
                    width: 100%;
                    margin: 0 auto;
                    font-family: sans-serif;
                }

                .layer-container {
                    display: flex;
                    justify-content: center;
                    align-items: flex-start;
                    gap: 40px;
                    padding: 20px;
                }

                .column {
                    display: flex;
                    flex-direction: column;
                    gap: 20px;
                    flex-shrink: 0;
                }

                .group-section {
                    margin-bottom: 0px;
                }

                .group-header {
                    color: var(--app-text);
                    font-size: 0.85rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 12px;
                    padding-left: 10px;
                    opacity: 0.7;
                    border-left: 3px solid var(--app-bg-secondary);
                }

                .group-grid {
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                }

                .agg-flex-container {
                    display: flex;
                    gap: 4px;
                    align-items: flex-start;
                }

                .sub-column {
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                    flex-shrink: 0;
                }

                ::slotted(button) {
                    padding: 12px 10px;
                    background: var(--app-bg);
                    cursor: pointer;
                    font-family: sans-serif;
                    font-size: 1.0rem;
                    font-weight: 700;
                    border: 0;
                    border-radius: 8px;
                    transition: all 0.2s ease;
                    text-align: center;
                    color: var(--app-text);
                    width: 210px;
                    box-sizing: border-box;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                }

                ::slotted(button:hover) {
                    background: var(--app-bg-secondary);
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                }

                ::slotted(button.active) {
                    background: var(--app-bg-secondary);
                    outline: 2px solid var(--app-accent, #2563eb);
                    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
                }
            </style>
            <div class="layer-container">
                ${finalHtml}
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

        // Notify parent components (like BottomDrawer) to auto-close
        this.dispatchEvent(new CustomEvent('layer-selected', {
            detail: { layerId },
            bubbles: true,
            composed: true
        }));
    }
}

customElements.define('layer-selector', LayerSelector);
