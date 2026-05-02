import { BaseLayer } from "./BaseLayer.js";
import { TriviaController } from "./TriviaController.js";
import { filterService } from "./services/FilterService.js";
import { ZoneInfo } from "./components/ZoneInfo.js";
import { zoneData } from "./services/ZoneDataService.js";

export class TriviaLayer extends BaseLayer {
    constructor(mapManager, backend) {
        super();

        this.name = "trivia";
        this.mapManager = mapManager;
        this.baseAppName = "NyFinder";

        // Add map layer
        const mapController = new TriviaController(backend);
        mapManager.addLayer(this.name, mapController);
        this.mapController = mapController;

        this.zoneInfoDiv = document.createElement("zone-info");

        this.unsubscribeSelectZone = null;

        this._setupUI();
    }

    _setupUI() {
        this.audioWin = new Audio('/static/audio/gana.m4a');

        this.banner = document.createElement('div');
        this.banner.className = 'trivia-banner';
        this.banner.style.display = 'none';
        document.body.appendChild(this.banner);

        this.modal = document.createElement('div');
        this.modal.className = 'trivia-modal-overlay';
        this.modal.style.display = 'none';

        this.modalContent = document.createElement('div');
        this.modalContent.className = 'trivia-modal-content';
        this.modal.appendChild(this.modalContent);

        document.body.appendChild(this.modal);

        this.mapController.onGameStateChange = this.handleGameState.bind(this);
    }

    getWinMessage(attempts) {
        if (attempts === 1) return "Eres un verdadero City enjoyer!";
        if (attempts <= 5) return "Atiende a la clase";
        if (attempts <= 15) return "Te falta calle, bro";
        return "No eres un verdadero taxista, y lo sabes...";
    }

    getAttemptsColor(attempts) {
        if (attempts <= 5) return '#10b981';
        if (attempts <= 15) return '#fbbf24';
        return '#ef4444';
    }

    handleGameState(state, attempts) {
        if (state === 'start') {
            const targetName = zoneData.getName(this.mapController.targetId);
            this.banner.textContent = `Find: ${targetName}`;
            this.modal.style.display = 'none';
        } else if (state === 'win') {
            const targetName = zoneData.getName(this.mapController.targetId);
            const color = this.getAttemptsColor(attempts);
            const msg = this.getWinMessage(attempts);

            this.modalContent.innerHTML = `
                <h2>¡Encontrado!</h2>
                <p>${targetName}</p>
                <div class="trivia-attempts" style="color: ${color};">${attempts} intento${attempts > 1 ? 's' : ''}</div>
                <div class="trivia-msg">${msg}</div>
                <button class="trivia-restart-btn">Restart</button>
            `;

            this.modalContent.querySelector('.trivia-restart-btn').addEventListener('click', () => {
                filterService.selectZone(null, true, true);
                this.zoneInfoDiv.visible = false;
                this.mapController.restart();
                this.mapController.backend.refresh();
            });

            this.modal.style.display = 'flex';

            // Hide the zone info card while the modal is up
            this.zoneInfoDiv.visible = false;

            if (attempts <= 5 && typeof confetti !== 'undefined') {
                confetti({
                    particleCount: 150,
                    spread: 70,
                    origin: { y: 0.6 },
                    zIndex: 2000
                });

                this.audioWin.currentTime = 0;
                this.audioWin.play().catch(e => console.error("Audio block", e));
            }
        }
    }

    bind() {
        document.body.appendChild(this.zoneInfoDiv);
        this.unsubscribeSelectZone = filterService.addListener("zones", (_) => { this.onSelectedZone(filterService.lastZone); })
        filterService.selectZone(filterService.lastZone, true, true);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = `• Zones`;
        }
        const titleEl = document.querySelector('.title-text');
        if (titleEl) titleEl.textContent = 'NyFinder';
        document.title = `${this.baseAppName} • Zones`;

        this.mapManager.toggleLayer(this.name, true);
        this.onSelectedZone(filterService.lastZone);

        this.banner.style.display = 'block';
        if (this.mapController.targetId) {
            const targetName = zoneData.getName(this.mapController.targetId);
            this.banner.textContent = `Find: ${targetName}`;
        }
    }

    unbind() {
        this.mapManager.toggleLayer(this.name, false);
        this.zoneInfoDiv.remove();
        if (this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = "";
        }
        const titleEl = document.querySelector('.title-text');
        if (titleEl) titleEl.textContent = 'NyFlow';
        document.title = this.baseAppName;

        this.banner.style.display = 'none';
        this.modal.style.display = 'none';
        this.audioWin.pause();
    }

    onSelectedZone(zone) {
        if (!zone) {
            this.zoneInfoDiv.visible = false;
            return;
        }

        this.zoneInfoDiv.visible = true;

        const name = zoneData.getName(zone);
        const borough = zoneData.getBorough(zone);

        this.zoneInfoDiv.heading = name;
        const data = {};

        const targetId = this.mapController.targetId;
        if (targetId) {
            const dist = zoneData.getDistance(zone, targetId);
            if (dist !== null) {
                const distKm = (dist / 1000).toFixed(1);
                data["Distance to target"] = `${distKm} km`;
            }
        }

        this.zoneInfoDiv.data = data;
    }
}
