import { ZoneController } from "./ZoneController.js";
import { getDistanceColors } from "./color.js";
import { zoneData } from "./services/ZoneDataService.js";
import { filterService } from "./services/FilterService.js";

export class TriviaController extends ZoneController {
    constructor(backend) {
        super(backend);
        this.selectedId = null;

        this.normalOpacity = 0.1;
        this.hoverOpacity = 0.4;
        this.selectOpacity = 0.8;

        this.attempts = 0;
        this.targetId = null;
        this.won = false;

        this.onGameStateChange = null; // Callback for UI updates

        this._init();
    }

    async _init() {
        await zoneData.load();
        this.restart();
    }

    restart() {
        const ids = zoneData.getIds();
        const selId = ids[Math.floor(Math.random()*ids.length)];
        
        this.targetId = selId;
        this.attempts = 0;
        this.won = false;

        const arrDistancias = []
        for (const id of ids){
            arrDistancias.push(zoneData.getDistance(selId, id));
        }
        
        const palette = getDistanceColors(arrDistancias);

        this.zoneInfo = {}
        for (let i=0; i<ids.length; ++i){
            const id = ids[i];
            this.zoneInfo[id] = {
                isObjective: id===selId,
                color: palette[i],
                isFound: false
            }
        }

        if(this._visible) {
            this.backend.refresh();
        }

        if (this.onGameStateChange) {
            this.onGameStateChange('start');
        }
    }

    update(data) {
        super.update(data);

        if(data.click !== undefined) {
            filterService.selectZone(null);
            this.backend.refresh();
        }
    }

    getStyle(id) {
        if(!this.zoneInfo) return { opacity: 0, fillOpacity: 0 };

        const isSelected = filterService.isSelectedZone(id);
        const opacity = isSelected ? this.selectOpacity : this.normalOpacity;

        const {color, isFound} = this.zoneInfo[id];
        return {
            color: isFound? "grey":"black",
            fillColor: isFound? color:"indigo",
            fillOpacity: isFound? 1:opacity,
            weight: 1,
        };
    }

    onHover(e, id) {
        if(!this.zoneInfo) return { opacity: 0, fillOpacity: 0 };

        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);
        if(isSelected) return;
        
        const {isFound} = this.zoneInfo[id];
        target.setStyle({
            fillOpacity: isFound? 1:this.hoverOpacity,
        });
    }

    onUnhover(e, id) {
        if(!this.zoneInfo) return { opacity: 0, fillOpacity: 0 };

        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);

        if(isSelected) return;

        const {isFound} = this.zoneInfo[id];
        target.setStyle({
            fillOpacity: isFound? 1:this.normalOpacity,
        });
    }

    onClick(e, id) {
        if (this.won) return; // Ignore clicks if already won

        const isSelected = filterService.isSelectedZone(id);
        filterService.selectZone(id, !isSelected, true);

        const info = this.zoneInfo[id];
        if (!info.isFound) {
            info.isFound = true;
            this.attempts++;

            if (info.isObjective) {
                this.won = true;
                if (this.onGameStateChange) this.onGameStateChange('win', this.attempts);
            } else {
                if (this.onGameStateChange) this.onGameStateChange('wrong', this.attempts);
            }

            this.backend.refresh(); 
        }
    }
}
