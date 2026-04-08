import { ZoneController } from "./ZoneController.js";
import { getDistanceColors } from "./color.js";
import { zoneData } from "./services/ZoneDataService.js";
import { filterService } from "./services/FilterService.js";
import { themeService } from "./services/ThemeService.js";

export class TriviaController extends ZoneController {
    constructor(backend) {
        super(backend);
        this.selectedId = null;

        this.normalOpacity = 0.1;
        this.hoverOpacity = 0.4;
        this.selectOpacity = 0.8;

        this.normalOpacityDark = 0.3;
        this.hoverOpacityDark = 0.6;
        this.selectOpacityDark = 1;

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
        if(!this.zoneInfo || !themeService) return { opacity: 0, fillOpacity: 0 };

        const isSelected = filterService.isSelectedZone(id);
        const isLight = themeService.theme === 'light';
        const {color, isFound} = this.zoneInfo[id];

        if (isLight){
            const opacity = isSelected ? this.selectOpacity : this.normalOpacity;

            return {
                color: isFound? "grey":"black",
                fillColor: isFound? color:"indigo",
                fillOpacity: isFound? 1:opacity,
                weight: 1,
            };
        }else{
            const opacity = isSelected ? this.selectOpacityDark : this.normalOpacityDark;

            return {
                color: isFound? "grey":"white",
                fillColor: isFound? color:"blue",
                fillOpacity: isFound? 1:opacity,
                weight: 1,
            };
        }
        
    }

    onHover(e, id) {
        if(!this.zoneInfo || !themeService) return { opacity: 0, fillOpacity: 0 };

        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);
        if(isSelected) return;

        const isLight = themeService.theme === 'light';
        const {isFound} = this.zoneInfo[id];

        if (isLight){
            target.setStyle({
                fillOpacity: isFound? 1:this.hoverOpacity,
            });
        }else{
            target.setStyle({
                fillOpacity: isFound? 1:this.hoverOpacityDark,
            });
        }  
    }

    onUnhover(e, id) {
        if(!this.zoneInfo || !themeService) return { opacity: 0, fillOpacity: 0 };

        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);

        if(isSelected) return;

        const isLight = themeService.theme === 'light';
        const {isFound} = this.zoneInfo[id];

        if (isLight){
            target.setStyle({
                fillOpacity: isFound? 1:this.normalOpacity,
            });
        }else{
            target.setStyle({
                fillOpacity: isFound? 1:this.normalOpacityDark,
            });
        }

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
