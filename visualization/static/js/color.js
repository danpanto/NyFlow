const colorPalette = [
    "#637EFA",
    "#EF553B",
    "#00CC96",
    "#AB63FA",
    "#FFA15A",
    "#19D3F3",
    "#FF6692",
    "#B6E880",
    "#FF97FF",
    "#FECB52",
];

const GRADIENT_THEMES = {
    magma: ["#000004", "#3b0f70", "#8c2981", "#de4968", "#fe9f6d", "#fcfdbf"], // Purple, Red, Yellow
    viridis: ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"], // Green, Yellow
    ocean: ["#ebf8ff", "#90cdf4", "#4299e1", "#2b6cb0", "#2c5282"], // Blue
    balance: ["#d7191c", "#fdae61", "#ffffbf", "#abd9e9", "#2c7bb6"], // Red, Blue
    fire: ["#000000", "#800000", "#FF0000", "#FFA500", "#FFFF00", "#FFFFFF"], // High contrast
};

export function paletteFromList(list) {
    const palette = new Map();
    let i = 0;

    for (const value of list) {
        const color = colorPalette[i % colorPalette.length];
        palette.set(value, color);
        i++;
    }

    if (i > colorPalette.length) {
        console.warn(`Palette generation used repeated colors. Items: ${i}, Colors: ${colorPalette.length}`);
    }

    return {
        _palette: palette,

        get(id, theme = "light") {
            return this._palette.get(id) || "#CCCCCC";
        }
    };
}

export class Gradient {
    constructor(themeName = 'viridis', steps = null) {
        this.themes = {
            magma: ["#000004", "#3b0f70", "#8c2981", "#de4968", "#fe9f6d", "#fcfdbf"],
            viridis: ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"],
            ocean: ["#ebf8ff", "#90cdf4", "#4299e1", "#2b6cb0", "#2c5282"],
            balance: ["#d7191c", "#fdae61", "#ffffbf", "#abd9e9", "#2c7bb6"],
            fire: ["#000000", "#800000", "#FF0000", "#FFA500", "#FFFF00", "#FFFFFF"]
        };
        this.steps = steps; 
        this.changeTheme(themeName);
    }

    changeTheme(themeName) {
        this.currentTheme = this.themes[themeName] || this.themes['viridis'];
    }

    get(value, steps = this.steps) {
        let v = Math.max(0, Math.min(1, value));

        if (steps && steps > 1) {
            v = Math.floor(v * steps) / (steps - 1);
            v = Math.min(1, v); 
        }

        const stops = this.currentTheme.length;
        const i = Math.min(Math.floor(v * (stops - 1)), stops - 2);
        
        const color1 = this._hexToRgb(this.currentTheme[i]);
        const color2 = this._hexToRgb(this.currentTheme[i + 1]);
        
        const rangeStep = 1 / (stops - 1);
        const localWeight = (v - (i * rangeStep)) / rangeStep;

        const r = Math.round(color1.r + (color2.r - color1.r) * localWeight);
        const g = Math.round(color1.g + (color2.g - color1.g) * localWeight);
        const b = Math.round(color1.b + (color2.b - color1.b) * localWeight);

        return this._rgbToHex(r, g, b);
    }

    getCss(direction = 90, steps = this.steps) {
        if (!steps) {
            return `linear-gradient(${direction}deg, ${this.currentTheme.join(', ')})`;
        }

        const cssSteps = [];
        for (let i = 0; i < steps; i++) {
            const color = this.get(i / (steps - 1), steps);
            const startPos = (i / steps) * 100;
            const endPos = ((i + 1) / steps) * 100;
            cssSteps.push(`${color} ${startPos}%`, `${color} ${endPos}%`);
        }
        return `linear-gradient(${direction}deg, ${cssSteps.join(', ')})`;
    }

    _hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result ? {
            r: parseInt(result[1], 16), 
            g: parseInt(result[2], 16), 
            b: parseInt(result[3], 16)
        } : { r: 0, g: 0, b: 0 };
    }

    _rgbToHex(r, g, b) {
        return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
    }
}
