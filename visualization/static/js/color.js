const colorPalette = [
    "#636EFA", // Fixed typo (was 635EFA)
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
