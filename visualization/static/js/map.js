default_opacity = 0.2;
hover_opacity = 0.5;

zones_class = "zones_overlay";

function onEachFeatureZones(feature, layer) {
    if (feature.properties) {
        const popupContent = `
            <div>
                <h3>${feature.properties.zone}</h3>
                <div>
                    <span>Borough:</span> ${feature.properties.borough}
                </div>
                <div>
                    <span>Location ID:</span> ${feature.properties.locationid}
                </div>
            </div>
        `;

        layer.bindPopup(popupContent);
    }

    layer.on('mouseover', (e) => {
            const currentLayer = e.target;
            currentLayer.setStyle({
                fillOpacity: hover_opacity,
            });
    });

    layer.on('mouseout', (e) => {
        // Close popup after 5 secs
        setTimeout(() => {
            layer.closePopup(); 
        }, 1500);

        // Reset layer state
        const currentLayer = e.target;
        currentLayer.setStyle({
            fillOpacity: default_opacity,
        });
    });
}


function styleZones(feature) {
    // Default fallback
    var color = "#333333";

    switch (feature.properties.borough) {
        case 'Manhattan':     color = "#3b82f6"; break; // Blue
        case 'Brooklyn':      color = "#f97316"; break; // Orange
        case 'Queens':        color = "#a855f7"; break; // Purple
        case 'Bronx':         color = "#ef4444"; break; // Red
        case 'Staten Island': color = "#10b981"; break; // Teal
        case 'EWR':           color = "#64748b"; break; // Slate Gray
    }
    
    return {
        className: zones_class,
        weight: 1,
        color: "white",
        opacity: 1,
        fillColor: color,
        fillOpacity: default_opacity,
    };
}

async function getZonesLayer() {
    // Load taxi zones data
    const api_endpoint = "/api/taxi_zones";
    const response = await fetch(api_endpoint);
    if (!response.ok) {
        console.error(`Couldn't fetch data from '${api_endpoint}'.`);
        return null;
    }

    var data = undefined;
    try {
        data = await response.json();
    } catch (error) {
        console.error(`Couldn't decode data from '${api_endpoint}'.`);
        return null;
    }

    // Create layer
    const layer = L.geoJSON(
        data,
        {
            onEachFeature: onEachFeatureZones,
            style: styleZones,
        }
    );
    return layer;
}

document.addEventListener("DOMContentLoaded", async function() {

    // Initialize geographic map
    const initial_coordinates = [40.68, -73.95];
    const initial_zoom = 13;
    var map = L.map('map').setView(initial_coordinates, initial_zoom);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        minZoom: 11,
        maxZoom: 20,
    }).addTo(map);

    const zones_layer = await getZonesLayer();
    if (zones_layer != null) zones_layer.addTo(map);
});
