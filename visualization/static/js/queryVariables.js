const compactNumber = new Intl.NumberFormat('en-US', {
    notation: "compact",
    maximumFractionDigits: 1 // Turns 15400 into 15.4K
}).format;

const compactCurrency = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: "compact",
    maximumFractionDigits: 1 // Turns 1500000 into $1.5M
}).format;

const standardDecimal = new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 2 // Keeps small numbers normal, e.g., 3.45
}).format;

export const VARIABLE_CONFIG = {
    total_trips: {
        longName: "Total Trips",
        shortName: "Trips",
        description: "The absolute number of trips recorded in this zone.",
        formatter: compactNumber,
        units: "",
    },
    total_price: {
        longName: "Total Price",
        shortName: "Total Fare",
        description: "The cumulative sum of all fares paid.",
        formatter: compactCurrency,
        units: "USD"
    },
    mean_price: {
        longName: "Mean Price",
        shortName: "Avg Fare",
        description: "The average cost of a trip originating from this zone.",
        formatter: compactCurrency,
        units: "USD"
    },
    total_tip: {
        longName: "Total Tip",
        shortName: "Total Tips",
        description: "The cumulative sum of all tips given.",
        formatter: compactCurrency,
        units: "USD"
    },
    mean_tip: {
        longName: "Mean Tip",
        shortName: "Avg Tip",
        description: "The average tip amount left by passengers.",
        formatter: compactCurrency,
        units: "USD"
    },
    mean_distance: {
        longName: "Mean Distance",
        shortName: "Avg Distance",
        description: "The average distance traveled per trip.",
        formatter: standardDecimal,
        units: "mi"
    },
    mean_duration: {
        longName: "Mean Duration",
        shortName: "Avg Duration",
        description: "The average time spent per trip.",
        formatter: standardDecimal,
        units: "min"
    },
    mean_tip_time: {
        longName: "Mean Tip per Minute",
        shortName: "Tip / Min",
        description: "The average tip earned per minute of driving.",
        formatter: standardDecimal,
        units: "$/min"
    },
    mean_tip_dis: {
        longName: "Mean Tip per Mile",
        shortName: "Tip / Mile",
        description: "The average tip earned per mile driven.",
        formatter: standardDecimal,
        units: "$/mi"
    },
    mean_price_time: {
        longName: "Mean Price per Minute",
        shortName: "Price / Min",
        description: "The average revenue generated per minute.",
        formatter: standardDecimal,
        units: "$/min"
    },
    mean_price_dis: {
        longName: "Mean Price per Mile",
        shortName: "Price / Mile",
        description: "The average revenue generated per mile.",
        formatter: standardDecimal,
        units: "$/mi"
    },
    restaurant_ratings: {
        longName: "Restaurant Ratings",
        shortName: "Restaurant Rating",
        description: "The average rating of restaurants in this zone.",
        formatter: standardDecimal,
        units: "Pts"
    },
    asking_rent: {
        longName: "Asking Rent",
        shortName: "Rent",
        description: "The average asking rent in this zone.",
        formatter: compactCurrency,
        units: "USD"
    },
    landmarks: {
        longName: "Touristic Landmarks",
        shortName: "Landmarks",
        description: "National and Historic Landmarks. With tip per distance layer.",
        formatter: compactCurrency,
        units: "USD"
    },
    trivia: {
        longName: "Zone Game",
        shortName: "Zone Game",
        description: "Guess the Zone in the least amount of tries. The selected zone is colored based on the distance to the Zone."
    },
    demand_hourly: {
        longName: "Demand Hourly",
        shortName: "Demand Hourly",
        description: "Per-zone demand classification (low / medium / high) for a chosen hour of day."
    }
};

export const SUPPORTED_VARIABLES = Object.keys(VARIABLE_CONFIG).filter(v =>
    v !== 'restaurant_ratings' && v !== 'asking_rent' && v !== 'trivia' && v !== 'demand_hourly' && v !== 'landmarks'
);
