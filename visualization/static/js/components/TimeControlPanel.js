import { dataRangeservice } from '../services/DataRangeService.js';
import { filterService } from '../services/FilterService.js';
const template = document.createElement('template');
template.innerHTML = `
    <style>
        :host {
            display: block;
            width: 100%; 
            box-sizing: border-box;
            font-family: system-ui, sans-serif;
            color: var(--app-text, #1f2937);
        }
        
        h3 { margin: 0 0 16px 0; font-size: 1rem; color: var(--app-text-title, #111827); }

        .section-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: bold;
            color: var(--app-text-title, #111827);
            opacity: 0.7;
            margin-bottom: 6px;
        }

        .btn-group { display: flex; gap: 4px; margin-bottom: 16px; }
        
        .btn {
            flex: 1;
            background: var(--app-bg-secondary, #e5e7eb);
            border: 1px solid var(--map-bg, #d4dadc);
            color: var(--app-text, #1f2937);
            padding: 6px 0;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            text-align: center;
        }
        .btn:hover { border-color: var(--app-text-accent, #2563eb); }
        .btn.active {
            background: var(--app-text-accent, #2563eb);
            color: #ffffff;
            border-color: var(--app-text-accent, #2563eb);
        }

        .input-group {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 8px;
            background: var(--app-bg-secondary, #f3f4f6);
            padding: 6px 10px; 
            border-radius: 8px;
            border: 1px solid var(--map-bg, #d4dadc);
        }
        
        .input-group label { font-size: 0.8rem; font-weight: bold; color: var(--app-text-title, #111827); width: 36px; }

        .datetime-split { display: flex; gap: 4px; justify-content: left; }

        input[type="date"], select {
            border: 1px solid var(--map-bg, #d4dadc);
            background: var(--app-bg, #ffffff);
            color: var(--app-text, #1f2937);
            font-family: inherit;
            font-size: 0.9rem; 
            padding: 4px 6px; 
            border-radius: 4px;
            outline: none;
        }

        .shift-container { display: flex; gap: 6px; align-items: center; }
        
        /* Using variables from your style.css */
        .stepper-btn {
            background: var(--stepper-bg, #111827);
            color: var(--stepper-text, #ffffff);
            border: none;
            padding: 6px 8px;
            border-radius: 20px;
            font-size: 0.9rem;
            cursor: pointer;
            transition: opacity 0.2s;
            font-weight: bold;
        }
        .stepper-btn:hover { opacity: 0.8; }
        .stepper-btn:disabled { opacity: 0.4; cursor: not-allowed; }
        
        .step-selectors { flex: 1; }
        .step-selectors .btn-group { margin-bottom: 0; }
    </style>

    <h3>Time Filter</h3>
    
    <div class="section-label">Range Duration</div>
    <div class="btn-group" id="range-btns">
        <button class="btn range-btn" data-unit="h">1h</button>
        <button class="btn range-btn" data-unit="d">1d</button>
        <button class="btn range-btn" data-unit="w">1w</button>
        <button class="btn range-btn" data-unit="m">1m</button>
        <button class="btn range-btn" data-unit="y">1y</button>
        <button class="btn range-btn" data-unit="all">All</button>
    </div>

    <div class="input-group">
        <label>Start</label>
        <div class="datetime-split">
            <input type="date" id="start-date">
            <select id="start-hour"></select>
        </div>
    </div>
    <div class="input-group" style="margin-bottom: 16px;">
        <label>End</label>
        <div class="datetime-split">
            <input type="date" id="end-date">
            <select id="end-hour"></select>
        </div>
    </div>

    <div class="section-label">Shift Window By</div>
    <div class="shift-container">
        <button class="stepper-btn" id="shift-back">◀</button>
        <div class="step-selectors">
            <div class="btn-group" id="step-btns">
                <button class="btn step-btn" data-unit="h">1h</button>
                <button class="btn step-btn" data-unit="d">1d</button>
                <button class="btn step-btn" data-unit="w">1w</button>
                <button class="btn step-btn" data-unit="m">1m</button>
                <button class="btn step-btn" data-unit="y">1y</button>
            </div>
        </div>
        <button class="stepper-btn" id="shift-forward">▶</button>
    </div>
`;

export class TimeControlPanel extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
        this.shadowRoot.appendChild(template.content.cloneNode(true));

        this.startDate = this.shadowRoot.getElementById('start-date');
        this.startHour = this.shadowRoot.getElementById('start-hour');
        this.endDate = this.shadowRoot.getElementById('end-date');
        this.endHour = this.shadowRoot.getElementById('end-hour');
        this.btnBack = this.shadowRoot.getElementById('shift-back');
        this.btnForward = this.shadowRoot.getElementById('shift-forward');
        this.rangeBtns = this.shadowRoot.querySelectorAll('.range-btn');
        this.stepBtns = this.shadowRoot.querySelectorAll('.step-btn');

        this.currentStepUnit = 'd'; 
        this.isAllMode = false;
        this.preAllStart = null; 
        this.unsubscribeFilter = null;
        
        // NEW: Flag to track if we've already loaded the initial data limits
        this.isInitialized = false; 
    }

    connectedCallback() {
        if (!this.startHour.options.length) {
            this.populateHourSelects();
        }

        this.init();

        this.setupEventListeners();
        this.unsubscribeFilter = filterService.addListener("date", (r) => this.updateInputs(r));
        
        if (filterService.dateRange && filterService.dateRange.min) {
            this.updateInputs(filterService.dateRange);
        }
    }

    setupEventListeners() {
        if (this._listenersAdded) return;
        this._listenersAdded = true;

        this.startDate.addEventListener('change', () => this.handleManualInput());
        this.startHour.addEventListener('change', () => this.handleManualInput());
        this.endDate.addEventListener('change', () => this.handleManualInput());
        this.endHour.addEventListener('change', () => this.handleManualInput());

        this.rangeBtns.forEach(btn => {
            btn.addEventListener('click', (e) => this.applyRange(e.target.dataset.unit, e.target));
        });

        this.stepBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.stepBtns.forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentStepUnit = e.target.dataset.unit;
            });
        });

        this.btnBack.addEventListener('click', () => this.shiftWindow(-1));
        this.btnForward.addEventListener('click', () => this.shiftWindow(1));
    }

    disconnectedCallback() {
        if (this.unsubscribeFilter) {
            this.unsubscribeFilter();
            this.unsubscribeFilter = null;
        }
    }

    async init() {

        if (this.isInitialized) return;

        try {
            const range = await dataRangeservice.load(); 
            this.minAllowedDate = range.min;
            this.maxAllowedDate = range.max;
            
            this.setInitialState();
            
            this.isInitialized = true;
        } catch (err) { 
            console.error("Init failed", err); 
        }
    }

    setInitialState() {
        this.isAllMode = true;
        this.shadowRoot.querySelector('.range-btn[data-unit="all"]').classList.add('active');
        this.shadowRoot.querySelector('.step-btn[data-unit="d"]').classList.add('active');

        // This pushes the global state, which will trigger updateInputs()
        // via the listener we set up in connectedCallback
        filterService.selectDateRange({
            min: this.minAllowedDate,
            max: this.maxAllowedDate,
        });
    }
    populateHourSelects() {
        let optionsHtml = '';
        for(let i = 0; i < 24; i++) {
            const hour = i.toString().padStart(2, '0');
            optionsHtml += `<option value="${hour}:00">${hour}:00</option>`;
        }
        this.startHour.innerHTML = optionsHtml;
        this.endHour.innerHTML = optionsHtml;
    }

    getDates() {
        const sh = this.startHour.value ? this.startHour.value : "00:00";
        const eh = this.endHour.value ? this.endHour.value : "00:00";

        const start = new Date(`${this.startDate.value}T${sh}`);
        const end = new Date(`${this.endDate.value}T${eh}`);
        return { start, end };
    }

    adjustDate(dateObj, amount, unit) {
        let d = new Date(dateObj);
        if (unit === 'h') d.setHours(d.getHours() + amount);
        else if (unit === 'd') d.setDate(d.getDate() + amount);
        else if (unit === 'w') d.setDate(d.getDate() + (amount * 7));
        else if (unit === 'm') d.setMonth(d.getMonth() + amount);
        else if (unit === 'y') d.setFullYear(d.getFullYear() + amount);
        return d;
    }

    handleManualInput() {
        this.isAllMode = false;
        this.rangeBtns.forEach(b => b.classList.remove('active'));
        const { start, end } = this.getDates();
        const clamped = this.clampWindow(start, end);

        if (isNaN(start.getTime()) || isNaN(end.getTime())) {
            return;
        }

        filterService.selectDateRange({
            min: clamped.start,
            max: clamped.end,
        });
    }

    applyRange(unit, clickedBtn) {
        this.rangeBtns.forEach(b => b.classList.remove('active'));
        clickedBtn.classList.add('active');

        let { min: start, max: end } = filterService.dateRange;

        if (unit === 'all') {
            if (!this.isAllMode) {
                this.preAllStart = new Date(start);
                this.isAllMode = true;
            }
            start = new Date(this.minAllowedDate);
            end = new Date(this.maxAllowedDate);
        } else {
            if (this.isAllMode && this.preAllStart) {
                start = new Date(this.preAllStart);
                this.isAllMode = false;
            }
            end = this.adjustDate(start, 1, unit);
        }

        const clamped = this.clampWindow(start, end);
        filterService.selectDateRange({
            min: clamped.start,
            max: clamped.end,
        });
    }

    shiftWindow(direction) {
        if (this.isAllMode) return;
        let { min: start, max: end } = filterService.dateRange;
        const newStart = this.adjustDate(start, direction, this.currentStepUnit);
        const newEnd = this.adjustDate(end, direction, this.currentStepUnit);
        const clamped = this.clampWindow(newStart, newEnd);

        filterService.selectDateRange({
            min: clamped.start,
            max: clamped.end,
        });
    }

    clampWindow(targetStart, targetEnd) {
        let s = new Date(targetStart);
        let e = new Date(targetEnd);

        if(s > e) {
            let tmp = s;
            s = e;
            e = tmp;
        }

        let duration = Math.max(3_600_000, e.getTime() - s.getTime());
        if(this.maxAllowedDate && this.minAllowedDate) {
            duration = Math.min(duration, this.maxAllowedDate.getTime() - this.minAllowedDate.getTime());
        }


        if (this.maxAllowedDate && e > this.maxAllowedDate) {
            e = new Date(this.maxAllowedDate);
            s = new Date(e.getTime() - duration);
        }

        if (this.minAllowedDate && s < this.minAllowedDate) {
            s = new Date(this.minAllowedDate);
            const maxTime = this.maxAllowedDate ? this.maxAllowedDate.getTime() : Infinity;
            e = new Date(Math.min(s.getTime() + duration, maxTime));
        }

        return { start: s, end: e };
    }

    updateInputs(range) {
        const format = (d) => {
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            const hour = String(d.getHours()).padStart(2, '0');
            return { date: `${year}-${month}-${day}`, hour: `${hour}:00` };
        };

        const start = range["min"];
        const end = range["max"];

        const sFmt = format(start);
        const eFmt = format(end);

        this.isAllMode = start.getTime() == this.minAllowedDate?.getTime() && end.getTime() == this.maxAllowedDate?.getTime();

        this.startDate.value = sFmt.date;
        this.startHour.value = sFmt.hour;
        this.endDate.value = eFmt.date;
        this.endHour.value = eFmt.hour;

        this.syncRangeButtons(start, end);
    }

    syncRangeButtons(start, end) {
        this.rangeBtns.forEach(btn => btn.classList.remove('active'));

        if (this.isAllMode) {
            this.shadowRoot.querySelector('.range-btn[data-unit="all"]')?.classList.add('active');
            return;
        }

        const units = ['h', 'd', 'w', 'm', 'y'];
        for (const unit of units) {
            const expectedEnd = this.adjustDate(start, 1, unit);

            // If the current end matches the calculated end for this unit
            if (expectedEnd.getTime() === end.getTime()) {
                const match = this.shadowRoot.querySelector(`.range-btn[data-unit="${unit}"]`);
                if (match) match.classList.add('active');
                break;
            }
        }
    }

}

customElements.define('time-control-panel', TimeControlPanel);
