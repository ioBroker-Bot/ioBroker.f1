"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const utils = __importStar(require("@iobroker/adapter-core"));
const axios_1 = __importDefault(require("axios"));
const ws_1 = __importDefault(require("ws"));
// ── Constants ─────────────────────────────────────────────────────────────────
const SUBSCRIBE_STREAMS = [
    "TrackStatus",
    "SessionStatus",
    "SessionInfo",
    "WeatherData",
    "LapCount",
    "ExtrapolatedClock",
    "TimingData",
    "DriverList",
    "TimingAppData",
    "RaceControlMessages",
    "TopThree",
    "TeamRadio",
    "PitStopSeries",
    "TyreStintSeries",
];
const SIGNALR_CORE_RECORD_SEP = "\x1e";
const SIGNALR_CORE_NEGOTIATE_URL = "/signalrcore/negotiate";
const SIGNALR_CORE_WS_URL = "wss://livetiming.formula1.com/signalrcore";
const SESSION_DURATIONS = {
    "Practice 1": 60,
    "Practice 2": 60,
    "Practice 3": 60,
    Qualifying: 60,
    "Sprint Qualifying": 45,
    Sprint: 45,
    Race: 120,
};
const TRACK_STATUS_MAP = {
    1: "AllClear",
    2: "Yellow",
    3: "Flag",
    4: "SafetyCar",
    5: "RedFlag",
    6: "VSCDeployed",
    7: "VSCEnding",
    8: "SafetyCarEnding",
};
const LIVE_RACE_CONTROL_MAX = 200;
const LIVE_TEAM_RADIO_MAX = 100;
// ── Adapter class ─────────────────────────────────────────────────────────────
class F1 extends utils.Adapter {
    JOLPICA_BASE = "https://api.jolpi.ca/ergast/f1";
    // HTTP clients
    ergastApi;
    ltApi;
    // Timers
    scheduleInterval;
    liveCheckInterval;
    reconnectTimeout;
    postSessionRefreshTimeout;
    // Live state
    currentLiveSession = null;
    lastSavedSession = "";
    ws = null;
    wsConnecting = false;
    coreHandshakeAck = false;
    coreSubscribeSent = false;
    // Schedule cache — updated by refreshJolpicaData, reused by checkLiveStatus
    cachedRaces = [];
    cachedSessions = [];
    // In-memory SignalR stream caches (merged incrementally)
    driverList = {};
    timingData = {};
    timingAppData = {};
    tyreStintData = {};
    pitStopData = {}; // racing_number → pit stops array
    rcMessages = [];
    topThreeData = {};
    teamRadioCaptures = [];
    // Path of the current session (from SessionInfo), used to fetch static files
    sessionPath = "";
    liveFallbackActive = false;
    liveMeetingRound = null;
    liveMeetingName = "";
    liveMeetingCountry = "";
    liveSessionName = "";
    liveSessionType = "";
    liveSessionStartUTC = "";
    fallbackSuppressedUntilMs = 0;
    postSessionRefreshAttempts = 0;
    postSessionTargetRound = null;
    // Set true in onUnload so fire-and-forget async loops can exit early
    isUnloading = false;
    // ExtrapolatedClock: reference point for client-side countdown
    clockRefUtcMs = 0;
    clockRefRemainingMs = 0;
    clockExtrapolating = false;
    clockInterval;
    // Polling config (admin values are in seconds)
    dynamicPollingEnabled = true;
    normalPollingIntervalMs = 3600 * 1000;
    racePollingIntervalMs = 10 * 1000;
    currentLiveCheckIntervalMs = 0;
    constructor(options = {}) {
        super({ ...options, name: "f1" });
        this.ergastApi = axios_1.default.create({
            timeout: 15000,
            headers: { "User-Agent": "ioBroker.f1/1.0" },
        });
        this.ltApi = axios_1.default.create({
            baseURL: "https://livetiming.formula1.com",
            timeout: 8000,
            headers: { "User-Agent": "ioBroker.f1/1.0" },
        });
        this.on("ready", this.onReady.bind(this));
        this.on("unload", this.onUnload.bind(this));
    }
    async onReady() {
        this.log.info("Starting F1 adapter...");
        this.dynamicPollingEnabled = this.parseBooleanConfig(this.config.enableDynamicPolling, true);
        const normalIntervalSeconds = this.getValidatedIntervalSeconds(this.config.updateIntervalNormal, 3600, 60, 86400, "updateIntervalNormal");
        const raceIntervalSeconds = this.getValidatedIntervalSeconds(this.config.updateIntervalRace, 10, 5, 300, "updateIntervalRace");
        this.normalPollingIntervalMs = normalIntervalSeconds * 1000;
        this.racePollingIntervalMs = raceIntervalSeconds * 1000;
        await this.initializeStates();
        await this.setStateAsync("info.connection", { val: false, ack: true });
        // Initial full data load
        await this.refreshJolpicaData();
        // Jolpica refresh uses the validated configured normal polling interval.
        this.scheduleInterval = this.setInterval(() => void this.refreshJolpicaData(), this.normalPollingIntervalMs);
        // Live session checks use dynamic or static polling based on adapter config.
        await this.checkLiveStatus();
        this.updateLiveCheckInterval(new Date(), this.cachedSessions);
        await this.setStateAsync("info.connection", { val: true, ack: true });
    }
    parseBooleanConfig(value, fallback) {
        if (typeof value === "boolean") {
            return value;
        }
        if (typeof value === "string") {
            const normalized = value.trim().toLowerCase();
            if (normalized === "true") {
                return true;
            }
            if (normalized === "false") {
                return false;
            }
        }
        this.log.warn(`Invalid enableDynamicPolling value '${String(value)}'; using fallback ${String(fallback)}.`);
        return fallback;
    }
    getValidatedIntervalSeconds(value, fallbackSeconds, minSeconds, maxSeconds, configKey) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            this.log.warn(`Invalid ${configKey} value '${String(value)}'; using fallback ${fallbackSeconds}s (allowed ${minSeconds}-${maxSeconds}s).`);
            return fallbackSeconds;
        }
        const rounded = Math.trunc(parsed);
        const bounded = Math.min(maxSeconds, Math.max(minSeconds, rounded));
        if (bounded !== rounded) {
            this.log.warn(`Out-of-range ${configKey} value ${rounded}; clamped to ${bounded}s (allowed ${minSeconds}-${maxSeconds}s).`);
        }
        return bounded;
    }
    setLiveCheckInterval(intervalMs) {
        if (this.liveCheckInterval) {
            this.clearInterval(this.liveCheckInterval);
        }
        this.liveCheckInterval = this.setInterval(() => void this.checkLiveStatus(), intervalMs);
        this.currentLiveCheckIntervalMs = intervalMs;
    }
    shouldUseFastPolling(now, sessions) {
        if (this.currentLiveSession) {
            return true;
        }
        const nowMs = now.getTime();
        const FAST_PRE_MS = 60 * 60 * 1000;
        const FAST_POST_MS = 15 * 60 * 1000;
        for (const session of sessions) {
            const startMs = new Date(session.startUTC).getTime() - FAST_PRE_MS;
            const endMs = new Date(session.endUTC).getTime() + FAST_POST_MS;
            if (nowMs >= startMs && nowMs <= endMs) {
                return true;
            }
        }
        return false;
    }
    updateLiveCheckInterval(now, sessions) {
        const targetMs = this.dynamicPollingEnabled
            ? this.shouldUseFastPolling(now, sessions)
                ? this.racePollingIntervalMs
                : this.normalPollingIntervalMs
            : this.normalPollingIntervalMs;
        if (!this.liveCheckInterval || this.currentLiveCheckIntervalMs !== targetMs) {
            this.setLiveCheckInterval(targetMs);
        }
    }
    onUnload(callback) {
        this.isUnloading = true;
        try {
            if (this.scheduleInterval) {
                this.clearInterval(this.scheduleInterval);
            }
            if (this.liveCheckInterval) {
                this.clearInterval(this.liveCheckInterval);
            }
            if (this.reconnectTimeout) {
                this.clearTimeout(this.reconnectTimeout);
            }
            if (this.postSessionRefreshTimeout) {
                this.clearTimeout(this.postSessionRefreshTimeout);
                this.postSessionRefreshTimeout = undefined;
            }
            this.stopClockExtrapolation();
            this.disconnectSignalR(true); // full reset on adapter shutdown
            callback();
        }
        catch {
            callback();
        }
    }
    // ── State Initialization ──────────────────────────────────────────────────
    async initializeStates() {
        const channels = [
            {
                id: "schedule",
                name: "Race Schedule",
                states: [
                    { id: "next_race_name", name: "Next Race Name", type: "string", role: "text" },
                    { id: "next_race_round", name: "Next Race Round", type: "number", role: "value" },
                    { id: "next_race_circuit", name: "Next Race Circuit", type: "string", role: "text" },
                    { id: "next_race_country", name: "Next Race Country", type: "string", role: "text" },
                    { id: "next_race_date", name: "Next Race Date (UTC)", type: "string", role: "date" },
                    {
                        id: "next_race_countdown_days",
                        name: "Days until Race",
                        type: "number",
                        role: "value",
                        unit: "days",
                    },
                    { id: "next_session_name", name: "Next Session Name", type: "string", role: "text" },
                    { id: "next_session_type", name: "Next Session Type", type: "string", role: "text" },
                    { id: "next_session_date", name: "Next Session Date (UTC)", type: "string", role: "date" },
                    {
                        id: "next_session_countdown_hours",
                        name: "Hours until Session",
                        type: "number",
                        role: "value",
                        unit: "h",
                    },
                    { id: "weekend_json", name: "Current Weekend Sessions (JSON)", type: "string", role: "json" },
                    { id: "calendar", name: "Full Season Calendar (JSON)", type: "string", role: "json" },
                ],
            },
            {
                id: "standings",
                name: "Championship Standings",
                states: [
                    { id: "drivers", name: "Driver Standings (JSON)", type: "string", role: "json" },
                    { id: "teams", name: "Team Standings (JSON)", type: "string", role: "json" },
                    { id: "last_update", name: "Last Update", type: "string", role: "date" },
                ],
            },
            {
                id: "results",
                name: "Session Results",
                states: [
                    { id: "race", name: "Race Result (JSON)", type: "string", role: "json" },
                    { id: "qualifying", name: "Qualifying Result (JSON)", type: "string", role: "json" },
                    { id: "sprint", name: "Sprint Result (JSON)", type: "string", role: "json" },
                    { id: "last_update", name: "Last Update", type: "string", role: "date" },
                ],
            },
            {
                id: "live",
                name: "Live Session Data (F1 Live Timing)",
                states: [
                    { id: "is_live", name: "Session Active", type: "boolean", role: "indicator" },
                    { id: "session_name", name: "Session Name", type: "string", role: "text" },
                    { id: "session_status", name: "Session Status", type: "string", role: "text" },
                    {
                        id: "session_part",
                        name: "Qualifying Part (1=Q1 2=Q2 3=Q3 0=N/A)",
                        type: "number",
                        role: "value",
                    },
                    { id: "track_status", name: "Track Status", type: "string", role: "text" },
                    { id: "laps_current", name: "Current Lap", type: "number", role: "value" },
                    { id: "laps_total", name: "Total Laps", type: "number", role: "value" },
                    { id: "time_elapsed", name: "Time Elapsed", type: "string", role: "text" },
                    { id: "time_remaining", name: "Time Remaining", type: "string", role: "text" },
                    { id: "clock_utc", name: "Clock Reference UTC", type: "string", role: "date" },
                    { id: "weather", name: "Track Weather (JSON)", type: "string", role: "json" },
                    { id: "race_control", name: "Race Control Messages (JSON)", type: "string", role: "json" },
                    { id: "top_three", name: "Top 3 Drivers (JSON)", type: "string", role: "json" },
                    { id: "drivers", name: "All Drivers with Position/Tyre (JSON)", type: "string", role: "json" },
                    { id: "tyres", name: "Current Tyres per Driver (JSON)", type: "string", role: "json" },
                    { id: "pit_stops", name: "Pit Stops (JSON)", type: "string", role: "json" },
                    { id: "team_radio", name: "Team Radio (JSON)", type: "string", role: "json" },
                    { id: "last_update", name: "Last Update", type: "string", role: "date" },
                ],
            },
        ];
        for (const channel of channels) {
            await this.setObjectNotExistsAsync(channel.id, {
                type: "channel",
                common: { name: channel.name },
                native: {},
            });
            for (const state of channel.states) {
                await this.setObjectNotExistsAsync(`${channel.id}.${state.id}`, {
                    type: "state",
                    common: {
                        name: state.name,
                        type: state.type,
                        role: state.role,
                        read: true,
                        write: false,
                        ...(state.unit && { unit: state.unit }),
                    },
                    native: {},
                });
            }
        }
    }
    // ── Jolpica data ──────────────────────────────────────────────────────────
    /**
     * Fetch from Jolpica.
     * Returns null (instead of throwing) on not-available endpoints.
     *
     * @param path - API path, e.g. "/current/last/results.json"
     */
    async fetchErgast(path) {
        // Helper to detect unavailable endpoints and map them to null.
        // 400 = Jolpica "Endpoint does not support final filter" → treat as not-available
        const isNotFound = (e) => {
            const status = e?.response?.status;
            return status === 400 || status === 404 || status === 410;
        };
        try {
            const res = await this.ergastApi.get(`${this.JOLPICA_BASE}${path}`);
            return res.data;
        }
        catch (jolpicaErr) {
            if (isNotFound(jolpicaErr)) {
                this.log.debug(`Jolpica not-available for: ${path}`);
                return null;
            }
            throw jolpicaErr;
        }
    }
    async refreshJolpicaData() {
        try {
            const races = await this.fetchSchedule();
            const now = new Date();
            if (races.length > 0) {
                const allSessions = races.flatMap(r => this.buildSessionsFromRace(r));
                // Cache for reuse by checkLiveStatus (avoids repeated Jolpica requests)
                this.cachedRaces = races;
                this.cachedSessions = allSessions;
                await this.updateScheduleStates(races, allSessions, now);
                void this.updateLatestResults(races);
            }
            else if (this.cachedRaces.length > 0) {
                this.log.debug("Schedule fetch returned no races - using cached schedule for hourly state refresh");
                await this.updateScheduleStates(this.cachedRaces, this.cachedSessions, now);
                void this.updateLatestResults(this.cachedRaces);
            }
            else {
                this.log.debug("Schedule fetch returned no races and no cache exists yet");
            }
            await this.updateStandings();
        }
        catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            this.log.warn(`Jolpica refresh failed: ${msg}`);
        }
    }
    async fetchSchedule() {
        try {
            const data = await this.fetchErgast("/current.json");
            return data?.MRData?.RaceTable?.Races ?? [];
        }
        catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            this.log.warn(`Schedule fetch failed: ${msg}`);
            return [];
        }
    }
    buildSessionsFromRace(race) {
        const sessions = [];
        const round = parseInt(race.round, 10);
        const base = {
            round,
            raceName: race.raceName,
            circuit: race.Circuit.circuitName,
            country: race.Circuit.Location.country,
        };
        const add = (name, type, dt) => {
            if (!dt) {
                return;
            }
            const timeStr = dt.time && (dt.time.endsWith("Z") || dt.time.includes("+")) ? dt.time : `${dt.time}Z`;
            const startUTC = new Date(`${dt.date}T${timeStr}`);
            const durationMin = SESSION_DURATIONS[name] ?? 90;
            const endUTC = new Date(startUTC.getTime() + durationMin * 60 * 1000);
            sessions.push({
                ...base,
                name,
                type,
                startUTC: startUTC.toISOString(),
                endUTC: endUTC.toISOString(),
            });
        };
        add("Practice 1", "Practice", race.FirstPractice);
        add("Practice 2", "Practice", race.SecondPractice);
        add("Practice 3", "Practice", race.ThirdPractice);
        add("Sprint Qualifying", "SprintQualifying", race.SprintQualifying);
        add("Sprint", "Sprint", race.Sprint);
        add("Qualifying", "Qualifying", race.Qualifying);
        add("Race", "Race", { date: race.date, time: race.time });
        return sessions;
    }
    async updateScheduleStates(races, allSessions, now) {
        if (races.length === 0) {
            return;
        }
        // Full season calendar — include sprint weekend flag so clients can distinguish
        const calendar = races.map(r => ({
            round: parseInt(r.round, 10),
            race_name: r.raceName,
            circuit: r.Circuit.circuitName,
            country: r.Circuit.Location.country,
            date: r.date,
            time: r.time,
            is_sprint_weekend: !!r.Sprint,
        }));
        await this.setStateAsync("schedule.calendar", { val: JSON.stringify(calendar, null, 2), ack: true });
        // Next race (keep current race as "next" for 3h after start — same as f1_sensor)
        const toUTC = (date, time) => {
            const t = time && (time.endsWith("Z") || time.includes("+")) ? time : `${time}Z`;
            return new Date(`${date}T${t}`);
        };
        const GRACE_MS = 3 * 60 * 60 * 1000;
        const nextRace = races.find(r => toUTC(r.date, r.time).getTime() + GRACE_MS > now.getTime());
        if (nextRace) {
            const raceDate = toUTC(nextRace.date, nextRace.time);
            const daysUntil = Math.max(0, Math.ceil((raceDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24)));
            await this.setStateAsync("schedule.next_race_name", { val: nextRace.raceName, ack: true });
            await this.setStateAsync("schedule.next_race_round", {
                val: parseInt(nextRace.round, 10),
                ack: true,
            });
            await this.setStateAsync("schedule.next_race_circuit", {
                val: nextRace.Circuit.circuitName,
                ack: true,
            });
            await this.setStateAsync("schedule.next_race_country", {
                val: nextRace.Circuit.Location.country,
                ack: true,
            });
            await this.setStateAsync("schedule.next_race_date", { val: raceDate.toISOString(), ack: true });
            await this.setStateAsync("schedule.next_race_countdown_days", { val: daysUntil, ack: true });
        }
        // weekend_json: always show the CURRENT weekend (by detectCurrentRound),
        // falling back to the next upcoming race if between weekends.
        const currentRound = this.detectCurrentRound(races, now);
        const weekendRound = currentRound ?? (nextRace ? parseInt(nextRace.round, 10) : null);
        if (weekendRound !== null) {
            const weekendSessions = allSessions.filter(s => s.round === weekendRound);
            await this.setStateAsync("schedule.weekend_json", {
                val: JSON.stringify(weekendSessions, null, 2),
                ack: true,
            });
        }
        // Next individual session
        const nextSession = allSessions.find(s => new Date(s.startUTC) > now);
        if (nextSession) {
            const startDate = new Date(nextSession.startUTC);
            const hoursUntil = Math.max(0, Math.ceil((startDate.getTime() - now.getTime()) / (1000 * 60 * 60)));
            await this.setStateAsync("schedule.next_session_name", { val: nextSession.name, ack: true });
            await this.setStateAsync("schedule.next_session_type", { val: nextSession.type, ack: true });
            await this.setStateAsync("schedule.next_session_date", {
                val: nextSession.startUTC,
                ack: true,
            });
            await this.setStateAsync("schedule.next_session_countdown_hours", {
                val: hoursUntil,
                ack: true,
            });
        }
    }
    // ── Live Session Detection ─────────────────────────────────────────────────
    detectLiveSession(sessions, now) {
        const PRE_MS = 30 * 60 * 1000; // 30 min pre-buffer
        const POST_MS = 10 * 60 * 1000; // 10 min post-buffer
        for (const session of sessions) {
            const start = new Date(new Date(session.startUTC).getTime() - PRE_MS);
            const end = new Date(new Date(session.endUTC).getTime() + POST_MS);
            if (now >= start && now <= end) {
                return session;
            }
        }
        return null;
    }
    /**
     * OpenF1 currently returns a restricted-access message while a live F1 session
     * is running. We use this as fallback signal when schedule times lag behind.
     */
    async detectLiveSessionViaOpenF1() {
        try {
            const res = await axios_1.default.get("https://api.openf1.org/v1/sessions?session_key=latest", {
                timeout: 5000,
                headers: { "User-Agent": "ioBroker.f1/1.0" },
                validateStatus: () => true,
            });
            const detail = res.data?.detail;
            if (typeof detail === "string" && detail.includes("Live F1 session in progress")) {
                return true;
            }
            // When the endpoint returns an array, no live-session lock is active.
            if (Array.isArray(res.data)) {
                return false;
            }
            return false;
        }
        catch (error) {
            this.log.debug(`OpenF1 live fallback unavailable: ${error instanceof Error ? error.message : String(error)}`);
            return false;
        }
    }
    isTerminalSessionStatus(status) {
        const normalized = status.toLowerCase();
        return ["ends", "ended", "finished", "finalised", "finalized", "inactive", "completed"].includes(normalized);
    }
    async handleSessionEnded(reason, endedSessionOverride) {
        const endedSession = this.currentLiveSession ?? endedSessionOverride ?? null;
        if (!endedSession) {
            return;
        }
        this.currentLiveSession = null;
        this.liveFallbackActive = false;
        // Prevent immediate OpenF1 fallback re-entry after session end.
        this.fallbackSuppressedUntilMs = Date.now() + 45 * 60 * 1000;
        await this.setStateAsync("live.is_live", { val: false, ack: true });
        await this.setStateAsync("live.session_part", { val: 0, ack: true });
        await this.setStateAsync("live.time_elapsed", { val: "00:00:00", ack: true });
        this.stopClockExtrapolation();
        if (this.ws) {
            this.disconnectSignalR(true);
        }
        const savedKey = `${endedSession.round}-${endedSession.type}`;
        if (savedKey !== this.lastSavedSession) {
            this.lastSavedSession = savedKey;
            this.log.info(`Session ended (${reason}): ${endedSession.name} (round ${endedSession.round}). Refreshing Jolpica data...`);
            await this.refreshJolpicaData();
            this.startPostSessionRefreshRetries(endedSession.round);
        }
    }
    async getApiRoundStatus() {
        let resultsRound = null;
        let standingsRound = null;
        try {
            const resultsData = await this.fetchErgast("/current/last/results.json?limit=1");
            const parsed = parseInt(String(resultsData?.MRData?.RaceTable?.round ?? ""), 10);
            if (!isNaN(parsed)) {
                resultsRound = parsed;
            }
        }
        catch {
            // non-critical in retry evaluation
        }
        try {
            const standingsData = await this.fetchErgast("/current/driverstandings.json?limit=1");
            const parsed = parseInt(String(standingsData?.MRData?.StandingsTable?.round ?? ""), 10);
            if (!isNaN(parsed)) {
                standingsRound = parsed;
            }
        }
        catch {
            // non-critical in retry evaluation
        }
        return { resultsRound, standingsRound };
    }
    startPostSessionRefreshRetries(targetRound) {
        if (targetRound <= 0 || this.isUnloading) {
            return;
        }
        if (this.postSessionRefreshTimeout) {
            this.clearTimeout(this.postSessionRefreshTimeout);
            this.postSessionRefreshTimeout = undefined;
        }
        this.postSessionTargetRound = targetRound;
        this.postSessionRefreshAttempts = 0;
        this.scheduleNextPostSessionRefresh();
    }
    scheduleNextPostSessionRefresh() {
        const targetRound = this.postSessionTargetRound;
        if (targetRound === null || this.isUnloading) {
            return;
        }
        const MAX_ATTEMPTS = 6;
        const RETRY_MS = 10 * 60 * 1000;
        if (this.postSessionRefreshAttempts >= MAX_ATTEMPTS) {
            this.log.info(`Post-session refresh window finished for round ${targetRound}. API may still be delayed.`);
            this.postSessionTargetRound = null;
            return;
        }
        this.postSessionRefreshTimeout = this.setTimeout(async () => {
            this.postSessionRefreshAttempts += 1;
            const attempt = this.postSessionRefreshAttempts;
            await this.refreshJolpicaData();
            const rounds = await this.getApiRoundStatus();
            const resultsReady = rounds.resultsRound !== null && rounds.resultsRound >= targetRound;
            const standingsReady = rounds.standingsRound !== null && rounds.standingsRound >= targetRound;
            if (resultsReady && standingsReady) {
                this.log.info(`Post-session refresh complete for round ${targetRound} (attempt ${attempt}): API data now available.`);
                this.postSessionTargetRound = null;
                this.postSessionRefreshTimeout = undefined;
                return;
            }
            this.log.info(`Post-session refresh attempt ${attempt}/${MAX_ATTEMPTS} for round ${targetRound}: results round=${rounds.resultsRound ?? "n/a"}, standings round=${rounds.standingsRound ?? "n/a"}`);
            this.scheduleNextPostSessionRefresh();
        }, RETRY_MS);
    }
    async applyLiveFallbackScheduleStates(now) {
        const sessionName = this.liveSessionName || this.currentLiveSession?.name || "Live Session";
        const sessionType = this.liveSessionType || this.currentLiveSession?.type || "Unknown";
        const sessionDate = this.liveSessionStartUTC || this.currentLiveSession?.startUTC || now.toISOString();
        await this.setStateAsync("schedule.next_session_name", { val: sessionName, ack: true });
        await this.setStateAsync("schedule.next_session_type", { val: sessionType, ack: true });
        await this.setStateAsync("schedule.next_session_date", { val: sessionDate, ack: true });
        await this.setStateAsync("schedule.next_session_countdown_hours", { val: 0, ack: true });
    }
    async updateProvisionalResultsFromLiveData(now) {
        const sessionLabel = (this.liveSessionName || this.currentLiveSession?.name || "").toLowerCase();
        let targetState = null;
        if (sessionLabel.includes("qualifying") && !sessionLabel.includes("sprint")) {
            targetState = "results.qualifying";
        }
        else if (sessionLabel.includes("sprint") && !sessionLabel.includes("qualifying")) {
            targetState = "results.sprint";
        }
        else if (sessionLabel.includes("race")) {
            targetState = "results.race";
        }
        if (!targetState) {
            return;
        }
        const round = this.liveMeetingRound ?? this.currentLiveSession?.round ?? 0;
        const raceName = this.liveMeetingName || this.currentLiveSession?.raceName || "";
        const liveStatus = String((await this.getStateAsync("live.session_status"))?.val ?? "");
        const provisional = [];
        for (const [num, info] of Object.entries(this.driverList)) {
            if (!info || typeof info !== "object") {
                continue;
            }
            const timing = this.timingData[num] ?? {};
            const position = parseInt(String(timing?.Position ?? 0), 10);
            const driverNumber = parseInt(String(info.RacingNumber ?? num), 10);
            if (isNaN(driverNumber)) {
                continue;
            }
            provisional.push({
                position: !isNaN(position) && position > 0 ? position : 999,
                driver_number: driverNumber,
                name_acronym: String(info.Tla ?? info.Abbreviation ?? ""),
                full_name: String(info.FullName ?? info.BroadcastName ?? ""),
                team_name: String(info.TeamName ?? ""),
                team_colour: String(info.TeamColour ?? info.TeamColor ?? "FFFFFF"),
                best_lap_time: this.parseLapTimeToSeconds(timing?.BestLapTime?.Value ?? timing?.LastLapTime?.Value),
                lap_count: parseInt(String(timing?.NumberOfLaps ?? timing?.Laps ?? 0), 10) || 0,
                status: liveStatus || undefined,
                race_name: raceName || undefined,
                round: round > 0 ? round : undefined,
            });
        }
        if (provisional.length === 0) {
            return;
        }
        provisional.sort((a, b) => a.position - b.position);
        await this.setStateAsync(targetState, {
            val: JSON.stringify(provisional, null, 2),
            ack: true,
        });
        await this.setStateAsync("results.last_update", { val: now.toISOString(), ack: true });
    }
    async checkLiveStatus() {
        try {
            // Reuse cached schedule — refreshJolpicaData updates it every hour.
            // Only fall back to a fresh fetch if the cache is empty (first run race).
            if (this.cachedRaces.length === 0) {
                this.cachedRaces = await this.fetchSchedule();
                this.cachedSessions = this.cachedRaces.flatMap(r => this.buildSessionsFromRace(r));
            }
            const races = this.cachedRaces;
            const allSessions = this.cachedSessions;
            const now = new Date();
            const prevSession = this.currentLiveSession;
            this.currentLiveSession = this.detectLiveSession(allSessions, now);
            this.liveFallbackActive = false;
            // Fallback for short-notice schedule changes: force live mode if OpenF1
            // reports an active session although the calendar times are outdated.
            if (!this.currentLiveSession &&
                now.getTime() >= this.fallbackSuppressedUntilMs &&
                (await this.detectLiveSessionViaOpenF1())) {
                const fallbackRound = this.detectCurrentRound(races, now) ?? 0;
                this.log.info("Live session detected via OpenF1 fallback (schedule may be outdated)");
                this.liveFallbackActive = true;
                this.currentLiveSession = {
                    name: "Live Session",
                    type: "Unknown",
                    startUTC: new Date(now.getTime() - 30 * 60 * 1000).toISOString(),
                    endUTC: new Date(now.getTime() + 2 * 60 * 60 * 1000).toISOString(),
                    round: fallbackRound,
                    raceName: "",
                    circuit: "",
                    country: "",
                };
            }
            if (this.currentLiveSession) {
                const sessionNameForState = this.liveFallbackActive && this.liveSessionName
                    ? this.liveSessionName
                    : this.currentLiveSession.name;
                await this.setStateAsync("live.is_live", { val: true, ack: true });
                await this.setStateAsync("live.session_name", {
                    val: sessionNameForState,
                    ack: true,
                });
                // Connect SignalR if not already connected or connecting
                if (!this.ws || this.ws.readyState === ws_1.default.CLOSED) {
                    // Cancel pending reconnect timer to avoid dual connection attempts
                    if (this.reconnectTimeout) {
                        this.clearTimeout(this.reconnectTimeout);
                        this.reconnectTimeout = undefined;
                    }
                    void this.connectSignalR();
                }
                if (this.liveFallbackActive) {
                    await this.applyLiveFallbackScheduleStates(now);
                    await this.updateProvisionalResultsFromLiveData(now);
                    await this.updateTimeElapsedState(now.getTime());
                }
            }
            else {
                this.liveFallbackActive = false;
                if (prevSession) {
                    await this.handleSessionEnded("schedule window ended", prevSession);
                }
                else {
                    await this.setStateAsync("live.is_live", { val: false, ack: true });
                    await this.setStateAsync("live.session_part", { val: 0, ack: true });
                    await this.setStateAsync("live.time_elapsed", { val: "00:00:00", ack: true });
                    this.stopClockExtrapolation();
                    if (this.ws) {
                        this.disconnectSignalR(true); // full reset: session ended, wipe driver metadata
                    }
                }
            }
            this.updateLiveCheckInterval(now, allSessions);
        }
        catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            this.log.warn(`Live check failed: ${msg}`);
        }
    }
    // ── SignalR Connection (F1 Live Timing) ───────────────────────────────────
    async connectSignalR() {
        if (this.wsConnecting) {
            return;
        }
        this.wsConnecting = true;
        this.coreHandshakeAck = false;
        this.coreSubscribeSent = false;
        // Clear all per-session caches before each new connection so that the
        // R-replay after /start always starts with a clean slate — prevents
        // duplicate race-control messages, team-radio captures, and pit stops
        // from being re-appended on reconnect.
        this.disconnectSignalR(false);
        try {
            // SignalR Core flow for live sessions:
            // OPTIONS (to obtain AWSALBCORS) -> POST negotiate -> WS connect -> protocol handshake -> Subscribe.
            const optRes = await this.ltApi.options(SIGNALR_CORE_NEGOTIATE_URL, {
                params: { negotiateVersion: "1" },
                validateStatus: () => true,
            });
            if (optRes.status >= 400) {
                this.log.debug(`SignalR Core OPTIONS returned ${optRes.status}; continuing with best effort`);
            }
            const setCookieHeader = optRes.headers["set-cookie"];
            const setCookie = Array.isArray(setCookieHeader)
                ? setCookieHeader
                : typeof setCookieHeader === "string"
                    ? [setCookieHeader]
                    : [];
            const awsAlbCors = setCookie.map(v => /AWSALBCORS=([^;]+)/.exec(v)?.[1]).find(Boolean) ?? "";
            const negotiateHeaders = {};
            if (awsAlbCors) {
                negotiateHeaders.Cookie = `AWSALBCORS=${awsAlbCors}`;
            }
            const negRes = await this.ltApi.post(SIGNALR_CORE_NEGOTIATE_URL, null, {
                params: { negotiateVersion: "1" },
                headers: negotiateHeaders,
            });
            const rawToken = String(negRes.data?.connectionToken ?? "").trim();
            if (!rawToken) {
                throw new Error("SignalR Core negotiate returned no connectionToken");
            }
            const wsHeaders = {};
            if (awsAlbCors) {
                wsHeaders.Cookie = `AWSALBCORS=${awsAlbCors}`;
            }
            this.ws = new ws_1.default(`${SIGNALR_CORE_WS_URL}?id=${encodeURIComponent(rawToken)}`, {
                headers: wsHeaders,
            });
            this.ws.on("open", () => {
                this.wsConnecting = false;
                this.log.info("F1 Live Timing: SignalR Core connected — sending handshake + Subscribe");
                // SignalR Core JSON protocol handshake (required before invocations)
                this.ws.send(`${JSON.stringify({ protocol: "json", version: 1 })}${SIGNALR_CORE_RECORD_SEP}`);
            });
            this.ws.on("message", (raw) => {
                let str;
                if (Buffer.isBuffer(raw)) {
                    str = raw.toString("utf8");
                }
                else if (Array.isArray(raw)) {
                    str = Buffer.concat(raw).toString("utf8");
                }
                else {
                    str = Buffer.from(raw).toString("utf8");
                }
                void this.handleWsMessage(str);
            });
            this.ws.on("close", () => {
                this.wsConnecting = false;
                this.log.info("F1 Live Timing: SignalR Core disconnected");
                this.ws = null;
                // Reconnect after 5s if still in live window
                if (this.currentLiveSession) {
                    this.reconnectTimeout = this.setTimeout(() => void this.connectSignalR(), 5000);
                }
            });
            this.ws.on("error", (err) => {
                this.wsConnecting = false;
                this.log.warn(`F1 Live Timing SignalR Core error: ${err.message}`);
            });
        }
        catch (error) {
            this.wsConnecting = false;
            const msg = error instanceof Error ? error.message : String(error);
            this.log.warn(`F1 Live Timing connect failed (SignalR Core): ${msg}`);
            if (this.currentLiveSession) {
                this.reconnectTimeout = this.setTimeout(() => void this.connectSignalR(), 15000);
            }
        }
    }
    sendCoreSubscribeOnce() {
        if (this.coreSubscribeSent || !this.ws || this.ws.readyState !== ws_1.default.OPEN) {
            return;
        }
        this.ws.send(`${JSON.stringify({
            type: 1,
            target: "Subscribe",
            arguments: [SUBSCRIBE_STREAMS],
            invocationId: "1",
        })}${SIGNALR_CORE_RECORD_SEP}`);
        this.coreSubscribeSent = true;
    }
    /**
     * Disconnect the WebSocket and cancel any pending reconnect.
     * Pass `fullReset=true` at session end to clear driver metadata;
     * omit (or pass false) for transient reconnects so driver names/teams survive.
     *
     * @param fullReset - When true, also clears driverList metadata.
     */
    disconnectSignalR(fullReset = false) {
        if (this.reconnectTimeout) {
            this.clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = undefined;
        }
        if (this.ws) {
            this.ws.removeAllListeners();
            this.ws.close();
            this.ws = null;
        }
        // Always reset per-lap caches so stale timing data doesn't persist
        this.timingData = {};
        this.timingAppData = {};
        this.tyreStintData = {};
        this.pitStopData = {};
        this.rcMessages = [];
        this.topThreeData = {};
        this.teamRadioCaptures = [];
        // Only wipe driver metadata (names, teams) on a full session end.
        // Transient reconnects keep it so the drivers list stays populated
        // while awaiting the next /start replay.
        if (fullReset) {
            this.driverList = {};
            this.sessionPath = "";
        }
    }
    // ── SignalR Message Processing ─────────────────────────────────────────────
    async handleWsMessage(raw) {
        const chunks = raw.includes(SIGNALR_CORE_RECORD_SEP) ? raw.split(SIGNALR_CORE_RECORD_SEP) : [raw];
        for (const chunk of chunks) {
            const message = chunk.trim();
            if (!message) {
                continue;
            }
            let payload;
            try {
                payload = JSON.parse(message);
            }
            catch {
                continue;
            }
            // SignalR Core handshake acknowledgement is an empty JSON object.
            if (!this.coreHandshakeAck &&
                payload &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                Object.keys(payload).length === 0) {
                this.coreHandshakeAck = true;
                this.sendCoreSubscribeOnce();
                continue;
            }
            // SignalR Core ping -> respond to keep the connection alive.
            if (payload?.type === 6) {
                if (this.ws && this.ws.readyState === ws_1.default.OPEN) {
                    this.ws.send(`${JSON.stringify({ type: 6 })}${SIGNALR_CORE_RECORD_SEP}`);
                }
                continue;
            }
            if (payload?.type === 7) {
                this.log.debug(`SignalR Core server closed stream: ${String(payload?.error ?? "no error")}`);
                continue;
            }
            // SignalR Core feed invocation: {"type":1,"target":"feed","arguments":[stream,data,...]}
            if (payload?.type === 1 && payload?.target === "feed" && Array.isArray(payload?.arguments)) {
                const [stream, data] = payload.arguments;
                if (typeof stream === "string") {
                    await this.handleStreamData(stream, data);
                }
                continue;
            }
            // Legacy/RPC-like replay shape (or translated core completion):
            if (payload?.R && typeof payload.R === "object") {
                for (const [stream, data] of Object.entries(payload.R)) {
                    await this.handleStreamData(stream, data);
                }
                continue;
            }
            // Legacy incremental updates: {"M":[{"M":"feed","A":[stream,data,...]}]}
            for (const msg of payload?.M ?? []) {
                if (msg.M !== "feed" || !Array.isArray(msg.A) || msg.A.length < 2) {
                    continue;
                }
                const [stream, data] = msg.A;
                await this.handleStreamData(stream, data);
            }
        }
    }
    async handleStreamData(stream, data) {
        this.log.debug(`[SignalR] stream: ${stream} data: ${JSON.stringify(data)?.slice(0, 200)}`);
        try {
            switch (stream) {
                case "TrackStatus":
                    await this.onTrackStatus(data);
                    break;
                case "SessionStatus":
                    await this.onSessionStatus(data);
                    break;
                case "SessionInfo":
                    await this.onSessionInfo(data);
                    break;
                case "WeatherData":
                    await this.onWeatherData(data);
                    break;
                case "LapCount":
                    await this.onLapCount(data);
                    break;
                case "ExtrapolatedClock":
                    await this.onExtrapolatedClock(data);
                    break;
                case "DriverList":
                    this.driverList = this.deepMerge(this.driverList, data);
                    await this.publishDrivers();
                    break;
                case "TimingData":
                    if (data?.SessionPart != null) {
                        await this.onSessionPart(data.SessionPart);
                    }
                    if (data?.Lines) {
                        this.timingData = this.deepMerge(this.timingData, data.Lines);
                        await this.publishDrivers();
                    }
                    break;
                case "TimingAppData":
                    if (data?.Lines) {
                        this.timingAppData = this.deepMerge(this.timingAppData, data.Lines);
                        await this.publishDrivers();
                    }
                    break;
                case "RaceControlMessages":
                    await this.onRaceControl(data);
                    break;
                case "TopThree":
                    await this.onTopThree(data);
                    break;
                case "TeamRadio":
                    await this.onTeamRadio(data);
                    break;
                case "PitStopSeries":
                    await this.onPitStops(data);
                    break;
                case "TyreStintSeries":
                    await this.onTyreStints(data);
                    break;
            }
            await this.setStateAsync("live.last_update", { val: new Date().toISOString(), ack: true });
        }
        catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            this.log.debug(`Stream ${stream} error: ${msg}`);
        }
    }
    async onTrackStatus(data) {
        const statusCode = String(data?.Status ?? "");
        if (!statusCode) {
            return;
        }
        const mapped = TRACK_STATUS_MAP[statusCode] ?? data?.Message ?? statusCode;
        await this.setStateAsync("live.track_status", { val: mapped, ack: true });
    }
    async onSessionStatus(data) {
        const status = String(data?.Status ?? data ?? "").trim();
        // Ignore empty or placeholder values that are not real F1 session statuses
        if (!status || status === "no_session") {
            return;
        }
        await this.setStateAsync("live.session_status", { val: status, ack: true });
        if (this.isTerminalSessionStatus(status)) {
            await this.handleSessionEnded(`live status=${status}`);
        }
    }
    async onSessionPart(part) {
        const num = parseInt(String(part), 10);
        if (!isNaN(num) && num >= 0 && num <= 3) {
            await this.setStateAsync("live.session_part", { val: num, ack: true });
        }
    }
    async onSessionInfo(data) {
        const name = String(data?.Name ?? data?.Type ?? "");
        if (name) {
            this.liveSessionName = name;
            await this.setStateAsync("live.session_name", { val: name, ack: true });
        }
        const type = String(data?.Type ?? "").trim();
        if (type) {
            this.liveSessionType = type;
        }
        const meetingRound = parseInt(String(data?.Meeting?.Number ?? ""), 10);
        if (!isNaN(meetingRound) && meetingRound > 0) {
            this.liveMeetingRound = meetingRound;
        }
        const meetingName = String(data?.Meeting?.Name ?? "").trim();
        if (meetingName) {
            this.liveMeetingName = meetingName;
        }
        const meetingCountry = String(data?.Meeting?.Country?.Name ?? data?.Meeting?.Country?.Code ?? "").trim();
        if (meetingCountry) {
            this.liveMeetingCountry = meetingCountry;
        }
        const startDateRaw = String(data?.StartDate ?? data?.GmtStart ?? "").trim();
        if (startDateRaw) {
            const parsed = new Date(startDateRaw);
            if (!isNaN(parsed.getTime())) {
                this.liveSessionStartUTC = parsed.toISOString();
                await this.updateTimeElapsedState(parsed.getTime());
            }
        }
        if (this.liveFallbackActive) {
            void this.applyLiveFallbackScheduleStates(new Date());
            void this.updateProvisionalResultsFromLiveData(new Date());
            void this.updateTimeElapsedState(new Date().getTime());
        }
        // Capture session path and load the static DriverList which has full metadata
        // (names, teams, colours) that may be absent from SignalR incremental updates.
        const path = String(data?.Path ?? "").trim();
        if (path && path !== this.sessionPath) {
            this.sessionPath = path;
            void this.fetchStaticDriverList(path);
        }
    }
    /**
     * Fetch the static DriverList JSON from the F1 live timing server.
     * This file contains full driver metadata (FullName, Tla, TeamName, TeamColour)
     * which is often absent from mid-session SignalR replays.
     *
     * @param path - Session path from SessionInfo, e.g. "2026/2026-05-04_Miami_Grand_Prix/2026-05-02_Sprint/"
     */
    async fetchStaticDriverList(path) {
        try {
            const url = `/static/${path}DriverList.json`;
            const res = await this.ltApi.get(url);
            const list = res.data;
            if (list && typeof list === "object") {
                this.driverList = this.deepMerge(this.driverList, list);
                this.log.debug(`Static DriverList loaded: ${Object.keys(list).length} drivers from ${path}`);
                await this.publishDrivers();
            }
        }
        catch (e) {
            if (axios_1.default.isAxiosError(e)) {
                const status = e.response?.status;
                if (status === 401 || status === 403) {
                    this.log.debug("Static DriverList access denied (401/403), continuing with SignalR DriverList data");
                    return;
                }
            }
            this.log.debug(`Static DriverList fetch failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
        }
    }
    async onWeatherData(data) {
        const weather = {
            air_temperature: parseFloat(data?.AirTemp || 0),
            track_temperature: parseFloat(data?.TrackTemp || 0),
            humidity: parseFloat(data?.Humidity || 0),
            pressure: parseFloat(data?.Pressure || 0),
            rainfall: parseFloat(data?.Rainfall || 0),
            wind_speed: parseFloat(data?.WindSpeed || 0),
            wind_direction: parseInt(String(data?.WindDirection || 0), 10),
        };
        await this.setStateAsync("live.weather", { val: JSON.stringify(weather, null, 2), ack: true });
    }
    async onLapCount(data) {
        if (data?.CurrentLap != null) {
            await this.setStateAsync("live.laps_current", {
                val: parseInt(String(data.CurrentLap), 10),
                ack: true,
            });
        }
        if (data?.TotalLaps != null) {
            await this.setStateAsync("live.laps_total", {
                val: parseInt(String(data.TotalLaps), 10),
                ack: true,
            });
        }
    }
    async onExtrapolatedClock(data) {
        const remainingStr = String(data?.Remaining ?? "");
        const utcStr = String(data?.Utc ?? "");
        const extrapolating = data?.Extrapolating === true;
        if (remainingStr) {
            await this.setStateAsync("live.time_remaining", { val: remainingStr, ack: true });
        }
        if (utcStr) {
            await this.setStateAsync("live.clock_utc", { val: utcStr, ack: true });
        }
        // Store reference point for client-side extrapolation
        if (utcStr && remainingStr) {
            this.clockRefUtcMs = new Date(utcStr).getTime();
            this.clockRefRemainingMs = this.parseRemainingToMs(remainingStr);
            this.clockExtrapolating = extrapolating;
            await this.updateTimeElapsedState(this.clockRefUtcMs);
        }
        if (extrapolating) {
            this.startClockExtrapolation();
        }
        else {
            this.stopClockExtrapolation();
        }
    }
    startClockExtrapolation() {
        if (this.clockInterval) {
            return;
        }
        this.clockInterval = this.setInterval(() => void this.tickClock(), 1000);
    }
    stopClockExtrapolation() {
        if (this.clockInterval) {
            this.clearInterval(this.clockInterval);
            this.clockInterval = undefined;
        }
    }
    async tickClock() {
        if (!this.clockExtrapolating) {
            this.stopClockExtrapolation();
            return;
        }
        const nowMs = Date.now();
        const elapsedMs = nowMs - this.clockRefUtcMs;
        const remainingMs = Math.max(0, this.clockRefRemainingMs - elapsedMs);
        await this.setStateAsync("live.time_remaining", {
            val: this.formatRemainingMs(remainingMs),
            ack: true,
        });
        await this.updateTimeElapsedState(nowMs);
        if (remainingMs === 0) {
            this.stopClockExtrapolation();
        }
    }
    parseRemainingToMs(str) {
        const parts = str.split(":");
        if (parts.length === 3) {
            return (parseInt(parts[0], 10) * 3600 + parseInt(parts[1], 10) * 60 + parseFloat(parts[2])) * 1000;
        }
        return 0;
    }
    getSessionStartMs() {
        const start = this.liveSessionStartUTC || this.currentLiveSession?.startUTC;
        if (!start) {
            return null;
        }
        const ms = new Date(start).getTime();
        return isNaN(ms) ? null : ms;
    }
    async updateTimeElapsedState(nowMs = Date.now()) {
        const startMs = this.getSessionStartMs();
        if (startMs === null) {
            return;
        }
        const elapsedMs = Math.max(0, nowMs - startMs);
        await this.setStateAsync("live.time_elapsed", {
            val: this.formatRemainingMs(elapsedMs),
            ack: true,
        });
    }
    formatRemainingMs(ms) {
        const totalSeconds = Math.floor(ms / 1000);
        const h = Math.floor(totalSeconds / 3600);
        const m = Math.floor((totalSeconds % 3600) / 60);
        const s = totalSeconds % 60;
        return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
    }
    async onRaceControl(data) {
        // Messages can come as object {"0": {...}, "1": {...}} or array
        const incoming = data?.Messages ? Object.values(data.Messages) : Array.isArray(data) ? data : [];
        if (incoming.length === 0) {
            return;
        }
        this.rcMessages.push(...incoming);
        if (this.rcMessages.length > LIVE_RACE_CONTROL_MAX) {
            this.rcMessages = this.rcMessages.slice(-LIVE_RACE_CONTROL_MAX);
        }
        await this.setStateAsync("live.race_control", {
            val: JSON.stringify(this.rcMessages, null, 2),
            ack: true,
        });
    }
    async onTopThree(data) {
        if (data?.SessionPart != null) {
            await this.onSessionPart(data.SessionPart);
        }
        if (!data?.Lines) {
            return;
        }
        if (Array.isArray(data.Lines)) {
            // Full replay: replace cache entirely, keyed by array index
            this.topThreeData = {};
            data.Lines.forEach((l, i) => {
                this.topThreeData[String(i)] = l;
            });
        }
        else if (typeof data.Lines === "object") {
            // Incremental update: merge into cache so partial updates don't wipe existing entries
            this.topThreeData = this.deepMerge(this.topThreeData, data.Lines);
        }
        const entries = Object.values(this.topThreeData).sort((a, b) => {
            const posA = parseInt(String(a?.Position ?? 99), 10);
            const posB = parseInt(String(b?.Position ?? 99), 10);
            return posA - posB;
        });
        if (entries.length === 0) {
            return;
        }
        const top3 = entries.slice(0, 3).map((l) => ({
            position: parseInt(String(l.Position ?? 0), 10),
            racing_number: String(l.RacingNumber ?? ""),
            full_name: String(l.FullName ?? ""),
            name_acronym: String(l.Tla ?? ""),
            team: String(l.Team ?? ""),
        }));
        await this.setStateAsync("live.top_three", { val: JSON.stringify(top3, null, 2), ack: true });
    }
    async onTeamRadio(data) {
        const incoming = data?.Captures ? Object.values(data.Captures) : Array.isArray(data) ? data : [];
        if (incoming.length === 0) {
            return;
        }
        this.teamRadioCaptures.push(...incoming);
        if (this.teamRadioCaptures.length > LIVE_TEAM_RADIO_MAX) {
            this.teamRadioCaptures = this.teamRadioCaptures.slice(-LIVE_TEAM_RADIO_MAX);
        }
        await this.setStateAsync("live.team_radio", {
            val: JSON.stringify(this.teamRadioCaptures, null, 2),
            ack: true,
        });
    }
    async onPitStops(data) {
        // R-replay may wrap all driver data under a top-level "Stops" key:
        //   {"Stops": {"1": {PitStop: [{...}]}, "3": {...}}}
        // Incremental updates send the driver map directly:
        //   {"1": [{...}], "3": [{...}], ...}
        const driverMap = data?.Stops && typeof data.Stops === "object" && !Array.isArray(data.Stops) ? data.Stops : data;
        for (const [num, raw] of Object.entries(driverMap ?? {})) {
            // Per-driver: array directly, or object with PitStop/Stops key
            const incoming = Array.isArray(raw)
                ? raw
                : raw && typeof raw === "object"
                    ? Object.values(raw.PitStop ?? raw.Stops ?? raw)
                    : [];
            if (incoming.length === 0) {
                continue;
            }
            // R-replay delivers all stops for this driver at once — use as replacement.
            // Incremental updates add individual stops — append to existing cache.
            const existing = this.pitStopData[num] ?? [];
            if (existing.length === 0) {
                this.pitStopData[num] = incoming.map(p => ({ racing_number: num, ...p }));
            }
            else {
                // Incremental: append only truly new stops (deduplicate by lap number)
                for (const p of incoming) {
                    const lapKey = p.Lap ?? p.lap ?? p.LapNumber;
                    const isDuplicate = lapKey != null && existing.some(e => (e.Lap ?? e.lap ?? e.LapNumber) === lapKey);
                    if (!isDuplicate) {
                        existing.push({ racing_number: num, ...p });
                    }
                }
            }
        }
        const allStops = Object.values(this.pitStopData).flat();
        if (allStops.length > 0) {
            await this.setStateAsync("live.pit_stops", {
                val: JSON.stringify(allStops, null, 2),
                ack: true,
            });
        }
    }
    async onTyreStints(data) {
        // R-replay wraps all driver data under a top-level "Stints" key:
        //   {"Stints": {"1": [{...}], "3": [{...}], ...}}
        // Incremental updates send the driver map directly:
        //   {"1": [{...}], "3": [{...}], ...}
        const driverMap = data?.Stints && typeof data.Stints === "object" && !Array.isArray(data.Stints) ? data.Stints : data;
        const tyres = [];
        for (const [num, raw] of Object.entries(driverMap ?? {})) {
            // Per-driver format: array of stint objects (both replay and incremental)
            // Older format seen in practice: {Stints: {"0": {...}, "1": {...}}}
            let stints;
            if (Array.isArray(raw)) {
                stints = raw;
            }
            else if (raw && typeof raw === "object") {
                const inner = raw.Stints;
                stints = inner ? Object.values(inner) : Object.values(raw);
            }
            else {
                continue;
            }
            if (stints.length === 0) {
                continue;
            }
            const current = stints[stints.length - 1];
            if (!current || typeof current !== "object") {
                continue;
            }
            // Merge into tyreStintData so incremental updates (e.g. only TotalLaps) don't wipe Compound/New
            this.tyreStintData[num] = { ...(this.tyreStintData[num] ?? {}), ...current };
        }
        for (const [num, merged] of Object.entries(this.tyreStintData)) {
            tyres.push({
                racing_number: num,
                compound: merged.Compound ?? "",
                total_laps: merged.TotalLaps ?? 0,
                is_new: merged.New ?? false,
            });
        }
        if (tyres.length > 0) {
            await this.setStateAsync("live.tyres", { val: JSON.stringify(tyres, null, 2), ack: true });
        }
    }
    /**
     * Merge DriverList + TimingData + TimingAppData into one `live.drivers` state.
     * This mirrors what f1_sensor does with its LiveDriversCoordinator.
     */
    async publishDrivers() {
        const drivers = [];
        const keys = new Set([
            ...Object.keys(this.driverList),
            ...Object.keys(this.timingData),
            ...Object.keys(this.timingAppData),
            ...Object.keys(this.tyreStintData),
        ]);
        for (const num of keys) {
            const info = this.driverList[num] ?? {};
            if (!info || typeof info !== "object") {
                continue;
            }
            const timing = this.timingData[num] ?? {};
            const appData = this.timingAppData[num] ?? {};
            // Tyre: prefer TyreStintSeries cache (has Compound), fall back to TimingAppData stints
            const tyreCached = this.tyreStintData[num];
            const appStints = appData?.Stints ? Object.values(appData.Stints) : [];
            const appStint = appStints.length > 0 ? appStints[appStints.length - 1] : null;
            const compound = tyreCached?.Compound ?? appStint?.Compound ?? null;
            const tyreLaps = tyreCached?.TotalLaps ?? appStint?.TotalLaps ?? null;
            const tyreNew = tyreCached?.New ?? appStint?.New ?? null;
            const position = parseInt(String(timing?.Position ?? 0), 10) || null;
            drivers.push({
                racing_number: info.RacingNumber ?? num,
                // FullName, Tla, TeamName, TeamColour come from the static DriverList.
                // BroadcastName is a fallback if the static fetch hasn't completed yet.
                full_name: info.FullName ?? info.BroadcastName ?? "",
                name_acronym: info.Tla ?? info.Abbreviation ?? "",
                team_name: info.TeamName ?? "",
                team_colour: info.TeamColour ?? info.TeamColor ?? "",
                position,
                gap_to_leader: timing?.GapToLeader ?? null,
                interval: timing?.IntervalToPositionAhead?.Value ?? null,
                last_lap_time: timing?.LastLapTime?.Value ?? null,
                tyre_compound: compound,
                tyre_laps: tyreLaps,
                tyre_new: tyreNew,
            });
        }
        if (drivers.length === 0) {
            return;
        }
        drivers.sort((a, b) => (a.position ?? 99) - (b.position ?? 99));
        await this.setStateAsync("live.drivers", {
            val: JSON.stringify(drivers, null, 2),
            ack: true,
        });
    }
    // ── Standings ─────────────────────────────────────────────────────────────
    async updateStandings() {
        const delays = [10000, 30000, 90000];
        for (let attempt = 0; attempt < 3; attempt++) {
            try {
                const [driverRes, constructorRes] = await Promise.all([
                    this.fetchErgast("/current/driverstandings.json?limit=100"),
                    this.fetchErgast("/current/constructorstandings.json?limit=100"),
                ]);
                const driverStandings = driverRes?.MRData?.StandingsTable?.StandingsLists?.[0]?.DriverStandings ?? [];
                const constructorStandings = constructorRes?.MRData?.StandingsTable?.StandingsLists?.[0]?.ConstructorStandings ?? [];
                if (driverStandings.length > 0) {
                    const drivers = driverStandings.map((s) => ({
                        position: parseInt(String(s.position), 10),
                        driver_number: parseInt(String(s.Driver.permanentNumber), 10),
                        full_name: `${s.Driver.givenName} ${s.Driver.familyName}`,
                        name_acronym: s.Driver.code ?? "",
                        team_name: s.Constructors?.[0]?.name ?? "",
                        team_colour: this.getTeamColour(s.Constructors?.[0]?.constructorId ?? ""),
                        points: parseFloat(String(s.points)),
                        wins: parseInt(String(s.wins), 10),
                    }));
                    await this.setStateAsync("standings.drivers", {
                        val: JSON.stringify(drivers, null, 2),
                        ack: true,
                    });
                }
                if (constructorStandings.length > 0) {
                    const teams = constructorStandings.map((s) => ({
                        position: parseInt(String(s.position), 10),
                        team_name: s.Constructor.name,
                        team_colour: this.getTeamColour(s.Constructor.constructorId),
                        points: parseFloat(String(s.points)),
                        wins: parseInt(String(s.wins), 10),
                    }));
                    await this.setStateAsync("standings.teams", {
                        val: JSON.stringify(teams, null, 2),
                        ack: true,
                    });
                }
                await this.setStateAsync("standings.last_update", {
                    val: new Date().toISOString(),
                    ack: true,
                });
                this.log.debug("Standings updated");
                return;
            }
            catch (error) {
                const msg = error instanceof Error ? error.message : String(error);
                if (attempt < 2) {
                    this.log.warn(`Standings fetch failed (attempt ${attempt + 1}/3): ${msg}. Retrying in ${delays[attempt] / 1000}s...`);
                    await new Promise(resolve => this.setTimeout(() => resolve(), delays[attempt]));
                    if (this.isUnloading) {
                        return;
                    }
                }
                else {
                    this.log.error(`Failed to update standings after 3 attempts: ${msg}`);
                }
            }
        }
    }
    // ── Results ───────────────────────────────────────────────────────────────
    /**
     * Determine the round number for the current (or most recent) race weekend
     * from the schedule. A weekend is considered active from 5 days before the
     * race through 1 day after. This is used as the authoritative round number
     * so we never show stale results from a previous race when a new weekend has
     * started but its main Race / Qualifying haven't happened yet.
     *
     * @param races - Season race list from Jolpica
     * @param now   - Current date
     */
    detectCurrentRound(races, now) {
        if (races.length === 0) {
            return null;
        }
        const BEFORE_MS = 5 * 24 * 60 * 60 * 1000; // weekend starts ~5 days before race day
        const AFTER_MS = 24 * 60 * 60 * 1000; // keep current for 1 day after race
        // Active weekend: today falls within [raceDate - 5d, raceDate + 1d]
        for (const race of races) {
            // race.time already contains the timezone (e.g. "20:00:00Z") — do NOT append Z
            const raceDate = new Date(`${race.date}T${race.time ?? "14:00:00Z"}`);
            if (now >= new Date(raceDate.getTime() - BEFORE_MS) && now <= new Date(raceDate.getTime() + AFTER_MS)) {
                return parseInt(race.round, 10);
            }
        }
        // Between weekends: use the most recently completed race
        const past = races.filter(r => new Date(`${r.date}T${r.time ?? "14:00:00Z"}`) < now);
        if (past.length > 0) {
            return parseInt(past[past.length - 1].round, 10);
        }
        return null;
    }
    /**
     * Return true if a scheduled session has ended (start time + bufferMinutes has passed).
     * The buffer accounts for session duration and API publishing delay.
     * Returns false for undefined sessions (e.g. FP2/FP3 on sprint weekends).
     *
     * @param session       - Session date/time from Jolpica schedule
     * @param now           - Current date
     * @param bufferMinutes - Minutes after start to consider the session done (default 90)
     */
    sessionHasPassed(session, now, bufferMinutes = 90) {
        if (!session) {
            return false;
        }
        // session.time already contains the timezone (e.g. "16:00:00Z") — do NOT append Z
        const timeStr = session.time || "12:00:00Z";
        const sessionDate = new Date(`${session.date}T${timeStr}`);
        if (isNaN(sessionDate.getTime())) {
            return false;
        }
        return now.getTime() > sessionDate.getTime() + bufferMinutes * 60 * 1000;
    }
    async updateLatestResults(races = []) {
        const wrap = async (label, fn) => {
            try {
                await fn();
            }
            catch (e) {
                this.log.warn(`Results [${label}] failed: ${e instanceof Error ? e.message : String(e)}`);
            }
        };
        const now = new Date();
        const scheduleRound = this.detectCurrentRound(races, now);
        this.log.debug(`Schedule round: ${scheduleRound ?? "unknown"}`);
        if (scheduleRound == null) {
            this.log.debug("No current round from schedule — skipping results update");
            await this.setStateAsync("results.last_update", { val: now.toISOString(), ack: true });
            return;
        }
        const entry = races.find(r => parseInt(r.round, 10) === scheduleRound);
        if (!entry) {
            this.log.debug(`No race entry for round ${scheduleRound}`);
            await this.setStateAsync("results.last_update", { val: now.toISOString(), ack: true });
            return;
        }
        // Each session is loaded individually — only when its scheduled time + buffer has passed.
        // Note: FP1/FP2/FP3 and sprint qualifying are not available via Jolpica/Ergast.
        // ── Sprint (sprint weekends only) ────────────────────────────────────────
        if (this.sessionHasPassed(entry.Sprint, now)) {
            await wrap("sprint", () => this.updateSprintResults(scheduleRound));
        }
        else {
            await this.setStateAsync("results.sprint", { val: null, ack: true });
        }
        // ── Qualifying ───────────────────────────────────────────────────────────
        if (this.sessionHasPassed(entry.Qualifying, now)) {
            await wrap("qualifying", () => this.updateQualifyingResults(scheduleRound));
        }
        else {
            await this.setStateAsync("results.qualifying", { val: null, ack: true });
        }
        // ── Race (buffer 180 min: ~120 min race duration + ~60 min API publishing delay) ──
        const raceSession = { date: entry.date, time: entry.time ?? "14:00:00Z" };
        if (this.sessionHasPassed(raceSession, now, 180)) {
            await wrap("race", () => this.updateRaceResults(scheduleRound));
        }
        else {
            await this.setStateAsync("results.race", { val: null, ack: true });
        }
        await this.setStateAsync("results.last_update", { val: now.toISOString(), ack: true });
        this.log.info("Results updated");
    }
    async updateRaceResults(expectedRound) {
        const data = await this.fetchErgast("/current/last/results.json?limit=100");
        const race = data?.MRData?.RaceTable?.Races?.[0];
        if (!race) {
            this.log.debug("No race results from Ergast");
            return null;
        }
        const round = parseInt(race.round, 10);
        if (expectedRound !== undefined && round !== expectedRound) {
            this.log.debug(`Race results are for round ${round}, expected ${expectedRound} — clearing stale data`);
            await this.setStateAsync("results.race", { val: null, ack: true });
            return null;
        }
        const results = race.Results.map(r => ({
            position: parseInt(r.positionText, 10) || 0,
            driver_number: parseInt(r.number, 10),
            name_acronym: r.Driver.code ?? "",
            full_name: `${r.Driver.givenName} ${r.Driver.familyName}`,
            team_name: r.Constructor.name,
            team_colour: this.getTeamColour(r.Constructor.constructorId),
            best_lap_time: this.parseLapTimeToSeconds(r.FastestLap?.Time?.time),
            lap_count: parseInt(r.laps, 10),
            status: r.status,
            race_name: race.raceName ?? "",
            round,
        }));
        await this.setStateAsync("results.race", { val: JSON.stringify(results, null, 2), ack: true });
        return round;
    }
    async updateQualifyingResults(expectedRound) {
        const data = await this.fetchErgast("/current/last/qualifying.json?limit=100");
        const race = data?.MRData?.RaceTable?.Races?.[0];
        if (!race) {
            this.log.debug("No qualifying results from Ergast");
            return null;
        }
        const round = parseInt(race.round, 10);
        if (expectedRound !== undefined && round !== expectedRound) {
            this.log.debug(`Qualifying results are for round ${round}, expected ${expectedRound} — clearing stale data`);
            await this.setStateAsync("results.qualifying", { val: null, ack: true });
            return null;
        }
        const results = race.QualifyingResults.map(r => ({
            position: parseInt(r.position, 10),
            driver_number: parseInt(r.number, 10),
            name_acronym: r.Driver.code ?? "",
            full_name: `${r.Driver.givenName} ${r.Driver.familyName}`,
            team_name: r.Constructor.name,
            team_colour: this.getTeamColour(r.Constructor.constructorId),
            best_lap_time: this.parseLapTimeToSeconds(r.Q3 ?? r.Q2 ?? r.Q1),
            lap_count: 0,
            q1: r.Q1,
            q2: r.Q2,
            q3: r.Q3,
            race_name: race.raceName ?? "",
            round,
        }));
        await this.setStateAsync("results.qualifying", {
            val: JSON.stringify(results, null, 2),
            ack: true,
        });
        return round;
    }
    /**
     * Fetch sprint results for a specific round.
     * Jolpica returns all season sprints via /current/sprint.json — we filter by round
     * so we never show a stale sprint from a previous weekend.
     *
     * @param currentRound - The race round number to load sprint results for.
     */
    async updateSprintResults(currentRound) {
        const data = await this.fetchErgast("/current/sprint.json?limit=100");
        const races = data?.MRData?.RaceTable?.Races ?? [];
        // Only use sprint data that matches the current round
        const race = races.find(r => parseInt(r.round, 10) === currentRound);
        if (!race) {
            this.log.debug(`No sprint results for round ${currentRound} (may not be available yet)`);
            await this.setStateAsync("results.sprint", { val: null, ack: true });
            return;
        }
        const results = race.SprintResults.map(r => ({
            position: parseInt(r.positionText, 10) || 0,
            driver_number: parseInt(r.number, 10),
            name_acronym: r.Driver.code ?? "",
            full_name: `${r.Driver.givenName} ${r.Driver.familyName}`,
            team_name: r.Constructor.name,
            team_colour: this.getTeamColour(r.Constructor.constructorId),
            best_lap_time: null,
            lap_count: parseInt(r.laps, 10),
            status: r.status,
            race_name: race.raceName ?? "",
            round: currentRound,
        }));
        await this.setStateAsync("results.sprint", { val: JSON.stringify(results, null, 2), ack: true });
    }
    // ── Helpers ───────────────────────────────────────────────────────────────
    /**
     * Deep merge two plain objects (for SignalR incremental updates)
     *
     * @param target - The target object to merge into
     * @param source - The source object to merge from
     */
    deepMerge(target, source) {
        const result = { ...target };
        for (const [key, val] of Object.entries(source)) {
            if (val !== null &&
                typeof val === "object" &&
                !Array.isArray(val) &&
                result[key] !== null &&
                typeof result[key] === "object" &&
                !Array.isArray(result[key])) {
                result[key] = this.deepMerge(result[key], val);
            }
            else {
                result[key] = val;
            }
        }
        return result;
    }
    parseLapTimeToSeconds(timeStr) {
        if (!timeStr) {
            return null;
        }
        const parts = timeStr.split(":");
        if (parts.length === 2) {
            return parseInt(parts[0], 10) * 60 + parseFloat(parts[1]);
        }
        const val = parseFloat(timeStr);
        return isNaN(val) ? null : val;
    }
    getTeamColour(constructorId) {
        const colours = {
            mercedes: "00D2BE",
            ferrari: "E8002D",
            red_bull: "3671C6",
            mclaren: "FF8000",
            alpine: "0093CC",
            aston_martin: "229971",
            haas: "B6BABD",
            alphatauri: "6692FF",
            rb: "6692FF",
            williams: "64C4FF",
            sauber: "52E252",
            kick_sauber: "52E252",
            audi: "52E252",
        };
        return colours[constructorId] ?? "FFFFFF";
    }
}
if (require.main !== module) {
    module.exports = (options) => new F1(options);
}
else {
    (() => new F1())();
}
//# sourceMappingURL=main.js.map