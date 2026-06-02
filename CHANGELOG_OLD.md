# Older Changelog Entries

<!--
    Older changelog entries are stored here.
    This file is supported by @alcalzone/releasescript.
-->

### 0.1.7 (2026-05-03)

- (bloop) Added robust post-race refresh retry logic (6 attempts every 10 minutes)
- (bloop) Added API round consistency monitoring for standings/results sync
- (bloop) Improved post-session refresh flow with detailed retry logging

### 0.1.6 (2026-05-02)

- (bloop) Removed `results.fp1`, `results.fp2`, `results.fp3` states — Jolpica/Ergast API does not provide practice session results
- (bloop) Removed `results.sprint_qualifying` state — Jolpica/Ergast API does not expose a sprint qualifying endpoint (HTTP 400)

### 0.1.5 (2026-05-02)

- (bloop) Fixed per-session result loading — each session is loaded independently when its scheduled time has passed
- (bloop) Fixed race result buffer to 180 min (was 90 min) to account for race duration + API publishing delay
- (bloop) Fixed `weekend_json` to show the currently active race weekend instead of always the next upcoming race
- (bloop) Fixed `checkLiveStatus` to avoid re-fetching the Jolpica schedule API every 60 s (uses cached hourly data)
- (bloop) Fixed sprint qualifying and practice states to be explicitly `null` when the API endpoint is unavailable
- (bloop) Fixed double-Z date parsing bug (`new Date("...Z"+"Z")` -> Invalid Date) which caused round detection to always return null
- (bloop) Added `is_sprint_weekend` flag in `schedule.calendar` JSON entries
- (bloop) Removed unused internal `SIGNALR_BASE` constant

### 0.1.4 (2026-04-15)

- (bloop) Removed VIS1 widgets (development discontinued)

### 0.1.3 (2026-03-29)

- (bloop) Complete adapter rewrite — new clean 4-channel data structure
- (bloop) Replaced OpenF1 REST polling with F1 Live Timing SignalR WebSocket (real-time push)
- (bloop) Replaced OpenF1 schedule with Jolpica/Ergast API (more reliable, works outside race weekends)
- (bloop) Added automatic fallback from Jolpica to ergast.com on connectivity issues
- (bloop) Added 404-safe result fetching — partial failures no longer block other results
- (bloop) New data points: time_remaining, time_elapsed, laps_current, top_three, team_radio, full season calendar
- (bloop) Live session detection via schedule timing (±30 min window)
- (bloop) Automatic result & standings refresh on session end
- (bloop) Removed telemetry/car-data/location endpoints (not available outside active sessions)

### 0.1.2 (2026-03-23)

- (bloop) Widget development (discontinued in 0.1.4)

### 0.1.1 (2026-03-22)

- (bloop) Removed unused widgets
- (bloop) Fixed repository checker findings for Dependabot and CI
- (bloop) Added missing translations and maintenance metadata

### 0.1.0 (2026-03-15)

- (bloop) Initial release
- (bloop) Live F1 data from OpenF1 API
- (bloop) Next race info, standings, live session data
