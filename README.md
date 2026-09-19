# FastAndSlow

> **Jev reacts. Devin reasons. FastAndSlow acts.**

A simulated autonomous wildfire incident-response sandbox. It replays baked historical Deepfire data; every drone, warning, route, and incident action is simulated.

## Safety boundary

**AUTONOMY SANDBOX MODE — Historical Deepfire replay + simulated actions only. No real-world communications, aircraft, dispatch, or public-safety actions occur.**

The UI keeps this disclaimer visible and preserves `[SIMULATED]` in the audit view. The runtime has no integration with communications, emergency dispatch, public warning, or aircraft systems.

## Simulation behavior

The replay scores only hotspot evidence available at the current simulated instant. Verification drones receive standoff observation routes rather than fire-centroid destinations; the backend samples candidate route segments against an uncertainty buffer and vetoes unsafe paths. Because the historical Deepfire fixtures currently have no spread ensembles, forecast arrival/confidence are shown as unavailable instead of inferred from hotspots. A deterministic sequence of incident, scan, buffer-expansion, reroute, and safe-egress events makes policy behavior repeatable in demos; reset the timeline and select `16×` for the short demo arc.

## Stack

- Vite + React + TypeScript + Tailwind
- PixiJS v8 pixel-tile renderer with nearest-neighbor scaling
- Zustand for Phase 1–2 local UI state
- Rust/axum authoritative simulation API
- SQLite audit event store via rusqlite
- Async Cognition Devin API client behind a deterministic policy engine

## Run

```bash
npm install
cp .env.example .env # then fill server-side credentials
make up
```

This starts the frontend at `http://127.0.0.1:5174` and backend at `http://127.0.0.1:8787`. Vite proxies `/sim/*` to axum.

```bash
make status       # process and readiness status
make logs         # follow both logs; Ctrl-C does not stop services
make restart      # restart both services
make down         # safely stop services started by make
make down-force   # additionally stop listeners occupying ports 5174/8787
make help         # list all targets
```

Override the frontend port when needed with `make up FRONTEND_PORT=5175`. The frontend also works standalone via `npm run dev`; it uses static baked fixtures or deterministic demo fixtures if the backend is unavailable.

## Bake real Deepfire fixtures

Credentials are read only by the disposable Node script. No Deepfire credential is bundled into the frontend.

```bash
# Safe discovery pass: clusters, complete hotspot history, perimeters, values at risk
npm run bake -- --discover-only

# Full pass: additionally requests 12h / 10-member spread simulations
npm run bake

# One target only
npm run bake -- --discover-only --target avila-burgohondo
```

Outputs are written under `public/fixtures/`. Runtime code makes **no live Deepfire calls**.

### Current Deepfire limitation

The five historical OGC datasets bake successfully. As of this build, `POST /v1/fire-spread/simulations` rejects these July clusters because it permits at most a 168-hour hotspot lookback from the current date (`422: No cluster hotspots match the selected sources and lookback`). The baker intentionally fails rather than presenting fabricated polygons as Deepfire simulation output. When spread output is absent, the map explicitly renders an **OBSERVED HEAT FOOTPRINT** tile overlay from accumulated real hotspot detections; arrival and ensemble-confidence values remain unavailable.

## Backend API

| Method | Route | Purpose |
|---|---|---|
| GET | `/health` | Health/mode |
| GET | `/sim/fires` | Baked fixtures |
| GET | `/sim/state` | Authoritative sim mirror |
| POST | `/sim/select/:cluster_id` | Start/reset a historical replay |
| POST | `/sim/control` | Play, scrub, speed |
| GET | `/sim/audit` | Last 250 SQLite audit events |
| POST | `/sim/devin/trigger/:incident_id` | Freeze state, upload fixture, create session |
| POST | `/sim/devin/sessions/:id/redirect` | Mid-session wind-shift redirect |

## Devin setup

1. Create a Cognition service user with `ManageOrgSessions`.
2. Set `DEVIN_API_KEY`.
3. Obtain the org id via `GET /v3/enterprise/organizations`; set `DEVIN_ORG_ID`.
4. Create one reusable wildfire incident forecast playbook; set `DEVIN_PLAYBOOK_ID`.
5. Optionally set `DEVIN_MAX_ACU` (default `2`).

The backend uploads the frozen fixture, creates an asynchronous session with a required action schema, polls structured output, logs each proposal, and runs it through `policy.rs`. Approved and vetoed decisions are separate SQLite events. A 90-second no-action timeout falls back to deterministic Jev verification logic.

## Verification

```bash
npm run build
cargo check --manifest-path backend/Cargo.toml
curl http://127.0.0.1:8787/health
```

## Data notes

`valuesAtRisk` targets are manually authored because VaR is not currently exposed by Deepfire. Their lead times are fixture constants. Reported burned areas are contextual metadata and are not inferred from hotspot counts.
