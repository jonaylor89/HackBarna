mod devin;
mod models;
mod policy;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{Duration, Utc};
use devin::DevinClient;
use models::*;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{sync::RwLock, time};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

type ApiError = (StatusCode, String);
type ApiResult<T> = Result<Json<T>, ApiError>;

struct AppState {
    sim: RwLock<SimState>,
    fixtures: Vec<Value>,
    fixture_files: Vec<(String, PathBuf)>,
    audit: Mutex<Connection>,
    devin: Option<DevinClient>,
}

fn audit_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS audit_events (
        id TEXT PRIMARY KEY, sim_tick INTEGER NOT NULL, timestamp TEXT NOT NULL,
        actor TEXT NOT NULL, action_type TEXT NOT NULL, target TEXT NOT NULL,
        reason TEXT NOT NULL, status TEXT NOT NULL, payload_json TEXT NOT NULL
    ); CREATE INDEX IF NOT EXISTS audit_tick ON audit_events(sim_tick);",
    )
}

fn load_fixtures() -> (Vec<Value>, Vec<(String, PathBuf)>) {
    let dir = PathBuf::from("public/fixtures");
    let index: Vec<Value> = fs::read_to_string(dir.join("index.json"))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let mut fixtures = vec![];
    let mut files = vec![];
    for entry in index {
        let id = entry
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let filename = entry
            .get("file")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                entry
                    .get("slug")
                    .and_then(Value::as_str)
                    .map(|s| format!("{s}.json"))
            })
            .unwrap_or_else(|| format!("{id}.json"));
        let path = dir.join(filename);
        if let Ok(raw) = fs::read_to_string(&path) {
            if let Ok(value) = serde_json::from_str(&raw) {
                files.push((id, path));
                fixtures.push(value);
            }
        }
    }
    (fixtures, files)
}

fn soft_norm(value: f64, reference: f64) -> f64 {
    (1.0 - (-value / reference).exp()).clamp(0.0, 1.0)
}

fn visible_hotspots<'a>(f: &'a Value, hour: f64) -> Vec<&'a Value> {
    let cluster = &f["cluster"];
    let Some(start) = cluster
        .get("firstObserved")
        .and_then(Value::as_str)
        .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok())
    else {
        return vec![];
    };
    let Some(end) = cluster
        .get("lastObserved")
        .and_then(Value::as_str)
        .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok())
    else {
        return vec![];
    };
    let cutoff = start
        + Duration::milliseconds(
            ((end - start).num_milliseconds() as f64 * (hour / 12.0).clamp(0.0, 1.0)) as i64,
        );
    f.get("hotspots")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|h| {
                    h.get("observedAt")
                        .and_then(Value::as_str)
                        .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok())
                        .is_some_and(|t| t <= cutoff)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn heat_geometry(f: &Value, hour: f64) -> Option<([f64; 2], f64)> {
    let hotspots = visible_hotspots(f, hour);
    if hotspots.is_empty() {
        return None;
    }
    let points = hotspots
        .iter()
        .filter_map(|h| {
            let a = h.get("location")?.as_array()?;
            Some([a.first()?.as_f64()?, a.get(1)?.as_f64()?])
        })
        .collect::<Vec<_>>();
    if points.is_empty() {
        return None;
    }
    let center = [
        points.iter().map(|p| p[0]).sum::<f64>() / points.len() as f64,
        points.iter().map(|p| p[1]).sum::<f64>() / points.len() as f64,
    ];
    let radius = points
        .iter()
        .map(|p| ((p[0] - center[0]).powi(2) + (p[1] - center[1]).powi(2)).sqrt())
        .fold(0.0, f64::max);
    Some((center, radius))
}

fn path_risk(path: &[[f64; 2]], center: [f64; 2], radius: f64) -> f64 {
    if path.len() < 2 {
        return 0.0;
    }
    let danger = radius + 0.035;
    let mut closest = f64::MAX;
    for pair in path.windows(2) {
        for i in 0..=20 {
            let t = i as f64 / 20.0;
            let p = [
                pair[0][0] + (pair[1][0] - pair[0][0]) * t,
                pair[0][1] + (pair[1][1] - pair[0][1]) * t,
            ];
            closest = closest.min(((p[0] - center[0]).powi(2) + (p[1] - center[1]).powi(2)).sqrt());
        }
    }
    if closest <= danger {
        1.0
    } else {
        (1.0 - (closest - danger) / 0.08).clamp(0.0, 1.0)
    }
}

fn safe_route(home: [f64; 2], center: [f64; 2], radius: f64, east: bool) -> Vec<[f64; 2]> {
    let standoff = (radius + 0.09).max(0.11);
    let side = if east { 1.0 } else { -1.0 };
    let observation = [center[0] + side * standoff, center[1] - standoff * 0.35];
    let safe_lat = center[1] - standoff;
    let departure = [home[0], safe_lat.min(home[1])];
    let corridor = [observation[0], safe_lat];
    vec![home, departure, corridor, observation]
}

fn jev_signals(f: &Value, hour: f64, drones: &[Drone]) -> Signals {
    let hotspots = visible_hotspots(f, hour);
    let count = hotspots.len() as f64;
    let high = hotspots
        .iter()
        .filter(|h| h.get("confidenceTier").and_then(Value::as_str) == Some("HIGH"))
        .count() as f64;
    let avg_frp = if count > 0.0 {
        hotspots
            .iter()
            .filter_map(|h| h.get("fireRadiativePower").and_then(Value::as_f64))
            .sum::<f64>()
            / count
    } else {
        0.0
    };
    let persistence = if hotspots.len() < 2 {
        0.0
    } else {
        let first = hotspots
            .first()
            .and_then(|h| h.get("observedAt"))
            .and_then(Value::as_str)
            .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok());
        let last = hotspots
            .last()
            .and_then(|h| h.get("observedAt"))
            .and_then(Value::as_str)
            .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok());
        match (first, last) {
            (Some(a), Some(b)) => (b - a).num_minutes().max(0) as f64 / 60.0,
            _ => 0.0,
        }
    };
    let incident_confidence = (0.30 * soft_norm(count, 40.0)
        + 0.30 * if count > 0.0 { high / count } else { 0.0 }
        + 0.20 * soft_norm(avg_frp, 60.0)
        + 0.20 * soft_norm(persistence, 48.0))
    .clamp(0.0, 1.0);
    let risk = heat_geometry(f, hour)
        .map(|(c, r)| {
            drones
                .iter()
                .map(|d| {
                    let mut remaining = vec![d.position];
                    remaining.extend(d.path.iter().skip(d.path_index + 1).copied());
                    path_risk(&remaining, c, r)
                })
                .fold(0.0, f64::max)
        })
        .unwrap_or(0.0);
    let lead = f
        .get("valuesAtRisk")
        .and_then(Value::as_array)
        .and_then(|x| x.first())
        .and_then(|x| x.get("preparationLeadMinutes"))
        .and_then(Value::as_f64);
    Signals {
        incident_confidence,
        path_risk: risk,
        conservative_arrival_minutes: None,
        preparation_lead_minutes: lead,
        forecast_confidence: None,
    }
}

fn make_drones(center: [f64; 2]) -> Vec<Drone> {
    [
        ("EMBER", [-0.45, -0.42]),
        ("KITE", [0.55, -0.40]),
        ("NOVA", [-0.50, 0.50]),
    ]
    .into_iter()
    .map(|(id, o)| {
        let home = [center[0] + o[0], center[1] + o[1]];
        Drone {
            id: id.into(),
            position: home,
            home,
            path: vec![home],
            path_index: 0,
            status: "idle".into(),
        }
    })
    .collect()
}

fn new_action(
    tick: u64,
    actor: Actor,
    action_type: &str,
    target: &str,
    reason: &str,
    confidence: f64,
    status: ActionStatus,
    payload: Value,
) -> SimAction {
    SimAction {
        id: Uuid::new_v4().to_string(),
        sim_tick: tick,
        timestamp: Utc::now().to_rfc3339(),
        actor,
        action_type: action_type.into(),
        target: target.into(),
        reason: reason.into(),
        confidence,
        status,
        params: payload,
        simulated: true,
    }
}

fn write_audit(state: &AppState, action: &SimAction) {
    let conn = state.audit.lock().expect("audit mutex poisoned");
    let _ = conn.execute("INSERT INTO audit_events (id,sim_tick,timestamp,actor,action_type,target,reason,status,payload_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)", params![
        action.id, action.sim_tick, action.timestamp, format!("{:?}",action.actor).to_uppercase(), action.action_type,
        action.target, action.reason, format!("{:?}",action.status).to_uppercase(), action.params.to_string()
    ]);
}

async fn health() -> Json<Value> {
    Json(json!({"ok":true,"service":"fastandslow","mode":"AUTONOMY_SANDBOX"}))
}
async fn fires(State(s): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({"fires":s.fixtures}))
}
async fn sim_state(State(s): State<Arc<AppState>>) -> Json<SimState> {
    Json(s.sim.read().await.clone())
}

async fn select_fire(
    Path(id): Path<String>,
    State(s): State<Arc<AppState>>,
) -> ApiResult<SimState> {
    // Browser reloads must not reset the sim or create duplicate paid sessions.
    {
        let sim = s.sim.read().await;
        if sim.fire_id.as_deref() == Some(&id) && !sim.incidents.is_empty() {
            return Ok(Json(sim.clone()));
        }
    }
    let fixture = s
        .fixtures
        .iter()
        .find(|f| f["cluster"]["id"].as_str() == Some(&id))
        .ok_or((StatusCode::NOT_FOUND, "unknown fire".into()))?;
    let c = fixture["cluster"]["centroid"].as_array().ok_or((
        StatusCode::UNPROCESSABLE_ENTITY,
        "fixture has no centroid".into(),
    ))?;
    let center = [c[0].as_f64().unwrap_or(0.0), c[1].as_f64().unwrap_or(0.0)];
    let mut sim = s.sim.write().await;
    *sim = SimState {
        fire_id: Some(id),
        drones: make_drones(center),
        playing: true,
        ..SimState::default()
    };
    Ok(Json(sim.clone()))
}

async fn control(
    State(s): State<Arc<AppState>>,
    Json(req): Json<ControlRequest>,
) -> Json<SimState> {
    let mut sim = s.sim.write().await;
    if let Some(v) = req.playing {
        sim.playing = v;
    }
    if let Some(v) = req.hour {
        sim.hour = v.clamp(0.0, 12.0);
        if v <= 0.0 {
            sim.tick = 0;
            sim.actions.clear();
            sim.incidents.clear();
            sim.devin_sessions.clear();
            if let Some(center) = sim
                .fire_id
                .as_ref()
                .and_then(|id| {
                    s.fixtures
                        .iter()
                        .find(|f| f["cluster"]["id"].as_str() == Some(id))
                })
                .and_then(|f| f["cluster"]["centroid"].as_array())
                .map(|c| [c[0].as_f64().unwrap_or(0.0), c[1].as_f64().unwrap_or(0.0)])
            {
                sim.drones = make_drones(center)
            }
        }
    }
    if let Some(v) = req.speed {
        sim.speed = v.clamp(0.1, 16.0);
    }
    Json(sim.clone())
}

async fn audit_events(State(s): State<Arc<AppState>>) -> ApiResult<Value> {
    let conn = s
        .audit
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "audit lock".into()))?;
    let mut stmt=conn.prepare("SELECT id,sim_tick,timestamp,actor,action_type,target,reason,status,payload_json FROM audit_events ORDER BY rowid DESC LIMIT 250")
        .map_err(internal)?;
    let rows=stmt.query_map([],|r|Ok(json!({"id":r.get::<_,String>(0)?,"simTick":r.get::<_,u64>(1)?,"timestamp":r.get::<_,String>(2)?,"actor":r.get::<_,String>(3)?,"actionType":r.get::<_,String>(4)?,"target":r.get::<_,String>(5)?,"reason":r.get::<_,String>(6)?,"status":r.get::<_,String>(7)?,"payload":serde_json::from_str::<Value>(&r.get::<_,String>(8)?).unwrap_or(Value::Null)}))).map_err(internal)?;
    Ok(Json(
        json!({"events":rows.filter_map(Result::ok).collect::<Vec<_>>()}),
    ))
}

fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

async fn trigger_devin_session(incident_id: String, s: Arc<AppState>) -> Result<Value, ApiError> {
    let client = s.devin.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Devin is not configured: set DEVIN_ORG_ID and DEVIN_PLAYBOOK_ID".into(),
    ))?;
    let (incident, fire_id, tick) = {
        let sim = s.sim.read().await;
        (
            sim.incidents
                .iter()
                .find(|x| x.id == incident_id)
                .cloned()
                .ok_or((StatusCode::NOT_FOUND, "unknown incident".into()))?,
            sim.fire_id.clone().unwrap_or_default(),
            sim.tick,
        )
    };
    let path = s
        .fixture_files
        .iter()
        .find(|(id, _)| id == &fire_id)
        .map(|x| x.1.clone())
        .ok_or((StatusCode::NOT_FOUND, "fixture file not found".into()))?;
    let bytes = fs::read(&path).map_err(internal)?;
    let (attachment_id, attachment_url) = client
        .upload_attachment(
            path.file_name()
                .and_then(|x| x.to_str())
                .unwrap_or("spread.json"),
            bytes,
        )
        .await
        .map_err(internal)?;
    let prompt = format!(
        "You are performing the wildfire incident forecast playbook in AUTONOMY SANDBOX MODE. Historical replay and simulated actions only. Ground-truth Jev signals: {}. Read the attached baked Deepfire fixture. Recommend only actions from the structured schema. Update structured_output immediately whenever you add or revise an action. Never claim real dispatch, notification, evacuation, or aircraft action.",
        serde_json::to_string(&incident.signals).unwrap()
    );
    let created = client
        .create_session(&prompt, &attachment_url)
        .await
        .map_err(internal)?;
    let session_id = created
        .get("session_id")
        .or_else(|| created.get("id"))
        .and_then(Value::as_str)
        .ok_or((
            StatusCode::BAD_GATEWAY,
            "Devin response had no session id".into(),
        ))?
        .to_owned();
    let action = new_action(
        tick,
        Actor::Devin,
        "DEVIN_TRIGGERED",
        &incident_id,
        "Jev threshold froze state and created a Devin session",
        incident.signals.incident_confidence,
        ActionStatus::Proposed,
        json!({"sessionId":session_id,"attachmentId":attachment_id}),
    );
    write_audit(&s, &action);
    let ds = DevinSession {
        session_id: session_id.clone(),
        incident_id: incident_id.clone(),
        status: "running".into(),
        triggered_at: Utc::now().to_rfc3339(),
        last_actions_hash: String::new(),
        no_action_deadline: (Utc::now() + Duration::seconds(300)).to_rfc3339(),
        structured_output: None,
    };
    let mut sim = s.sim.write().await;
    sim.actions.push(action);
    sim.devin_sessions.push(ds);
    if let Some(i) = sim.incidents.iter_mut().find(|x| x.id == incident_id) {
        i.devin_session_id = Some(session_id.clone())
    }
    Ok(json!({"sessionId":session_id,"status":"triggered","simulated":true}))
}

async fn trigger_devin(
    Path(incident_id): Path<String>,
    State(s): State<Arc<AppState>>,
) -> ApiResult<Value> {
    Ok(Json(trigger_devin_session(incident_id, s).await?))
}

async fn devin_organizations(State(s): State<Arc<AppState>>) -> ApiResult<Value> {
    let client = s
        .devin
        .clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Devin unavailable".into()))?;
    Ok(Json(client.organizations().await.map_err(internal)?))
}

async fn redirect_devin(
    Path(session_id): Path<String>,
    State(s): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> ApiResult<Value> {
    let client = s
        .devin
        .clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Devin unavailable".into()))?;
    let message = body
        .get("message")
        .and_then(Value::as_str)
        .ok_or((StatusCode::BAD_REQUEST, "message required".into()))?;
    Ok(Json(
        client
            .redirect(&session_id, message)
            .await
            .map_err(internal)?,
    ))
}

fn proposed_path(action: &DevinAction) -> Vec<[f64; 2]> {
    action
        .params
        .get("path")
        .and_then(Value::as_array)
        .map(|points| {
            points
                .iter()
                .filter_map(|p| {
                    let a = p.as_array()?;
                    Some([a.first()?.as_f64()?, a.get(1)?.as_f64()?])
                })
                .collect()
        })
        .unwrap_or_default()
}

fn apply_approved_action(sim: &mut SimState, action: &DevinAction) {
    match action.action_type.as_str() {
        "dispatch_verification_drone" => {
            let index = sim
                .drones
                .iter()
                .position(|d| d.id == action.target)
                .or_else(|| sim.drones.iter().position(|d| d.status == "idle"));
            if let Some(index) = index {
                sim.drones[index].status = "verifying".into();
            }
        }
        "recall_drone" => {
            if let Some(drone) = sim.drones.iter_mut().find(|d| d.id == action.target) {
                drone.position = drone.home;
                drone.path.clear();
                drone.path_index = 0;
                drone.status = "recalled".into();
            }
        }
        "write_drone_path" => {
            if let Some(drone) = sim.drones.iter_mut().find(|d| d.id == action.target) {
                if action.params.get("path").is_some() {
                    drone.path = proposed_path(action);
                    drone.path_index = 0;
                    drone.status = "rerouting".into();
                }
            }
        }
        _ => {}
    }
}

async fn tick_loop(s: Arc<AppState>) {
    let mut timer = time::interval(time::Duration::from_millis(100));
    loop {
        timer.tick().await;
        let mut sim = s.sim.write().await;
        if !sim.playing {
            continue;
        }
        sim.tick += 1;
        sim.hour = (sim.hour + sim.speed * 0.004).min(12.0);
        let step = 0.001 * sim.speed;
        for drone in &mut sim.drones {
            if !matches!(
                drone.status.as_str(),
                "verifying" | "rerouting" | "patrolling" | "returning"
            ) {
                continue;
            }
            let Some(target) = drone.path.get(drone.path_index + 1).copied() else {
                if matches!(drone.status.as_str(), "verifying" | "rerouting") {
                    drone.status = "scanning".into()
                }
                continue;
            };
            let dx = target[0] - drone.position[0];
            let dy = target[1] - drone.position[1];
            let distance = (dx * dx + dy * dy).sqrt();
            if distance <= step {
                drone.position = target;
                drone.path_index += 1;
                if drone.path_index + 1 >= drone.path.len() {
                    drone.status = if drone.status == "returning" {
                        "recalled".into()
                    } else {
                        "scanning".into()
                    };
                }
            } else {
                drone.position[0] += dx / distance * step;
                drone.position[1] += dy / distance * step;
            }
        }

        let fixture = sim
            .fire_id
            .as_ref()
            .and_then(|id| {
                s.fixtures
                    .iter()
                    .find(|f| f["cluster"]["id"].as_str() == Some(id))
            })
            .cloned();
        if let Some(fixture) = fixture {
            let signals = jev_signals(&fixture, sim.hour, &sim.drones);
            if sim.incidents.is_empty()
                && signals.incident_confidence
                    >= sim.policy.auto_create_incident_when_confidence_gte
            {
                let id = sim.fire_id.clone().unwrap_or_default();
                let incident = Incident {
                    id: format!("incident-{id}"),
                    cluster_id: id,
                    created_at: Utc::now().to_rfc3339(),
                    signals: signals.clone(),
                    devin_session_id: None,
                };
                let action = new_action(
                    sim.tick,
                    Actor::Jev,
                    "auto_create_incident",
                    &incident.id,
                    "Time-local hotspot evidence crossed the incident threshold",
                    signals.incident_confidence,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour,"hotspotsVisible":visible_hotspots(&fixture,sim.hour).len()}),
                );
                write_audit(&s, &action);
                sim.actions.push(action);
                sim.incidents.push(incident);
            } else if let Some(incident) = sim.incidents.first_mut() {
                incident.signals = signals.clone()
            }

            let dispatched = sim
                .actions
                .iter()
                .any(|a| a.action_type == "dispatch_verification_drone");
            if !sim.incidents.is_empty()
                && !dispatched
                && signals.incident_confidence
                    >= sim
                        .policy
                        .auto_dispatch_verification_drone_when_confidence_gte
            {
                if let Some((center, radius)) = heat_geometry(&fixture, sim.hour) {
                    if let Some(ember) = sim.drones.iter_mut().find(|d| d.id == "EMBER") {
                        ember.path = safe_route(ember.home, center, radius, false);
                        ember.path_index = 0;
                        ember.status = "verifying".into();
                    }
                }
                let action = new_action(
                    sim.tick,
                    Actor::Jev,
                    "dispatch_verification_drone",
                    "EMBER",
                    "Confidence crossed 70%; assigned an upwind observation point outside the heat safety buffer",
                    signals.incident_confidence,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour,"mission":"UPWIND_PERIMETER_SCAN","reservePct":72}),
                );
                write_audit(&s, &action);
                sim.actions.push(action);
            }

            let has_scan = sim
                .actions
                .iter()
                .any(|a| a.action_type == "verification_scan_complete");
            if dispatched && sim.hour >= 3.0 && !has_scan {
                let action = new_action(
                    sim.tick,
                    Actor::Jev,
                    "verification_scan_complete",
                    "EMBER",
                    "Thermal pass confirmed an active edge while maintaining standoff",
                    signals.incident_confidence,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour,"sensor":"THERMAL","standoffKm":8.0}),
                );
                write_audit(&s, &action);
                sim.actions.push(action);
            }

            let has_shift = sim
                .actions
                .iter()
                .any(|a| a.action_type == "safety_buffer_expanded");
            if dispatched && sim.hour >= 4.5 && !has_shift {
                let proposed = new_action(
                    sim.tick,
                    Actor::Jev,
                    "write_drone_path",
                    "EMBER",
                    "Candidate direct corridor would shorten transit by 3 minutes",
                    0.81,
                    ActionStatus::Proposed,
                    json!({"simHour":sim.hour,"candidate":"DIRECT"}),
                );
                let veto = new_action(
                    sim.tick,
                    Actor::PolicyEngine,
                    "write_drone_path",
                    "EMBER",
                    "Route rejected: newly observed heat expands into the corridor safety buffer",
                    0.91,
                    ActionStatus::Vetoed,
                    json!({"simHour":sim.hour,"minimumStandoffKm":8.0}),
                );
                let expanded = new_action(
                    sim.tick,
                    Actor::Jev,
                    "safety_buffer_expanded",
                    "Incident Alpha",
                    "New hotspot detections expanded the observed-heat uncertainty buffer",
                    signals.incident_confidence,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour}),
                );
                if let Some((center, radius)) = heat_geometry(&fixture, sim.hour) {
                    if let Some(ember) = sim.drones.iter_mut().find(|d| d.id == "EMBER") {
                        ember.path = safe_route(ember.position, center, radius, true);
                        ember.path_index = 0;
                        ember.status = "rerouting".into();
                    }
                }
                let reroute = new_action(
                    sim.tick,
                    Actor::PolicyEngine,
                    "auto_reroute_drone",
                    "EMBER",
                    "Applied east-flank observation route with an 8 km modeled standoff",
                    0.91,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour,"reservePct":58}),
                );
                for action in [&proposed, &veto, &expanded, &reroute] {
                    write_audit(&s, action)
                }
                sim.actions.extend([proposed, veto, expanded, reroute]);
            }

            let recalled = sim.actions.iter().any(|a| a.action_type == "recall_drone");
            if dispatched && sim.hour >= 7.0 && !recalled {
                if let Some((center, radius)) = heat_geometry(&fixture, sim.hour) {
                    if let Some(ember) = sim.drones.iter_mut().find(|d| d.id == "EMBER") {
                        ember.path = safe_route(ember.position, center, radius + 0.18, true);
                        ember.path_index = 0;
                        ember.status = "returning".into();
                    }
                }
                let recall = new_action(
                    sim.tick,
                    Actor::PolicyEngine,
                    "recall_drone",
                    "EMBER",
                    "Growth uncertainty consumed the mission reserve; exiting to a contingency loiter point",
                    0.94,
                    ActionStatus::Approved,
                    json!({"simHour":sim.hour,"minimumReservePct":45,"mode":"SAFE_EGRESS"}),
                );
                write_audit(&s, &recall);
                sim.actions.push(recall);
            }
        }
        if sim.hour >= 12.0 {
            sim.playing = false;
        }
    }
}

async fn devin_poll_loop(s: Arc<AppState>) {
    let Some(client) = s.devin.clone() else {
        return;
    };
    let mut timer = time::interval(time::Duration::from_secs(5));
    loop {
        timer.tick().await;
        let sessions = {
            s.sim
                .read()
                .await
                .devin_sessions
                .iter()
                .filter(|x| x.status == "running")
                .cloned()
                .collect::<Vec<_>>()
        };
        for session in sessions {
            let deadline = chrono::DateTime::parse_from_rfc3339(&session.no_action_deadline)
                .map(|x| x.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            let has_output = {
                s.sim.read().await.actions.iter().any(|a| {
                    matches!(a.actor, Actor::Devin)
                        && a.action_type != "DEVIN_TRIGGERED"
                        && a.timestamp >= session.triggered_at
                })
            };
            if Utc::now() > deadline && !has_output {
                let mut sim = s.sim.write().await;
                if let Some(ds) = sim
                    .devin_sessions
                    .iter_mut()
                    .find(|x| x.session_id == session.session_id)
                {
                    ds.status = "timeout".into()
                }
                let signals = sim
                    .incidents
                    .iter()
                    .find(|x| x.id == session.incident_id)
                    .map(|x| x.signals.clone())
                    .unwrap_or_default();
                let fallback = new_action(
                    sim.tick,
                    Actor::Jev,
                    "dispatch_verification_drone",
                    "EMBER",
                    "Devin produced no action inside the demo time budget; deterministic Jev fallback activated",
                    signals.incident_confidence,
                    ActionStatus::Approved,
                    json!({"fallback":true,"sessionId":session.session_id}),
                );
                write_audit(&s, &fallback);
                sim.actions.push(fallback);
                if let Some(d) = sim.drones.iter_mut().find(|d| d.id == "EMBER") {
                    d.status = "verifying".into()
                }
                continue;
            }
            match client.session(&session.session_id).await {
                Ok(value) => {
                    let output = value
                        .pointer("/structured_output")
                        .or_else(|| value.pointer("/structuredOutput"))
                        .cloned()
                        .unwrap_or(Value::Null);
                    if !output.is_null() {
                        if let Some(ds) = s
                            .sim
                            .write()
                            .await
                            .devin_sessions
                            .iter_mut()
                            .find(|x| x.session_id == session.session_id)
                        {
                            ds.structured_output = Some(output.clone())
                        }
                    }
                    if let Ok(parsed) = serde_json::from_value::<DevinStructuredOutput>(output) {
                        let _assessment = &parsed.assessment;
                        for proposed in parsed.actions {
                            let key = format!(
                                "{}|{}|{}",
                                proposed.action_type, proposed.target, proposed.reason
                            );
                            let already = {
                                s.sim.read().await.actions.iter().any(|x| {
                                    x.params.get("dedupeKey").and_then(Value::as_str) == Some(&key)
                                })
                            };
                            if already {
                                continue;
                            }
                            let (tick, policy_cfg, signals, hour, fire_id) = {
                                let sim = s.sim.read().await;
                                let incident =
                                    sim.incidents.iter().find(|x| x.id == session.incident_id);
                                (
                                    sim.tick,
                                    sim.policy.clone(),
                                    incident.map(|x| x.signals.clone()).unwrap_or_default(),
                                    sim.hour,
                                    sim.fire_id.clone(),
                                )
                            };
                            let proposal = new_action(
                                tick,
                                Actor::Devin,
                                &proposed.action_type,
                                &proposed.target,
                                &proposed.reason,
                                proposed.confidence,
                                ActionStatus::Proposed,
                                json!({"params":proposed.params,"dedupeKey":key}),
                            );
                            write_audit(&s, &proposal);
                            let mut verdict = policy::evaluate(&policy_cfg, &signals, &proposed);
                            if verdict.is_ok() && proposed.action_type == "write_drone_path" {
                                let path = proposed_path(&proposed);
                                verdict = if path.len() < 2 {
                                    Err("route requires at least two valid waypoints".into())
                                } else if let Some((center, radius)) = fire_id
                                    .as_ref()
                                    .and_then(|id| {
                                        s.fixtures
                                            .iter()
                                            .find(|f| f["cluster"]["id"].as_str() == Some(id))
                                    })
                                    .and_then(|f| heat_geometry(f, hour))
                                {
                                    let risk = path_risk(&path, center, radius);
                                    if risk >= policy_cfg.auto_reroute_drone_when_path_risk_gte {
                                        Err(format!(
                                            "proposed route risk {:.2} exceeds safety threshold {:.2}",
                                            risk, policy_cfg.auto_reroute_drone_when_path_risk_gte
                                        ))
                                    } else {
                                        Ok(())
                                    }
                                } else {
                                    Err("no observed heat geometry available for route validation"
                                        .into())
                                };
                            }
                            let (status, reason) = match verdict {
                                Ok(()) => (ActionStatus::Approved, proposed.reason.clone()),
                                Err(e) => (ActionStatus::Vetoed, e),
                            };
                            let decision = new_action(
                                tick,
                                Actor::PolicyEngine,
                                &proposed.action_type,
                                &proposed.target,
                                &reason,
                                proposed.confidence,
                                status,
                                json!({"dedupeKey":key,"originalReason":proposed.reason}),
                            );
                            write_audit(&s, &decision);
                            let approved = matches!(decision.status, ActionStatus::Approved);
                            let mut sim = s.sim.write().await;
                            sim.actions.extend([proposal, decision]);
                            if approved {
                                apply_approved_action(&mut sim, &proposed);
                            }
                        }
                    }
                    let status = value
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("running");
                    if matches!(
                        status,
                        "completed" | "finished" | "exit" | "suspended" | "error" | "failed"
                    ) {
                        if let Some(x) = s
                            .sim
                            .write()
                            .await
                            .devin_sessions
                            .iter_mut()
                            .find(|x| x.session_id == session.session_id)
                        {
                            x.status = status.into()
                        }
                    }
                }
                Err(e) => warn!("Devin poll {} failed: {}", session.session_id, e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confidence_uses_only_evidence_visible_at_sim_time() {
        let fixture = json!({
            "cluster":{"firstObserved":"2026-01-01T00:00:00Z","lastObserved":"2026-01-01T12:00:00Z"},
            "hotspots":[
                {"observedAt":"2026-01-01T00:00:00Z","confidenceTier":"LOW","fireRadiativePower":2.0,"location":[0.0,0.0]},
                {"observedAt":"2026-01-01T12:00:00Z","confidenceTier":"HIGH","fireRadiativePower":100.0,"location":[0.1,0.1]}
            ],
            "valuesAtRisk":[]
        });
        let early = jev_signals(&fixture, 0.0, &[]).incident_confidence;
        let late = jev_signals(&fixture, 12.0, &[]).incident_confidence;
        assert!(
            late > early,
            "future evidence must increase confidence only after it arrives"
        );
    }

    #[test]
    fn generated_observation_route_stays_outside_heat_buffer() {
        let center = [0.0, 0.0];
        let route = safe_route([-0.3, -0.3], center, 0.08, false);
        assert!(path_risk(&route, center, 0.08) < 0.6);
        assert_ne!(*route.last().unwrap(), center);
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let (fixtures, fixture_files) = load_fixtures();
    let conn = Connection::open("fastandslow.sqlite").expect("open sqlite");
    audit_schema(&conn).expect("create audit schema");
    let devin = match DevinClient::from_env() {
        Ok(x) => {
            info!("Devin integration enabled");
            Some(x)
        }
        Err(e) => {
            warn!("{e}; running without Devin");
            None
        }
    };
    let state = Arc::new(AppState {
        sim: RwLock::new(SimState::default()),
        fixtures,
        fixture_files,
        audit: Mutex::new(conn),
        devin,
    });
    tokio::spawn(tick_loop(state.clone()));
    tokio::spawn(devin_poll_loop(state.clone()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/sim/fires", get(fires))
        .route("/sim/state", get(sim_state))
        .route("/sim/select/{id}", post(select_fire))
        .route("/sim/control", post(control))
        .route("/sim/audit", get(audit_events))
        .route("/sim/devin/organizations", get(devin_organizations))
        .route("/sim/devin/trigger/{incident_id}", post(trigger_devin))
        .route(
            "/sim/devin/sessions/{session_id}/redirect",
            post(redirect_devin),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8787")
        .await
        .expect("bind port 8787");
    info!("FastAndSlow backend listening on http://127.0.0.1:8787");
    if let Err(e) = axum::serve(listener, app).await {
        error!("server error: {e}")
    }
}
