import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate } from "k6/metrics";

export const accepted = new Counter("eventmesh_api_accepted_total");
export const rejected = new Counter("eventmesh_api_rejected_total");
export const acceptedRate = new Rate("eventmesh_api_accepted_rate");

const profiles = {
  smoke: {
    executor: "constant-arrival-rate",
    rate: intEnv("K6_SMOKE_RPS", 2),
    timeUnit: "1s",
    duration: env("K6_SMOKE_DURATION", "30s"),
    preAllocatedVUs: intEnv("K6_SMOKE_VUS", 5),
    maxVUs: intEnv("K6_SMOKE_MAX_VUS", 10),
  },
  baseline: {
    executor: "constant-arrival-rate",
    rate: intEnv("K6_BASELINE_RPS", 50),
    timeUnit: "1s",
    duration: env("K6_BASELINE_DURATION", "5m"),
    preAllocatedVUs: intEnv("K6_BASELINE_VUS", 50),
    maxVUs: intEnv("K6_BASELINE_MAX_VUS", 200),
  },
  stress: {
    executor: "ramping-arrival-rate",
    startRate: intEnv("K6_STRESS_START_RPS", 10),
    timeUnit: "1s",
    preAllocatedVUs: intEnv("K6_STRESS_VUS", 100),
    maxVUs: intEnv("K6_STRESS_MAX_VUS", 500),
    stages: [
      { target: intEnv("K6_STRESS_RPS_1", 50), duration: env("K6_STRESS_STAGE_1", "2m") },
      { target: intEnv("K6_STRESS_RPS_2", 100), duration: env("K6_STRESS_STAGE_2", "2m") },
      { target: intEnv("K6_STRESS_RPS_3", 200), duration: env("K6_STRESS_STAGE_3", "2m") },
      { target: 0, duration: env("K6_STRESS_COOLDOWN", "1m") },
    ],
  },
  soak: {
    executor: "constant-arrival-rate",
    rate: intEnv("K6_SOAK_RPS", 25),
    timeUnit: "1s",
    duration: env("K6_SOAK_DURATION", "30m"),
    preAllocatedVUs: intEnv("K6_SOAK_VUS", 50),
    maxVUs: intEnv("K6_SOAK_MAX_VUS", 200),
  },
  spike: {
    executor: "ramping-arrival-rate",
    startRate: intEnv("K6_SPIKE_START_RPS", 10),
    timeUnit: "1s",
    preAllocatedVUs: intEnv("K6_SPIKE_VUS", 200),
    maxVUs: intEnv("K6_SPIKE_MAX_VUS", 800),
    stages: [
      { target: intEnv("K6_SPIKE_BASE_RPS", 25), duration: env("K6_SPIKE_WARMUP", "30s") },
      { target: intEnv("K6_SPIKE_PEAK_RPS", 300), duration: env("K6_SPIKE_PEAK", "30s") },
      { target: intEnv("K6_SPIKE_BASE_RPS", 25), duration: env("K6_SPIKE_RECOVERY", "2m") },
      { target: 0, duration: env("K6_SPIKE_COOLDOWN", "30s") },
    ],
  },
};

const profileName = env("LOAD_PROFILE", "smoke").toLowerCase();
const scenario = profiles[profileName];

if (!scenario) {
  throw new Error(`unknown LOAD_PROFILE=${profileName}; expected one of ${Object.keys(profiles).join(", ")}`);
}

export const options = {
  scenarios: {
    events: scenario,
  },
  thresholds: {
    http_req_failed: [env("K6_HTTP_ERROR_THRESHOLD", "rate<0.01")],
    http_req_duration: [
      env("K6_HTTP_P95_THRESHOLD", "p(95)<500"),
      env("K6_HTTP_P99_THRESHOLD", "p(99)<1000"),
    ],
    eventmesh_api_accepted_rate: [env("K6_ACCEPTED_RATE_THRESHOLD", "rate>0.99")],
  },
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

const eventTypes = ["order.created", "payment.received", "shipment.created"];
const baseURL = env("BASE_URL", "http://localhost:8080");
const source = env("EVENT_SOURCE", "k6-loadtest");

export default function () {
  const eventType = eventTypes[__ITER % eventTypes.length];
  const orderID = `ORD-K6-${__VU}-${__ITER}-${Date.now()}`;
  const payload = JSON.stringify({
    type: eventType,
    source,
    payload: {
      order_id: orderID,
      customer_id: `CUST-${(__ITER % 10000) + 1}`,
      amount: Number((10 + Math.random() * 490).toFixed(2)),
      currency: "USD",
      load_profile: profileName,
    },
  });

  const response = http.post(`${baseURL}/v1/events`, payload, {
    headers: {
      "Content-Type": "application/json",
      "X-Request-ID": `k6-${profileName}-${__VU}-${__ITER}`,
    },
    tags: {
      route: "/v1/events",
      event_type: eventType,
      profile: profileName,
    },
  });

  const isAccepted = response.status === 202;
  acceptedRate.add(isAccepted);
  if (isAccepted) {
    accepted.add(1);
  } else {
    rejected.add(1);
  }

  check(response, {
    "POST /v1/events returns 202": (r) => r.status === 202,
    "response includes event_id": (r) => {
      try {
        return Boolean(r.json("event_id"));
      } catch (_) {
        return false;
      }
    },
  });

  sleep(floatEnv("K6_ITERATION_SLEEP_SECONDS", 0));
}

function env(name, fallback) {
  return __ENV[name] || fallback;
}

function intEnv(name, fallback) {
  const value = Number.parseInt(__ENV[name] || "", 10);
  return Number.isFinite(value) ? value : fallback;
}

function floatEnv(name, fallback) {
  const value = Number.parseFloat(__ENV[name] || "");
  return Number.isFinite(value) ? value : fallback;
}

