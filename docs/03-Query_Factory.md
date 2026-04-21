## Phase-2 Dynamic Query Factory Contract (V1)

Target artifact:
1. `dataset/benchmark/quam_multigt_v1/query_triplets/*.jsonl`
2. one row = one triplet with replayable generation metadata.

Triplet output schema:

```json
{
  "query_id": "string",
  "dataset": "string",
  "user_id": "string",
  "constraint_hint": {
    "time_window": {},
    "spatial_scope": {},
    "category_scope": {},
    "hard_constraints": [],
    "soft_preferences": []
  },
  "metadata": {
    "difficulty_level": "L1|L2|L3",
    "difficulty_score": 0.0,
    "profile_snapshot": {},
    "sampling_trace": {},
    "perturbation_trace": [],
    "seed": 0
  },
  "query": "string"
}
```

NetArena-inspired principles adapted to QUAM-Eval:
1. Unified generation interface:
   high-level config in, unlimited stochastic query generation out.
2. State-action style abstraction for reproducibility:
   query state is represented by `(user profile, history slice,
   candidate environment state, perturbation state)`.
3. Controlled complexity scaling:
   each generated query has explicit level + factors in metadata.
4. Dynamic perturbation injection:
   perturbations are sampled at runtime and logged for replay.

External reference anchors (for implementation intent):
1. OpenReview paper (ICLR 2026):
   https://openreview.net/forum?id=BPVPOtzoOz
2. Published PDF (details on state-action abstraction, stochastic
   generation, complexity-aware evaluation):
   https://openreview.net/pdf?id=BPVPOtzoOz
3. Official codebase:
   https://github.com/Froot-NetSys/NetArena

### High-level generation config

Factory input config (single source of truth):
1. `query_count`
2. `dataset_mix`
3. `difficulty_mix` (`L1/L2/L3` ratios)
4. `perturbation_budget` (rate and allowed types)
5. `seed`
6. `min_profile_events` (default aligned with profile threshold)

### Difficulty control (decision-complete)

Difficulty composition:
1. `user_complexity` from profile entropy/novelty/mobility signals.
2. `constraint_complexity` by number and strictness of constraints.
3. `history_complexity` by trajectory length and ambiguity.
4. `perturbation_complexity` by perturbation count/severity.

Levels:
1. `L1` (single-focus):
   one dominant intent and low perturbation (or none).
2. `L2` (compositional):
   multiple constraints and moderate perturbation.
3. `L3` (adversarial/realistic):
   high ambiguity, conflicting constraints, and multi-step perturbation.

### Environmental perturbation model

Perturbation families:
1. Temporal perturbation:
   shift local time band / inject peak-hour stress.
2. Spatial perturbation:
   tighten/expand radius or relocate anchor zone.
3. Category perturbation:
   mask top category or inject category conflict.
4. Availability/popularity perturbation:
   down-rank frequent POIs, simulate closures/unavailability.
5. Context noise perturbation:
   inject distractor metadata fields or irrelevant recent events.

Policy:
1. Perturbations must be explicit in `metadata.perturbation_trace`.
2. Perturbations must preserve plausibility (no impossible geo/time).
3. Every perturbation must be replayable by `(seed, trace)`.

### Query synthesis pipeline

1. Profile-aware sampling:
   sample user and anchor state from `users/interactions/profile_json`.
2. Scenario assembly:
   sample constraints from profile + target difficulty.
3. Perturbation injection:
   apply perturbation operators by budget and level.
4. NL rendering:
   render `(constraint_hint, metadata)` into final `query`.
5. Quality gates:
   dedup, validity checks, level-balance checks, replay checks.

### Acceptance requirements for Phase-2 output

1. `query_triplets` generated from config are reproducible under same
   seed.
2. Difficulty distribution matches configured mix within tolerance.
3. Perturbation traces are non-empty when perturbation budget > 0.
4. Each row includes valid `constraint_hint`, `metadata`, and `query`.
5. Generated queries pass plausibility validators (time/space/category).
