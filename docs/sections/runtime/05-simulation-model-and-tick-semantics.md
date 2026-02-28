## 5. Simulation model and tick semantics

### 5.1 Neuron state (runtime)

Each neuron has:

* Persistent buffer `B` (float)
* Inbox accumulator `I` for contributions arriving for the next merge (float + flags for product accumulation)
* Enabled flag (runtime control; separate from “exists in definition”; potentially different Proto.Actor inbox mode/function)
* Function selectors and quantized parameters (from `.nbn` neuron record)
* Outgoing axon list (targets + strength codes, from `.nbn`, with optional overlay from `.nbs`)

Neurons “exist” if their neuron record exists in `.nbn` (Input/Output always exist). Other regions may be absent entirely (no region section).

### 5.2 Double-buffered accumulation with persistent buffer

Signals delivered during tick `N` must not affect neuron activation within tick `N`. They affect tick `N+1`.

Mechanism:

* During delivery phase of tick `N`, incoming contributions accumulate into `I`.
* At compute phase start of tick `N+1`, `I` is merged into `B` according to the neuron’s accumulation function, then `I` is cleared.

This avoids losing accumulation when a neuron does not activate in a tick, because `B` persists across ticks.

### 5.3 Accumulation functions

Accumulation functions are selected per neuron (2-bit ID). Defined in Appendix B and the `.proto` enums.

Required semantics:

* **SUM:** `B ← B + I`
* **PRODUCT (first-input special):**

  * `I` is tracked as `(hasInput, value)`
  * If no input, merge does nothing
  * If there is input, `B ← B * I.value`
* **MAX:** `B ← max(B, I)`
* **NONE:** merge does nothing

### 5.4 Activation and reset gates

Each tick compute phase:

1. If neuron is disabled (runtime), it does not compute activation; however it may still accumulate inbox depending on policy (default: inbox still accumulates, merge still occurs, activation suppressed).
2. Pre-activation gate: activate only if `B > PreActivationThreshold` (threshold may be negative).
3. Activation function computes `potential`.
4. Reset function updates `B` based on `(B, potential, activation_threshold, out_degree)`.
5. Fire if `abs(potential) > ActivationThreshold`, producing outgoing axon contributions.

### 5.5 I/O connectivity rules (invariants)

These apply to all brain creation paths: random generation, manual editing, reproduction, and imports.

* **Input region (0):**

  * No axon may target any neuron in region 0 (no `ToAddress.region_id == 0`).
* **Output region (31):**

  * Output region neurons may have outgoing axons.
  * No axon from output region may target any neuron in region 31 (no `From.region==31 AND To.region==31`).
* **Self-loop axons:**

  * Allowed for all regions except output region (disallowed by the rule above) and input region (disallowed because nothing may target input).
* **Duplicate axons:**

  * For any source neuron, at most one axon may target a given `(target_region_id, target_neuron_id)`.

---
