# Appendix B: Function catalog (IDs, formulas, tiers, costs)

All functions operate on float32 values (`MathF` semantics). The â€œcost weightâ€ is an abstract multiplier used by cost accounting; it is not wall-clock time.

## B.1 AccumulationFunction (2-bit)

* `ACCUM_SUM (0)`

  * Merge: `B = B + I`
  * Tier: A, Cost weight: 1.0
* `ACCUM_PRODUCT (1)`

  * Inbox tracked as `(hasInput, value)`
  * Merge if `hasInput`: `B = B * value`
  * Tier: A, Cost weight: 1.2
* `ACCUM_MAX (2)`

  * Merge: `B = max(B, I)`
  * Tier: A, Cost weight: 1.0
* `ACCUM_NONE (3)`

  * Merge does nothing
  * Tier: A, Cost weight: 0.1

## B.2 ActivationFunction (6-bit)

`Activate(buffer B, paramA A, paramB Bp) -> potential`

* `ACT_NONE (0)`
  potential = 0
  Tier A, cost 0.0

* `ACT_IDENTITY (1)`
  potential = B
  Tier A, cost 1.0

* `ACT_STEP_UP (2)`
  potential = (B <= 0) ? 0 : 1
  Tier A, cost 1.0

* `ACT_STEP_MID (3)`
  potential = (B < 0) ? -1 : (B == 0 ? 0 : 1)
  Tier A, cost 1.0

* `ACT_STEP_DOWN (4)`
  potential = (B < 0) ? -1 : 0
  Tier A, cost 1.0

* `ACT_ABS (5)`
  potential = abs(B)
  Tier A, cost 1.1

* `ACT_CLAMP (6)`
  potential = clamp(B, -1, +1)
  Tier A, cost 1.1

* `ACT_RELU (7)`
  potential = max(0, B)
  Tier A, cost 1.1

* `ACT_NRELU (8)`
  potential = min(B, 0)
  Tier A, cost 1.1

* `ACT_SIN (9)`
  potential = sin(B)
  Tier B, cost 1.4

* `ACT_TAN (10)`
  potential = clamp(tan(B), -1, +1)
  Tier B, cost 1.6

* `ACT_TANH (11)`
  potential = tanh(B)
  Tier B, cost 1.6

* `ACT_ELU (12)` uses A
  potential = (B > 0) ? B : A * (exp(B) - 1)
  Tier B, cost 1.8

* `ACT_EXP (13)`
  potential = exp(B)
  Tier B, cost 1.8

* `ACT_PRELU (14)` uses A
  potential = (B >= 0) ? B : A * B
  Tier A/B, cost 1.4

* `ACT_LOG (15)`
  potential = (B == 0) ? 0 : log(B)
  Tier B, cost 1.9

* `ACT_MULT (16)` uses A
  potential = B * A
  Tier A, cost 1.2

* `ACT_ADD (17)` uses A
  potential = B + A
  Tier A, cost 1.2

* `ACT_SIG (18)`
  potential = 1 / (1 + exp(-B))
  Tier B, cost 2.0

* `ACT_SILU (19)`
  potential = B / (1 + exp(-B))
  Tier B, cost 2.0

* `ACT_PCLAMP (20)` uses A and Bp
  potential = (Bp <= A) ? 0 : clamp(B, A, Bp)
  Tier A, cost 1.3

* `ACT_MODL (21)` uses A
  potential = B % A
  Tier C, cost 2.6

* `ACT_MODR (22)` uses A
  potential = A % B
  Tier C, cost 2.6

* `ACT_SOFTP (23)`
  potential = log(1 + exp(B))
  Tier C, cost 2.8

* `ACT_SELU (24)` uses A and Bp
  potential = Bp * (B >= 0 ? B : A*(exp(B)-1))
  Tier C, cost 2.8

* `ACT_LIN (25)` uses A and Bp
  potential = A * B + Bp
  Tier A, cost 1.4

* `ACT_LOGB (26)` uses A
  potential = (A == 0) ? 0 : log(B, A)
  Tier C, cost 3.0

* `ACT_POW (27)` uses A
  potential = pow(B, A)
  Tier C, cost 3.5

* `ACT_GAUSS (28)`
  potential = exp((-B)^2)
  Tier C, cost 5.0

* `ACT_QUAD (29)` uses A and Bp
  potential = A*(B^2) + Bp*B
  Tier C, cost 6.0

## B.3 ResetFunction (6-bit)

`Reset(buffer B, potential P, activation_threshold T, out_degree K) -> new_buffer`

Below, `clamp(x, lo, hi)` clamps x.

* `RESET_ZERO (0)`
  new = 0
  Tier A, cost 0.2

* `RESET_HOLD (1)`
  new = clamp(B, -T, +T)
  Tier A, cost 1.0

* `RESET_CLAMP_POTENTIAL (2)`
  new = clamp(B, -abs(P), +abs(P))
  Tier A, cost 1.0

* `RESET_CLAMP1 (3)`
  new = clamp(B, -1, +1)
  Tier A, cost 1.0

* `RESET_POTENTIAL_CLAMP_BUFFER (4)`
  new = clamp(P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL_CLAMP_BUFFER (5)`
  new = clamp(-P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER (6)`
  new = clamp(0.01*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL_CLAMP_BUFFER (7)`
  new = clamp(0.1*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL_CLAMP_BUFFER (8)`
  new = clamp(0.5*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER (9)`
  new = clamp(2*P, -abs(B), +abs(B))
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL_CLAMP_BUFFER (10)`
  new = clamp(5*P, -abs(B), +abs(B))
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER (11)`
  new = clamp(-0.01*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER (12)`
  new = clamp(-0.1*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER (13)`
  new = clamp(-0.5*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER (14)`
  new = clamp(-2*P, -abs(B), +abs(B))
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER (15)`
  new = clamp(-5*P, -abs(B), +abs(B))
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL_CLAMP_BUFFER (16)`
  new = clamp(1/P, -abs(B), +abs(B))
  Tier C, cost 1.8

* `RESET_POTENTIAL_CLAMP1 (17)`
  new = clamp(P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL_CLAMP1 (18)`
  new = clamp(-P, -1, +1)
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL_CLAMP1 (19)`
  new = clamp(0.01*P, -1, +1)
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL_CLAMP1 (20)`
  new = clamp(0.1*P, -1, +1)
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL_CLAMP1 (21)`
  new = clamp(0.5*P, -1, +1)
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL_CLAMP1 (22)`
  new = clamp(2*P, -1, +1)
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL_CLAMP1 (23)`
  new = clamp(5*P, -1, +1)
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1 (24)`
  new = clamp(-0.01*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL_CLAMP1 (25)`
  new = clamp(-0.1*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL_CLAMP1 (26)`
  new = clamp(-0.5*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL_CLAMP1 (27)`
  new = clamp(-2*P, -1, +1)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL_CLAMP1 (28)`
  new = clamp(-5*P, -1, +1)
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL_CLAMP1 (29)`
  new = clamp(1/P, -1, +1)
  Tier C, cost 1.8

* `RESET_POTENTIAL (30)`
  new = clamp(P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL (31)`
  new = clamp(-P, -T, +T)
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL (32)`
  new = clamp(0.01*P, -T, +T)
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL (33)`
  new = clamp(0.1*P, -T, +T)
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL (34)`
  new = clamp(0.5*P, -T, +T)
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL (35)`
  new = clamp(2*P, -T, +T)
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL (36)`
  new = clamp(5*P, -T, +T)
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL (37)`
  new = clamp(-0.01*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL (38)`
  new = clamp(-0.1*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL (39)`
  new = clamp(-0.5*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL (40)`
  new = clamp(-2*P, -T, +T)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL (41)`
  new = clamp(-5*P, -T, +T)
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL (42)`
  new = clamp(1/P, -T, +T)
  Tier C, cost 1.8

* `RESET_HALF (43)`
  new = clamp(0.5*B, -T, +T)
  Tier A, cost 1.0

* `RESET_TENTH (44)`
  new = clamp(0.1*B, -T, +T)
  Tier A, cost 1.0

* `RESET_HUNDREDTH (45)`
  new = clamp(0.01*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEGATIVE (46)`
  new = clamp(-B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HALF (47)`
  new = clamp(-0.5*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_TENTH (48)`
  new = clamp(-0.1*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HUNDREDTH (49)`
  new = clamp(-0.01*B, -T, +T)
  Tier A, cost 1.0

* `RESET_DOUBLE_CLAMP1 (50)`
  new = clamp(2*B, -1, +1)
  Tier B, cost 1.2

* `RESET_FIVEX_CLAMP1 (51)`
  new = clamp(5*B, -1, +1)
  Tier B, cost 1.3

* `RESET_NEG_DOUBLE_CLAMP1 (52)`
  new = clamp(-2*B, -1, +1)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_CLAMP1 (53)`
  new = clamp(-5*B, -1, +1)
  Tier B, cost 1.3

* `RESET_DOUBLE (54)`
  new = clamp(2*B, -T, +T)
  Tier B, cost 1.2

* `RESET_FIVEX (55)`
  new = clamp(5*B, -T, +T)
  Tier B, cost 1.3

* `RESET_NEG_DOUBLE (56)`
  new = clamp(-2*B, -T, +T)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX (57)`
  new = clamp(-5*B, -T, +T)
  Tier B, cost 1.3

* `RESET_DIVIDE_AXON_CT (58)`
  new = clamp(B / max(1, K), -T, +T)
  Tier A, cost 1.1

* `RESET_INVERSE_CLAMP1 (59)`
  new = clamp(-1/B, -1, +1)
  Tier C, cost 1.8

* `RESET_INVERSE (60)`
  new = clamp(-1/B, -T, +T)
  Tier C, cost 1.8

---
