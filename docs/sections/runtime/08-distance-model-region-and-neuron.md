## 8. Distance model (region + neuron)

### 8.1 Region distance (3D-inspired axial layering)

Regions are modeled as slices along an input-to-output axis with “planes” of regions per slice. This induces locality preference without complex geometry.

Define a per-region axial coordinate `z(region_id)`:

* `z(0)  = -3`  (Input end)
* `z(1)  = -2`, `z(2)  = -2`, `z(3)  = -2`
* `z(4..8)  = -1`
* `z(9..22) =  0`  (Center mass; includes 9..15 and 16..22)
* `z(23..27)= +1`
* `z(28..30)= +2`
* `z(31) = +3`  (Output end)

Region distance units:

* If `region_a == region_b`: `region_dist = 0`
* Else if `z(a) == z(b)`: `region_dist = region_intraslice_unit` (default 3 -- note that **same** region does not incur this)
* Else: `region_dist = region_axial_unit * abs(z(a) - z(b))` (default axial unit 5)

This yields:

* Input and Output far apart
* Regions within the same slice equidistant
* Adjacent slices uniformly close

### 8.2 Neuron distance within a region (ring metric with wrap)

Within a region of span `S` (neuron IDs `0..S-1`):

* `d = abs(i - j)`
* `wrap_d = min(d, S - d)`
* `neuron_dist_units = wrap_d`

Optionally scale to reduce magnitude (default):

* `neuron_dist_units = wrap_d >> neuron_dist_shift` (default shift 10; 1024 neurons = 1 unit)

### 8.3 Combined distance and cost

For an axon from `(rA, nA)` to `(rB, nB)` in the same brain:

* `dist_units = region_weight * region_dist(rA, rB) + neuron_dist_units(nA, nB, span_of_rB)`
* `distance_cost = axon_base_cost + axon_unit_cost * dist_units`

Region span for neuron distance uses the destination region span. If span is unknown at compute time (should not happen), use a fallback shift-only metric.

---
