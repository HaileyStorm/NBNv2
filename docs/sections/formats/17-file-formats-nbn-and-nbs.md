## 17. File formats: `.nbn` and `.nbs`

### 17.1 Common conventions

* Endianness: little-endian for all multibyte integers
* UUID bytes: RFC 4122 network order (big-endian; matches canonical hex string)
* All bitfields are defined with bit 0 as LSB of their integer container
* File extensions:

  * `.nbn` = brain definition
  * `.nbs` = brain state snapshot

---

### 17.2 `.nbn` format (NBN2)

#### 17.2.1 Top-level layout

1. Fixed header (`NbnHeaderV2`, 1024 bytes)
2. Region sections (0..31), each at offsets specified by header directory

Regions 0 and 31 must be present. Regions without neurons are not included.

#### 17.2.2 `NbnHeaderV2` (1024 bytes)

Offsets and sizes:

**0x000–0x003 (4 bytes)**

* `magic` = ASCII `"NBN2"`

**0x004–0x005 (2 bytes)**

* `version_u16` = 2

**0x006 (1 byte)**

* `endianness_u8` = 1 (little)

**0x007 (1 byte)**

* `header_bytes_pow2_u8` = 10 (1024 bytes)

**0x008–0x00F (8 bytes)**

* `brain_seed_u64`

**0x010–0x013 (4 bytes)**

* `axon_stride_u32` (default 1024)

**0x014–0x017 (4 bytes)**

* `flags_u32`

  * bit 0: reserved (must be 0)
  * bit 1: reserved
  * others reserved

**0x018–0x01F (8 bytes)**

* `reserved_u64`

**0x020–0x0FF (224 bytes)** Quantization schema block (fixed fields)
This block defines the decode/encode mapping parameters for each quantized field in neuron/axon records. Bit widths are fixed by record layout; this block defines ranges and mapping types.

Each quant field uses the structure:

* `map_type_u8` (0x00)
* `reserved_u8` (0x01)
* `reserved_u16` (0x02–0x03)
* `min_f32` (0x04–0x07)
* `max_f32` (0x08–0x0B)
* `gamma_f32` (0x0C–0x0F)

Total: 16 bytes per field.

Fields in order (16 bytes each):

1. Strength (axon strength code, 5 bits)
2. PreActivationThreshold (6 bits)
3. ActivationThreshold (6 bits)
4. ParamA (6 bits)
5. ParamB (6 bits)

Quant block size: 5 * 16 = 80 bytes, stored starting at 0x020. Remaining bytes up to 0x0FF reserved and must be zero.

**Quant map types:**

* `0 = LINEAR_SIGNED_CENTERED`
  codes map to symmetric range with two mid codes mapping to 0 when code count is even
* `1 = LINEAR_UNSIGNED`
  codes map to [min, max] with min>=0 typically
* `2 = GAMMA_SIGNED_CENTERED`
  like signed centered but with gamma companding toward 0
* `3 = GAMMA_UNSIGNED`
  like unsigned with gamma companding toward min

Default recommended mappings:

* Strength: GAMMA_SIGNED_CENTERED, min=-1, max=+1, gamma=2.0
* PreActivationThreshold: GAMMA_SIGNED_CENTERED, min=-1, max=+1, gamma=2.0
* ActivationThreshold: GAMMA_UNSIGNED, min=0, max=1, gamma=2.0
* ParamA/B: GAMMA_SIGNED_CENTERED, min=-3, max=+3, gamma=2.0

**0x100–0x3FF (768 bytes)** Region directory: 32 entries × 24 bytes
Entry `i` corresponds to `region_id = i`.

Each entry layout (24 bytes):

* `neuron_span_u32` (4 bytes)
* `total_axons_u64` (8 bytes)
* `region_offset_u64` (8 bytes)
* `region_flags_u32` (4 bytes; reserved, must be 0)

Rules:

* `neuron_span_u32` MUST be in [0..4194303] (22-bit) because axon records store `target_neuron_id` in 22 bits. `axon_stride_u32` defines the checkpoint spacing and shard-alignment unit: RegionShards MUST start on stride boundaries and (except for the final tail shard) cover a stride-multiple neuron count. The final shard in a region MAY be shorter to cover `[last_stride_boundary, neuron_span)` when `neuron_span` is not a multiple of `axon_stride_u32`.
* region_offset is 0 if region absent.
* region 0 and 31 must have neuron_span>0 and region_offset>0.

**0x400–0x3FF (end)** does not exist; header ends at 0x3FF (1024 bytes)

#### 17.2.3 Quantization mapping formulas

Let `bits` be the bit-width, `max_code = (1<<bits)-1`, and `code ∈ [0..max_code]`.

**Signed centered mapping (even code count)**
For even `max_code+1`, two center codes map to 0:

* `center_lo = (max_code+1)/2 - 1`
* `center_hi = (max_code+1)/2`

Define a signed normalized `t`:

* if `code == center_lo or code == center_hi`: `t = 0`
* else:

  * map codes below center to negative, above to positive
  * `k = code - center_hi` for above; `k = code - center_lo` for below
  * `t = k / (center_lo)` (approx symmetric)

Then:

* LINEAR_SIGNED_CENTERED: `value = t * max_abs` where `max_abs = max(abs(min), abs(max))`, then clamp to [min,max]
* GAMMA_SIGNED_CENTERED: `value = sign(t) * (abs(t)^gamma) * max_abs`, then clamp

**Unsigned mapping**
Let normalized `u = code / max_code`.

* LINEAR_UNSIGNED: `value = min + u * (max - min)`
* GAMMA_UNSIGNED: `value = min + (u^gamma) * (max - min)`

Encoding is the inverse mapping with rounding to nearest code and clamping.

#### 17.2.4 Region section layout

At `region_offset`:

**RegionSectionHeader**

* `region_id_u8` (1)
* `reserved_u8` (1)
* `reserved_u16` (2)
* `neuron_span_u32` (4)
* `total_axons_u64` (8)
* `stride_u32` (4) must equal header stride
* `checkpoint_count_u32` (4) = `ceil(neuron_span/stride) + 1` (integer math: `(neuron_span + stride - 1) / stride + 1`)

Then:

* `axon_checkpoints_u64[checkpoint_count]` (8 * checkpoint_count)
* `neuron_records` (6 * neuron_span bytes)
* `axon_records` (4 * total_axons bytes)

Checkpoint semantics:

* `axon_checkpoints[k]` is the cumulative axon count for all neurons with `neuron_id < k*stride`.
* `axon_checkpoints[0] = 0`
* `axon_checkpoints[last] = total_axons`, where `last = ceil(neuron_span/stride)`. The final stride boundary before `neuron_span` may be `< neuron_span` when the span is not a multiple of `stride`.

To find axon start offset for a neuron `i`:

* `k = i / stride`
* `base = axon_checkpoints[k]`
* scan neuron records from `k*stride` to `i-1` summing `axon_count`
* `start = base + sum`
* `count = axon_count(i)`
  Runtime implementations typically precompute per-neuron offsets on load for O(1) access (perhaps depending on brain/region size).

#### 17.2.5 Neuron record (6 bytes, 48 bits)

NeuronId is implicit as record index within region.

Bit layout (LSB=bit0):

* bits 0..8   : `axon_count` (9 bits, 0..511)
* bits 9..14  : `paramB_code` (6)
* bits 15..20 : `paramA_code` (6)
* bits 21..26 : `activation_threshold_code` (6)
* bits 27..32 : `preactivation_threshold_code` (6)
* bits 33..38 : `reset_function_id` (6)
* bits 39..44 : `activation_function_id` (6)
* bits 45..46 : `accumulation_function_id` (2)
* bit 47      : `exists` (1)

Rules:

* If `exists==0` (when deleted/disable by reproduction), `axon_count` must be 0.
* Regions 0 and 31:

  * all neuron records must have `exists==1` (cannot delete/disable definition loci)

#### 17.2.6 Axon record (4 bytes, 32 bits)

Bit layout:

* bits 0..4   : `strength_code` (5)
* bits 5..26  : `target_neuron_id` (22)
* bits 27..31 : `target_region_id` (5)

Rules:

* Axons for each neuron are stored contiguously in the region’s axon record array.
* Within a neuron’s axon list, records must be sorted by `(target_region_id, target_neuron_id)` ascending.
* `target_neuron_id` MUST be < the target region's `neuron_span` and <= 4194303.
* Duplicate axons from a given neuron to the same `(target_region_id, target_neuron_id)` are not allowed.
* Validation invariants:

  * no axon targets region 0
  * no axon from region 31 targets region 31

---

### 17.3 `.nbs` format (NBS2 state snapshot)

#### 17.3.1 Snapshot goals

* Full-brain restore after failures
* Store persistent buffers and optional enabled mask
* Store axon strength overlay codes (only where quantized code differs from base `.nbn`)
* Store energy balance and settings flags

#### 17.3.2 Snapshot layout

1. Fixed header (`NbsHeaderV2`, 512 bytes)
2. Region state sections for all regions present in base `.nbn`
3. Optional axon overlay section
4. Optional metadata (reserved)

#### 17.3.3 `NbsHeaderV2` (512 bytes)

Offsets and sizes:

**0x000–0x003**

* `magic` = ASCII `"NBS2"`

**0x004–0x005**

* `version_u16` = 2

**0x006**

* `endianness_u8` = 1

**0x007**

* `header_bytes_pow2_u8` = 9 (512 bytes)

**0x008–0x017 (16 bytes)**

* `brain_id_uuid` (RFC 4122 byte order)

**0x018–0x01F (8 bytes)**

* `snapshot_tick_id_u64`

**0x020–0x027 (8 bytes)**

* `timestamp_ms_u64`

**0x028–0x02F (8 bytes)**

* `energy_remaining_i64`

**0x030–0x04F (32 bytes)**

* `base_nbn_sha256`

**0x050–0x053 (4 bytes)**

* `flags_u32`

  * bit 0: enabled_bitset_included
  * bit 1: axon_overlay_included
  * bit 2: cost_enabled
  * bit 3: energy_enabled
  * bit 4: plasticity_enabled
  * others reserved

**0x054–0x07F (44 bytes)** Buffer quantization schema (fixed)

* `buffer_map_type_u8`
* `reserved_u8`
* `reserved_u16`
* `buffer_min_f32`
* `buffer_max_f32`
* `buffer_gamma_f32`
* remaining reserved to 44 bytes (must be zero)

Default buffer mapping:

* GAMMA_SIGNED_CENTERED, min=-4, max=+4, gamma=2.0
  Buffer is stored as `int16` code over this range.

**0x080–0x1FF**

* reserved, must be zero

#### 17.3.4 Region state section

For each region present in the base `.nbn` (region directory neuron_span>0), in region_id ascending:

Section header:

* `region_id_u8`
* `reserved_u8`
* `reserved_u16`
* `neuron_span_u32` (must match `.nbn`)
* `buffer_codes_i16[neuron_span]` (persistent B)
* optional `enabled_bitset` if header flag includes it:

  * length = ceil(neuron_span / 8) bytes

Snapshots are taken at tick boundaries; inbox accumulators are not stored.

#### 17.3.5 Axon overlay section

If `axon_overlay_included`:

* `overlay_count_u32`
* then `overlay_count` overlay records:

Overlay record (12 bytes):

* `from_addr32_u32`
* `to_addr32_u32`
* `strength_code_u8` (0..31)
* `reserved_u8`
* `reserved_u16`

Overlay semantics:

* If an overlay record exists for `(from,to)`, that strength code replaces the base strength code from `.nbn`.
* Only store overlay records where the overlay code differs from base `.nbn` code.

---
