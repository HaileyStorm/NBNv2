## 1. Purpose and scope

NothingButNeurons (NBN) is a distributed neural simulation framework. It models â€œbrainsâ€ composed of regions, and each region contains neurons connected by directed **axons**. NBN is designed for:

* **Cross-platform execution:** Windows and Ubuntu (headless services and GUIs).
* **Distributed execution:** compute and coordination can be spread across multiple processes and machines.
* **Tick-based pacing:** a global tick constrains timing differences caused by deployment topology and load.
* **Reproduction/evolution:** creation of new brain definitions from existing running brains, with configurable mutation and structural changes.
* **Observability:** debugging, visualization, and infrastructure-level telemetry are first-class and can be disabled.

NBN is not an ML training framework (no backpropagation/gradient descent).

---
