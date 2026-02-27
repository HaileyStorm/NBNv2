# Appendix C: Region axial map and examples

## C.1 Axial coordinate table

* z=-3: region 0
* z=-2: regions 1,2,3
* z=-1: regions 4,5,6,7,8
* z=0 : regions 9..22
* z=+1: regions 23..27
* z=+2: regions 28..30
* z=+3: region 31

## C.2 Region distance examples (default params)

With `region_intraslice_unit=3` and `region_axial_unit=5`:

* dist(0,31) = 5 * |(-3) - (+3)| = 30
* dist(1,2)  = 3 (same slice)  // note that if they were **same** region this cost would not be incurred
* dist(3,4)  = 5 * |(-2) - (-1)| = 5
* dist(8,23) = 5 * |(-1) - (+1)| = 10
