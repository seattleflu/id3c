# Barcode slices

## Motivation
The barcode minting process involves a hamming distance check
to ensure that a new candidate barcode is not created if it is
not different enough from an existing barcode. This check
(defined as `public.hamming_distance` and `public.hamming_distance_lte`)
goes character by character comparing the candidate barcode to all barcodes
in the `warehouse.identifier` table. This is a slow operation, and
as the volume of barcodes grew its cost slowed barcode minting drastically.

## Slices
We looked for a less expensive "pre-screener" operation to narrow down the
existing barcodes that a new candidate barcode would have to have its hamming
distance calculated against. Discussion in
https://stackoverflow.com/questions/9606492/hamming-distance-similarity-searches-in-a-database/47487949#47487949
led us to a solution that "chunks" or "slices" the characters of the barcode
and stores them in a text array that lets us leverage postgres's array intersection
operator `&&` for an efficient way to find existing barcodes that are similar to the candidate.
With a barcode width of 8 characters, we found that we could use a slice width of
2 characters and not miss barcodes that might fail the hamming distance check of < 3.
That is, we determined that two barcodes that have a hamming distance of < 3 will always
have two consecutive characters in common. Our slices are defined as the index (1-based)
value, two underscores, and the barcode's 0th and 1st characters from the index.

### Examples
(One)
* barcode1: a1b2c3d4 has slices {1__a1,2__1b,3__b2,4__2c,5__c3,6__3d,7__d4}
* barcode2: b2c3d4e5 has slices {1__b2,2__2c,3__c3,4__3d,5__d4,6__4e,7__e5}
* The two sets of slices do not intersect.
* Because the sets of slices don't intersect, there's no need to compute the hamming distance.
* The hamming distance is 8.

(Two)
* barcode1: a1b2c3d4 has slices {1__a1,2__1b,3__b2,4__2c,5__c3,6__3d,7__d4}
* barcode2: a1ddc3d4 has slices {1__a1,2__1d,3__dd,4__dc,5__c3,6__3d,7__d4}
* The two sets of slices do intersect.
* Because the sets of slices intersect, we should compute the hamming distance.
* The hamming distance is 2.

(Three)
* barcode1: a1b2c3d4 has slices {1__a1,2__1b,3__b2,4__2c,5__c3,6__3d,7__d4}
* barcode2: a1eeeeee has slices {1__a1,2__1e,3__ee,4__ee,5__ee,6__ee,7__ee}
* The two sets of slices do intersect.
* Because the sets of slices intersect, we should compute the hamming distance.
* The hamming distance is 6.

There is room for improvement here. With only one slice that intersects, we
could avoid the hamming distance check. However, without an extension, postgres
does not have a function for returning the members or the number of members
of two arrays that intersect; the only built in function returns true/false
indicating whether there is an intersection.
