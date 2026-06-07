import { xxHash32 } from "js-xxhash";

/**
 * Layered Bloom Filter
 * =====================
 * A standard bloom filter has a fixed bit array. As you add more keys, bits fill
 * up and the false positive rate (FPR) climbs. Since the filter never stores the
 * original keys it cannot rehash them into a larger array if you want to resize.
 *
 * This implementation uses a *layered* approach: instead of one large fixed array
 * it maintains a list of inner filter layers. When a layer is full a new one is
 * appended — no existing bits are touched.
 *
 * How `has()` stays correct across layers
 * ----------------------------------------
 * A key is added to whichever layer was current at insertion time. `has()` returns
 * true if *any* layer reports the key present. Because bloom filters have no false
 * negatives, a key's own layer always says yes; other layers may say yes (false
 * positive) or no. The OR across layers is therefore sound.
 *
 * Bounding the combined FPR
 * -------------------------
 * With N layers each at error rate p the combined FPR is bounded by their sum
 * (union bound for independent events):
 *
 *   P(FP overall) ≤ Σ P(FP layer i)
 *
 * If every layer shared the same rate p the total would grow unboundedly with N.
 * Instead, each layer i gets a geometrically tighter per-layer target:
 *
 *   layer_fpr(i) = errorRate × r^(i+1)     where r = 0.5
 *
 * This geometric series converges:
 *
 *   Σ errorRate × 0.5^(i+1)  =  errorRate × 0.5 × 1/(1-0.5)  =  errorRate  ✓
 *
 * So no matter how many layers accumulate the combined FPR never exceeds errorRate.
 *
 * Space efficiency vs. a single large filter
 * -------------------------------------------
 * The per-layer FPR tightening costs bits: hitting a lower FPR requires more bits
 * per element. Each layer i needs roughly (i+1)×1.44 extra bits per key on top of
 * the base ~9.6 bits/key (at 1% FPR). That overhead compounds on larger layers.
 *
 * Example with initialCapacityN=1024, errorRate=0.01:
 *
 *   Layer │ Capacity │ Per-layer FPR │   Bytes │ Running total
 *   ──────┼──────────┼───────────────┼─────────┼──────────────
 *     0   │    1,024 │        0.50%  │   1,412 │       1,412
 *     1   │    2,048 │        0.25%  │   3,193 │       4,605
 *     2   │    4,096 │        0.13%  │   7,125 │      11,730
 *     3   │    8,192 │        0.06%  │  15,728 │      27,458
 *     4   │   16,384 │        0.03%  │  34,408 │      61,866
 *     5   │   32,768 │        0.02%  │  74,692 │     136,558
 *     6   │   65,536 │       0.008%  │ 161,306 │     297,864
 *     7   │  131,072 │       0.004%  │ 346,319 │     644,183
 *     8   │  262,144 │       0.002%  │ 739,523 │   1,383,706
 *
 * A single pre-allocated 1 MB filter at 1% FPR would hold ~875 K keys. At 1 MB the
 * layered approach fits 8 layers (~644 KB) holding ~261 K keys — roughly 3× fewer
 * keys per byte at capacity.
 *
 * The layered approach wins when actual key counts are much smaller than maxSizeBytes
 * could theoretically hold: the filter only commits memory as layers are created
 * rather than pre-allocating the full budget upfront. If you know you will push near
 * the limit, pass a large `initialCapacityN` so the first layer dominates and the
 * layering overhead is minimised.
 *
 * Double hashing (Kirsch–Mitzenmacher, 2006)
 * ------------------------------------------
 * A bloom filter needs k independent bit positions per key. Computing k separate
 * hash functions is expensive. Instead, only two xxHash32 calls are made per key
 * (h1 and h2, with different seeds), and k positions are derived cheaply:
 *
 *   position_i = (h1 + i × h2) mod m     for i = 0 … k-1
 *
 * This achieves the same asymptotic FPR as k truly independent hash functions.
 * Each layer uses a distinct seed pair (layerIndex×2, layerIndex×2+1) so bit
 * positions are independent across layers.
 */

const DEFAULT_ERROR_RATE = 0.01;
// Number of keys the first layer is sized to hold before a new layer is created.
const DEFAULT_INITIAL_CAPACITY = 1024;
const TIGHTENING_RATIO = 0.5;
const LAYER_GROWTH_FACTOR = 2;

interface Layer {
    bits: Uint8Array;
    capacity: number;
    count: number;
    k: number;
    m: number;
    layerIndex: number;
}

interface LayerSnapshot {
    bits: number[];
    capacity: number;
    count: number;
    k: number;
    m: number;
    layerIndex: number;
}

export interface BloomFilterSnapshot {
    version: 1;
    errorRate: number;
    maxSizeBytes: number;
    initialCapacityN: number;
    layers: LayerSnapshot[];
}

export class BloomFilter {
    private readonly layers: Layer[];
    private readonly errorRate: number;
    private readonly maxSizeBytes: number;
    private readonly initialCapacityN: number;
    private usedBytes: number;

    private constructor(layers: Layer[], errorRate: number, maxSizeBytes: number, initialCapacityN: number) {
        this.layers = layers;
        this.errorRate = errorRate;
        this.maxSizeBytes = maxSizeBytes;
        this.initialCapacityN = initialCapacityN;
        this.usedBytes = layers.reduce((sum, l) => sum + l.bits.byteLength, 0);
    }

    static create(options: { errorRate?: number; maxSizeBytes: number; initialCapacityN?: number }): BloomFilter {
        const errorRate = options.errorRate ?? DEFAULT_ERROR_RATE;
        const initialCapacityN = options.initialCapacityN ?? DEFAULT_INITIAL_CAPACITY;
        const firstBytes = layerByteSize(computeLayerBitCount(0, errorRate, initialCapacityN));
        if (firstBytes > options.maxSizeBytes) {
            throw new Error(
                `maxSizeBytes (${options.maxSizeBytes}) is too small for the initial layer (${firstBytes} bytes required)`,
            );
        }
        const first = buildLayer(0, errorRate, initialCapacityN);
        return new BloomFilter([first], errorRate, options.maxSizeBytes, initialCapacityN);
    }

    static fromSnapshot(snapshot: BloomFilterSnapshot): BloomFilter {
        return new BloomFilter(
            snapshot.layers.map(restoreLayer),
            snapshot.errorRate,
            snapshot.maxSizeBytes,
            snapshot.initialCapacityN,
        );
    }

    add(key: string): boolean {
        const current = this.layers[this.layers.length - 1];

        if (current.count >= current.capacity) {
            const nextIndex = this.layers.length;
            const nextBytes = layerByteSize(computeLayerBitCount(nextIndex, this.errorRate, this.initialCapacityN));
            if (this.usedBytes + nextBytes > this.maxSizeBytes) {
                return false;
            }
            this.layers.push(buildLayer(nextIndex, this.errorRate, this.initialCapacityN));
            this.usedBytes += nextBytes;
        }

        layerAdd(this.layers[this.layers.length - 1], key);
        return true;
    }

    has(key: string): boolean {
        return this.layers.some((layer) => layerHas(layer, key));
    }

    keyCount(): number {
        return this.layers.reduce((sum, layer) => sum + layer.count, 0);
    }

    toSnapshot(): BloomFilterSnapshot {
        return {
            version: 1,
            errorRate: this.errorRate,
            maxSizeBytes: this.maxSizeBytes,
            initialCapacityN: this.initialCapacityN,
            layers: this.layers.map((layer) => ({
                bits: Array.from(layer.bits),
                capacity: layer.capacity,
                count: layer.count,
                k: layer.k,
                m: layer.m,
                layerIndex: layer.layerIndex,
            })),
        };
    }
}

/**
 * Returns the per-layer false positive rate target for a given layer index.
 *
 * Each layer i gets a rate of `baseErrorRate × 0.5^(i+1)`. The sum of this
 * geometric series converges to `baseErrorRate`, keeping the combined FPR
 * across all layers bounded regardless of how many layers exist.
 */
function layerFpr(baseErrorRate: number, layerIndex: number): number {
    return baseErrorRate * Math.pow(TIGHTENING_RATIO, layerIndex + 1);
}

/** Converts a bit count to the number of bytes needed to hold it. */
function layerByteSize(m: number): number {
    return Math.ceil(m / 8);
}

/**
 * Returns the optimal number of bits (m) for a layer without allocating anything.
 *
 * Derived from the standard bloom filter formula for optimal bit count given a
 * desired false positive rate p and expected element count n:
 *
 *   m = -n × ln(p) / ln(2)²
 *
 * This minimises m for a given (n, p) pair. Layer capacity doubles each index
 * via LAYER_GROWTH_FACTOR so that later layers absorb exponentially more keys.
 * This function is intentionally separated from buildLayer so that the caller
 * can check whether the next layer fits within maxSizeBytes before allocating.
 */
function computeLayerBitCount(layerIndex: number, baseErrorRate: number, initialCapacity: number): number {
    const fpr = layerFpr(baseErrorRate, layerIndex);
    const capacity = initialCapacity * Math.pow(LAYER_GROWTH_FACTOR, layerIndex);
    return Math.ceil((-capacity * Math.log(fpr)) / (Math.LN2 * Math.LN2));
}

/**
 * Allocates and returns a new inner filter layer for the given index.
 *
 * The optimal number of hash rounds k is:
 *
 *   k = (m / n) × ln(2)
 *
 * This value of k minimises the FPR for the chosen (m, n) pair. k is clamped
 * to at least 1 to guard against degenerate configurations at very small sizes.
 */
function buildLayer(layerIndex: number, baseErrorRate: number, initialCapacity: number): Layer {
    const fpr = layerFpr(baseErrorRate, layerIndex);
    const capacity = initialCapacity * Math.pow(LAYER_GROWTH_FACTOR, layerIndex);
    const m = Math.ceil((-capacity * Math.log(fpr)) / (Math.LN2 * Math.LN2));
    const k = Math.max(1, Math.round((m / capacity) * Math.LN2));
    return { bits: new Uint8Array(layerByteSize(m)), capacity, count: 0, k, m, layerIndex };
}

/** Restores an inner filter layer from its snapshot representation. */
function restoreLayer(s: LayerSnapshot): Layer {
    return { bits: new Uint8Array(s.bits), capacity: s.capacity, count: s.count, k: s.k, m: s.m, layerIndex: s.layerIndex };
}

/**
 * Returns the k bit positions for a key within a layer using double hashing.
 *
 * Only two xxHash32 calls are made (h1 and h2 with different seeds). All k
 * positions are then derived via the Kirsch–Mitzenmacher formula:
 *
 *   position_i = (h1 + i × h2) mod m     for i = 0 … k-1
 *
 * h2 is forced to at least 1 to prevent all positions collapsing to `h1 mod m`
 * in the rare case where xxHash32 returns 0 for the given key and seed.
 *
 * Seeds are offset by layerIndex so each layer hashes keys independently, which
 * is required for the combined FPR bound to hold.
 */
function bitPositions(key: string, k: number, m: number, layerIndex: number): number[] {
    const h1 = xxHash32(key, layerIndex * 2);
    const h2 = xxHash32(key, layerIndex * 2 + 1) || 1;
    const positions: number[] = [];
    for (let i = 0; i < k; i++) {
        positions.push((h1 + i * h2) % m);
    }
    return positions;
}

/**
 * Returns true if all k bit positions for the key are set in this layer.
 *
 * A bloom filter has no false negatives: if the key was added to this layer
 * every one of its k positions was set, so all k checks pass. The converse is
 * not guaranteed — another key (or set of keys) may have coincidentally set the
 * same positions, producing a false positive at rate layer_fpr(layerIndex).
 */
function layerHas(layer: Layer, key: string): boolean {
    return bitPositions(key, layer.k, layer.m, layer.layerIndex).every(
        (pos) => (layer.bits[pos >> 3] & (1 << (pos & 7))) !== 0,
    );
}

/**
 * Sets the k bit positions for the key in this layer's bit array.
 *
 * Bit position `pos` maps to byte index `pos >> 3` (i.e. pos ÷ 8) and bit
 * offset `pos & 7` (i.e. pos mod 8) within that byte.
 */
function layerAdd(layer: Layer, key: string): void {
    for (const pos of bitPositions(key, layer.k, layer.m, layer.layerIndex)) {
        layer.bits[pos >> 3] |= 1 << (pos & 7);
    }
    layer.count++;
}
