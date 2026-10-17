# How Distributed Sharded Databases Split (and Merge) Shards, Ranges, Tablets and Regions

The systems split shards in very different ways. Range-sharded systems built on an LSM storage engine (Bigtable, Spanner, CockroachDB, TiKV, HBase, YugabyteDB, FoundationDB) split automatically, mostly on size, and many also split on load. Most choose the split point cheaply from storage-engine metadata or from sampled request keys. The decision is made by the node holding the data (CockroachDB, HBase, the Bigtable tablet server), by a central coordinator (TiDB's PD, YugabyteDB's master, FoundationDB's data distributor, Spanner's and Cloud Bigtable's placement processes), or not at all: Vitess and, since 6.0.3, MongoDB do not auto-split. Hash-partitioned managed services (DynamoDB, Cosmos DB) split partitions internally when they cross fixed size and throughput limits, and they rarely or never merge.

## TL;DR

- **Triggers:** size thresholds are universal. The defaults are 512 MiB in CockroachDB, a 10 GB maximum file size in HBase, 10 GB per partition in DynamoDB, 50 GB in Cosmos DB, and roughly 10 GB (twice the 5 GB target) for ScyllaDB tablets. Load-based splitting (on QPS, CPU, or bytes/sec) is now standard in CockroachDB (2500 QPS or 500 ms CPU/sec), TiKV (3000 QPS, 30 MiB/s, or 0.25 CPU, sustained for 10 s), Spanner, Cloud Bigtable, DynamoDB ("split for heat"), and FoundationDB (write bandwidth). Vitess resharding is always an operator-run VReplication workflow.
- **Split-point selection:** there are two families. *Size splits* pick an approximate byte midpoint from storage metadata: TiKV uses RocksDB table properties, HBase takes the midkey of the HFile block index of the largest store file, and FoundationDB uses sampled byte histograms. *Load splits* pick the key that best balances sampled request traffic while minimising requests that would straddle the new boundary: CockroachDB uses a reservoir sample of request spans with left/right/contained counters, and TiKV uses balance and contained scores. All of them refuse to split a single hot key, and most detect sequential (monotonic) access and decline to split.
- **Stats and decisions:** durable stats are almost always storage-engine file metadata (SST/HFile properties and indexes) plus per-range accounting (CockroachDB's MVCCStats). Load stats are almost always in memory and ephemeral. The authoritative shard map lives in a replicated meta table (Bigtable/HBase METADATA/hbase:meta, CockroachDB meta2, FDB `\xff/keyServers`, ScyllaDB `system.tablets`, MongoDB `config.chunks`, the Vitess topo). Merge logic uses lower thresholds and cooldowns to avoid thrash: TiKV's `split-merge-interval` of 1h, CockroachDB's sticky bits on manual splits, and ScyllaDB's 2× / 0.5× hysteresis band.

## Key Findings

1. **Splitting is cheap only because the storage layer makes it cheap.** In Bigtable "the immutability of SSTables enables us to split tablets quickly. Instead of generating a new set of SSTables for each child tablet, we let the child tablets share the SSTables of the parent tablet."\[1\] HBase does the same with reference files, and CockroachDB and TiKV split by rewriting metadata inside a single LSM instance. Systems that physically copy data to split (Vitess, MongoDB moveRange) make splitting an explicit, heavyweight operation.
2. **Load-based splitting has to be guarded against making things worse.** CockroachDB raised its load-split QPS threshold from 250 to 2500 after experiments showed that "over-splitting" hurt performance.\[2\]\[3\] In its first experiments, "on a write heavy workload … load based splitting is explicitly making things worse."\[4\] Every load splitter checks balance (don't split if 99% of load is on one key) and boundary-crossing (don't split ranges served mostly by scans).\[5\]
3. **No system can fix a monotonically increasing key by splitting.** The AWS DynamoDB team, CockroachDB (`kv.loadsplitter.cleardirection`, >80% in one direction), Spanner and FoundationDB all document that splitting at the tail of an append-only keyspace just moves the hotspot to the new right-hand shard. The only fix is at the schema level: hash prefixes, bucketing, or UUIDs.
4. **The trend is toward fewer, larger, load-aware units and centralised balancing.** MongoDB removed auto-splitting in 6.0.3 and balances by data size. ScyllaDB moved from static vnodes to dynamically split tablets with a Raft-backed load balancer. Per the TiDB 8.4.0 release notes, TiKV's default `region-split-size` changed from 96 MiB to 256 MiB (and `region-split-keys` from 960,000 to 2,560,000) "to avoid the extra overhead caused by too many Regions", alongside larger PD merge thresholds. CockroachDB moved load splitting from QPS to CPU.

## Details by System

### Google Bigtable

- **Scheme and unit:** range-partitioned by row key. "The row range for a table is dynamically partitioned. Each row range is called a tablet, which is the unit of distribution and load balancing" (OSDI 2006).\[1\]
- **Triggers:** size. "Initially, each table consists of just one tablet. As a table grows, it is automatically split into multiple tablets, each approximately 100-200 MB in size by default."\[1\]\[6\] The paper predates load splits, but Cloud Bigtable's current docs say a primary process "splits busier or larger tablets in half and merges less-accessed/smaller tablets together … If a certain tablet gets a spike of traffic, Bigtable splits the tablet in two, then moves one of the new tablets to another node."\[7\]
- **Split point:** the paper does not say how the split key is chosen. The often-repeated "SSTable index midpoint" is plausible, because each SSTable has an in-memory block index of roughly 64 KB blocks, but it is not stated in the paper and should be treated as inference. Cloud Bigtable describes splitting "in half", by size or by load.
- **Statistics:** durable state is the METADATA table, which holds each tablet's location, its list of SSTables, and redo-log pointers. METADATA tablets are capped at 128 MB, and the root tablet "is never split". SSTable block indexes are persisted at the end of each file and loaded into memory.\[1\] Load statistics are not described in the paper; in Cloud Bigtable they are internal to the service.
- **Decision maker:** a hybrid. The *tablet server* initiates splits and "commits the split by recording information for the new tablet in the METADATA table. When the split has committed, it notifies the master". If that notification is lost, the master finds the split the next time it asks a server to load the now-partial tablet.\[1\]\[6\] The *master* initiates merges, creation and deletion.\[8\] Rebalancing is throttled, and a moved tablet is typically unavailable for under a second.\[1\] In Cloud Bigtable, rebalancing is pointer-only because data lives on Colossus.\[7\]

### Google Spanner

- **Scheme and unit:** range sharding by primary key ("A database is horizontally row-range sharded", SIGMOD 2017).\[9\] The paper unit is the *directory* ("a set of contiguous keys that share a common prefix"), which can be broken into *fragments* when it grows too large.\[10\] Cloud Spanner's user-facing unit is the "split". A Spanner tablet is a container that may hold several row ranges, unlike a Bigtable tablet.\[10\]
- **Triggers:** size and load. "Spanner automatically adds and removes split boundaries based on size and load."\[11\] For bulk loads, "after a few minutes of high load, Spanner introduces split boundaries between rows", and throughput should "double every few minutes until you saturate the available CPU."\[12\] Load is measured with a CPU-based `CPU_USAGE_SCORE`. A split is "hot" when 100% of its load is constrained, and splits that stay hot for 10 or more minutes are flagged for application-level fixes.\[13\] Size thresholds are not officially published. Third-party blogs cite 4 GB or "approximately 8 GB", which contradict each other, so treat both as unverified.\[14\]\[15\]
- **Split point:** boundaries are "specified as ranges of key prefixes and preserve row co-location of interleaved child tables."\[9\] In practice splits fall between parent rows of an interleave hierarchy, as long as a parent row plus its children stay under the size limit and aren't hot.\[16\] Load-based boundaries follow the key distribution of the workload and can isolate hot rows.\[14\] Spanner "refrains from excessively splitting the ranges covered by" scans.\[13\] The storage layer also keeps "a list of evenly distributed split points" for subshard query parallelism.\[9\]
- **Statistics:** shard metadata is in Spanner's own replicated metadata. Hot-split statistics are exposed in `SPANNER_SYS.SPLIT_STATS_TOP_MINUTE` at 1-minute granularity.\[13\] The internal load counters are not documented.
- **Decision maker:** centralised. The placement driver "periodically communicates with the spanservers to find data that needs to be moved, either to meet updated replication constraints or to balance load." Data moves with *movedir*, a background copy followed by a short transaction to switch the final "nominal amount" and update metadata.\[10\] Merges exist: "Shard boundaries may change dynamically", and queries must cope with "ongoing splitting, merging, and moving of the data."\[9\]

### CockroachDB

- **Scheme and unit:** a single sorted, range-partitioned keyspace split into *ranges*, each a Raft group. Hash-sharded indexes are an optional schema-level prefix and do not change the range mechanics.
- **Triggers:**
  - *Size:* `range_max_bytes`, default 512 MiB (zone config). The default was raised from 64 MiB in v20.1. `range_min_bytes` defaults to 128 MiB.\[17\] `kv.range.range_size_hard_cap` (8 GiB) blocks writes to a range that cannot split.\[18\]
  - *Load:* `kv.range_split.by_load_enabled` (default true), plus `kv.range_split.load_qps_threshold` (default 2500) or `kv.range_split.load_cpu_threshold`.\[18\] The CPU threshold was 250 ms/sec when introduced in v23.1\[5\]\[19\] and is 500 ms/sec in current v26.x docs.\[18\]\[19\] Load splitting uses CPU when the rebalancing objective is CPU, which is now the default.\[18\]
  - *Manual:* `ALTER TABLE/INDEX … SPLIT AT … [WITH EXPIRATION]` and `UNSPLIT AT`.
  - *Schema boundaries:* ranges split at table and index boundaries, and zone-config boundaries force splits.
- **Split point:**
  - *Size splits* pick an approximate byte midpoint of the range's MVCC data, then adjust it so a SQL row (all its column families) is not split. GitHub issue #43094 documents that keeping this guarantee is hard, because "splitting is inherently a KV operation."\[20\]
  - *Load splits* use the `split` package's finder (PR #31413). Once over threshold, "we begin tracking spans … We use a reservoir sample, inserting span.Key. If a sample doesn't replace an entry in the reservoir, we … increment one or both of two counters … left & right … If the span overlaps the entry's key, then both counters are incremented. When a range has seen sustained load over the threshold rate for a threshold duration, the key from the reservoir sample containing the most even balance of left and right values is chosen."\[2\] Candidates are also penalised by the count of requests that span the split point.\[2\] Later versions added a *weighted* finder (CPU-weighted samples) beside the unweighted one. The reported constants are a sample size of 20 and a minimum counter of 100, from a secondary code walkthrough. A 25% popular-key threshold matches the docs' `kv.loadsplitter.popularkey` metric ("most popular sampled split key occurs in >= 25% of the samples"). When no key qualifies, it logs "no split key found" with "popular key" or "clear direction" (>80% monotonic) diagnostics.\[5\]
- **Statistics:**
  - *Durable:* MVCCStats (live, key, value and intent bytes, and so on) are maintained incrementally by every Raft command and persisted in each range's replicated range-local keys. That is why size-split checks need no scan. Range descriptors live in meta1/meta2 ranges, and manual-split "sticky bits" are stored on the range descriptor.
  - *In memory (leaseholder only):* QPS/CPU load counters and the split finder's reservoir. These are lost on lease transfer. The merge queue has to RPC the right-hand neighbour's leaseholder (`RangeStats`) to learn its load.
- **Decision maker:** decentralised. Each store's *split queue* and *merge queue* scan only the replicas it holds leases for,\[21\] and the leaseholder issues `AdminSplit` as a transaction with a commit trigger.
- **Merges:** added in v19.1. A range below `range_min_bytes` is considered for merging with its right neighbour. The LHS coordinates.\[22\] Replica sets must first be aligned on the same stores,\[21\] and the combined range must be under the maximum size\[21\]\[22\] and, for load-split ranges, under the load threshold. The original RFC requires a merge to sit below the minimums for *both* size and load, while a split happens on *either*.\[23\] Per GitHub issue #171648, the merge queue runs with a default `kv.range_merge.queue_interval` of 5s and a concurrency of 1. Manual splits set a sticky bit (optionally expiring) that the merge queue respects. A 2026 GitHub issue (#171648) shows a non-expiring sticky bit can make the merge queue busy-loop.\[24\] A load-split merge delay setting (`kv.range_split.by_load_merge_delay`) exists to keep freshly load-split ranges from being merged back at once; its default could not be verified from public docs.

### TiDB / TiKV / PD

- **Scheme and unit:** range-partitioned *Regions* (Raft groups) over the ordered key space. At startup there is one Region covering (-inf, +inf).\[25\]
- **Triggers:**
  - *Size/keys:* `coprocessor.region-split-size` and `region-split-keys`. These were 96 MiB and 960,000 keys until, per the TiDB 8.4.0 release notes, the defaults changed to 256 MiB and 2,560,000 keys "to avoid the extra overhead caused by too many Regions"; docs recommend 48–256 MiB, warning against more than 1 GiB. `region-max-size` defaults to `region-split-size / 2 * 3` (1.5×).\[26\] A split check runs once a Region has written enough new data since the last check.\[25\]
  - *Load Base Split:* triggered if, for 10 consecutive seconds, read QPS exceeds `split.qps-threshold` (3000, or 7000 when region-split-size ≥ 4 GiB), traffic exceeds `split.byte-threshold` (30 MiB/s, or 100 MiB/s), or Unified Read Pool CPU exceeds `split.region-cpu-overload-threshold-ratio` (0.25, or 0.75; added in v6.2).\[26\]\[27\]
  - *Table split:* `coprocessor.split-region-on-table`.\[28\]
  - *Manual:* `SPLIT TABLE … BETWEEN … REGIONS` / `pd-ctl`.
- **Split point:**
  - *Size splits:* the split checker gets the approximate size from RocksDB table properties. A custom collector records sizes and key counts of small sub-ranges in each SST, so "we can calculate the approximate size of a region from the table properties without scanning any data at all."\[29\] The original scanning checker records the key where accumulated size first exceeds `region-split-size`, and reports splittable if the total exceeds `region-max-size`.\[25\] There is also a "half" policy that uses the approximate middle key (`middle_key_approximate_size` in `tikv-ctl`).\[30\]
  - *Load splits:* the split controller samples request keys and chooses a key that balances load (`split.split-balance-score`) and keeps requests from crossing the boundary (`split.split-contained-score`).\[27\]\[28\]
- **Statistics:**
  - *Durable:* SST table properties (range/size/keys and MVCC properties) inside the SST files, and Region metadata (epoch, peers) in RocksDB. PD persists Region metadata to its etcd-backed storage.
  - *In memory:* approximate Region size and keys cached on the TiKV leader, and refreshed after compaction events because compactions drop entries.\[31\] These are reported to PD in Region heartbeats, together with read/write flow. PD's hot-Region statistics are in memory. Approximations can drift; GitHub issue #6161 notes that delete tombstones skew them.\[32\]
- **Decision maker:** a hybrid. TiKV decides size and load splits locally and sends `AskBatchSplit` to PD only to get new Region and peer IDs,\[25\] then reports the result. PD is the central scheduler for merges, hot-Region balancing and replica placement. The TiKV RFC on dynamic Region size proposes that hot-spot splits be PD-triggered using global statistics.\[33\]
- **Merges:** PD's merge checker merges a Region with the smaller of its neighbours when size is under `max-merge-region-size`\[34\] (54 MiB since v8.4.0, previously 20) *and* keys are under `max-merge-region-keys` (540,000, previously 200,000).\[35\] The Regions' replicas must sit on the same TiKV stores. `split-merge-interval` (default 1h) blocks merging newly split Regions and also blocks merges for 1h after PD restarts.\[34\]\[36\] This is the main anti-thrash guard. `merge-schedule-limit` (8) limits concurrency.\[37\]

### Apache HBase

- **Scheme and unit:** range-partitioned *regions* by row key. Pre-splitting is supported at table creation.
- **Triggers:** checked after memstore flushes and compactions, by a pluggable `RegionSplitPolicy` (`hbase.regionserver.region.split.policy`):\[38\]
  - `ConstantSizeRegionSplitPolicy`: split when any store exceeds `hbase.hregion.max.filesize`, default 10 GB, with optional `hbase.hregion.max.filesize.jitter`.\[39\]\[40\]
  - `IncreasingToUpperBoundRegionSplitPolicy` (default 0.94–1.x): the split size is "the number of regions that are on this server that all are of the same table, cubed, times 2x the region flush size OR the maximum region split size, whichever is smaller."\[41\]\[42\]
  - `SteppingSplitPolicy` (default since 2.0, HBASE-16765): split at 2 × flush size when the table has exactly one region on this server, otherwise at max file size. It was introduced because the cubic policy "does tend to produce too many regions" for small tables.\[43\]
  - `KeyPrefixRegionSplitPolicy` / `DelimitedKeyPrefixRegionSplitPolicy`: change only the split *point*. `BusyRegionSplitPolicy` adds a load trigger based on blocked requests.
  - `DisabledRegionSplitPolicy`.\[44\]
  - Manual `split` from the shell.
- **Split point:** for all default policies, the midkey of the *largest store file in the largest store*, taken from the HFile's multi-level block index\[40\] (`HFileBlockIndex…midkey`).\[45\] If that key is the first or last row of the file, there is no split point.\[42\] The prefix policies truncate the midkey to a prefix or delimiter so rows sharing a prefix stay together. Splits never break a row.
- **Statistics:**
  - *Durable:* HFile trailers and block indexes (midkey, entry counts, sizes) in HDFS, region info and state in `hbase:meta`, and split/merge procedure state in the master's procedure store.
  - *In memory:* store sizes, and the request counters used by BusyRegionSplitPolicy.
- **Decision maker:** the RegionServer decides when to split under the policy. Since HBase 2.0 the split is carried out as a master-side procedure (`SplitTableRegionProcedure`), which creates reference files for the daughters and updates `hbase:meta`. Merges are master-driven, either through manual `merge_region` or the optional Region Normalizer, which splits large regions and merges small ones relative to the table's average.

### MySQL with Vitess

- **Scheme and unit:** a *keyspace* split into *shards* that cover contiguous ranges of the keyspace ID (for example `-80`, `80-`). Each row's keyspace ID is computed by the table's *primary vindex*, typically `hash` ("a pseudo-random way of distributing rows"),\[46\] but range, lookup and custom vindexes such as region-based ones are possible. Sharding is therefore hash-then-range.
- **Triggers:** **none are automatic.** Vitess keeps no per-shard split thresholds and never splits on its own. Operators decide, using MySQL and host metrics (size, QPS, replication lag), when to reshard and which keyranges to create. The docs list the questions an operator must answer to choose a vindex: highest-QPS queries, cardinality, and which rows should share a shard for joins or transactions.\[46\]
- **Mechanism:** the `Reshard` VReplication workflow:
  1. Provision target shards.
  2. `Reshard … create --source-shards '0' --target-shards '-40,40-80,80-c0,c0-'`,\[47\] which copies each row to the target whose keyrange contains its keyspace ID (`in_keyrange(...)` filters) and then streams binlogs.
  3. Verify with `VDiff`.
  4. `SwitchTraffic` for replica/rdonly and then primary. `--max-replication-lag-allowed` defaults to 30s, and reverse replication is enabled by default\[48\] so the switch can be undone with `ReverseTraffic`.
  5. `complete` to clean up.
  
  Merging shards is the same workflow run toward fewer target shards. `MoveTables` does vertical splitting into another keyspace.\[49\]
- **Split point:** chosen by the operator as keyrange boundaries. Usually these are even splits of the hashed keyspace ID space, which is why hash vindexes give near-uniform shards.
- **Statistics:** Vitess persists no load or size stats for splitting. Durable state is the topology server (etcd/ZooKeeper/Consul): the VSchema in the global topo and `SrvKeyspace` partitions per cell, which are rewritten at cutover.\[50\] Workflow state lives in each target's `_vt.vreplication` table.
- **Decision maker:** a human or an external automation tool, through vtctld.

### YugabyteDB (DocDB)

- **Scheme and unit:** *tablets* (Raft groups). Hash sharding by default; range sharding on request.
- **Triggers:** automatic tablet splitting (`enable_automatic_tablet_splitting`) is size-based and phased by tablet count per node:
  - *Low phase:* split tablets larger than `tablet_split_low_phase_size_threshold_bytes` while each node has fewer than `tablet_split_low_phase_shard_count_per_node` (8) tablets for the table. Current docs give 128 MiB; earlier releases and blogs give 512 MiB, so check your version.\[51\]\[52\]
  - *High phase:* until `tablet_split_high_phase_shard_count_per_node` (24), split at `tablet_split_high_phase_size_threshold_bytes` (10 GiB).\[51\]\[53\]
  - *Beyond that:* only `tablet_force_split_threshold_bytes` (reported as 100 GiB default in docs; 500 GB in a 2022 GitHub guidance issue) applies.\[53\]
  - Manual `yb-admin split_tablet` and pre-split DDL are also available.
  
  Size is "measured by the size of SST files in the leader tablet peer."\[52\] The phases deliberately split small tables early, for parallelism, and large ones rarely, to limit tablet counts.
- **Split point:** the master asks the tablet leader for a split key (the "Get Tablet Split Key" task).\[54\] The leader derives an approximate middle key from its RocksDB SST metadata. For hash tables the key is a hash-code midpoint.
- **Statistics:** durable SST metadata on tservers, and tablet metadata in the master's `sys.catalog`. Sizes are reported to the master in tserver heartbeats.
- **Decision maker:** centralised. The yb-master drives splitting, then "Split Tablet, then Stepdown Leader and Delete Tablet" for the parent.\[54\] Automatic tablet merging is not part of DocDB's documented automatic flow.

### Apache Cassandra and ScyllaDB

- **Cassandra (vnodes):** hash (Murmur3) token ring with statically assigned token ranges (`num_tokens`). Token ranges are **not** split dynamically by size or load. Ownership changes only on topology operations (bootstrap/decommission), and hot partitions cannot be split because a partition is the atomic unit.
- **ScyllaDB tablets (production since 2024.2, default since 2025.1):** each table's token range is divided into *tablets*, each with its own memtables and SSTables.
  - *Triggers and point:* split and merge are **table-wide resizes**. "If … the average tablet size is larger than double the target tablet size, the table will be split (the number of tablets will be doubled), and if the average tablet size is smaller than half the target tablet size, it will be merged."\[55\] The target is about 5 GB (`target_tablet_size_in_bytes`), so splits happen at about 10 GB and merges at about 2.5 GB.\[56\] Each tablet splits at its token-range midpoint, which is valid because tokens are uniformly hashed. The doubling/halving band is built-in hysteresis.
  - *Load:* load-based ("temperature-based") balancing is described as future work.\[57\] Today's balancer equalises disk usage and tablet counts.\[55\]
  - *Statistics:* `system.tablets` (durable, Raft-replicated group0 metadata: tablet count, token boundaries, replicas, transition state). Tablet sizes are computed from actual disk usage.
  - *Decision maker:* a centralised, Raft-based tablet load balancer and scheduler.\[58\]\[59\] A 2024 GitHub issue (#20054) showed splits stalling under heavy writes, with average tablet size reaching 53 GiB.\[60\]

### MongoDB

- **Scheme and unit:** range or hashed shard keys, split into *chunks* (now called "ranges"). The default range size is 128 MB (it was 64 MB before 6.0), configurable from 1 MB to 1024 MB.\[61\]\[62\]
- **Triggers:** before 6.0.3, an *auto-splitter* split chunks that exceeded the chunk size, based on approximate in-memory write tracking. **Since 6.0.3, "automatic chunk splitting is not performed … because of balancing policy improvements. Auto-splitting commands still exist, but do not perform an operation."**\[63\] The balancer now migrates data when the size difference between shards for a collection is at least 3× the range size (384 MB at the default).\[64\] `moveRange` splits as part of moving. Manual `sh.splitAt` and `sh.splitFind` remain, as does pre-splitting empty collections with `moveRange`. MongoDB warns against manually splitting populated collections.\[65\]
- **Split point:** `splitAt` uses the operator's key. `splitFind` and the old auto-splitter used the median of the chunk's documents in shard-key order. A chunk whose keys are all the same value cannot split and is marked *jumbo*; the jumbo flag is not cleared automatically when the chunk shrinks.\[62\]
- **Statistics:** durable chunk and range metadata in the config servers (`config.chunks`, `config.settings`). Data sizes are computed on shards on demand (`dataSize`).
- **Decision maker:** centralised. The balancer runs on the config server primary. The **AutoMerger** (7.0+, `sh.startAutoMerger`) merges contiguous chunks on the same shard,\[63\] and defragmentation reduces chunk counts.

### FoundationDB

- **Scheme and unit:** range-partitioned *shards* (key ranges), each owned by a storage-server team.
- **Triggers:** size (`shard.bytes > maxShardSize`, split priority `PRIORITY_SPLIT_SHARD` = 950) and write bandwidth (`bytesWrittenPerKSecond > SHARD_MAX_BYTES_PER_KSEC`).\[66\] The target size grows with database size to about 125–500 MB at the largest scales.\[67\] Storage servers notify data distribution "once a shard size changes by more than 5% from the previous estimate."\[68\] Read-hot shards are not split; data distribution only moves them, and the 7.2+ read-aware rebalancing still "doesn't split shard", per a 2025 forum report.\[67\]\[69\] Version 7.3's `rangeconfig` lets users inject explicit shard boundaries.\[70\]
- **Split point:** storage servers use "their sampled byte/write histograms to compute split points". Each resulting piece must be under `SHARD_SPLIT_BYTES_PER_KSEC` (described as 250 MB/s), and a single hot key cannot be split.\[66\]
- **Statistics:** shard-to-server mapping is durable in the system keyspace (`\xff/keyServers/`, `\xff/serverKeys/`), guarded by `moveKeysLock`.\[66\] Byte samples are maintained on storage servers. Bandwidth metrics are in memory.
- **Decision maker:** centralised. A singleton data distributor (recruited by the cluster controller) runs per-shard trackers, and the `DDQueue` handles relocations. Shards that are too small are merged with neighbours.\[66\]

### Amazon DynamoDB

- **Scheme and unit:** hash of the partition key, with items with the same partition key ordered by sort key. The unit is the *partition*; the AWS DynamoDB Developer Guide states "each individual partition is limited to 1,000 write units per second and 3,000 read units per second," and third-party sources put its storage limit at about 10 GB.
- **Triggers:** size, when a partition nears 10 GB. Throughput: when provisioned capacity requires more partitions, or through **split for heat**, part of adaptive capacity, which can "spread data items having the same partition key value across different partitions" when one partition gets uneven traffic. AWS calls when and how this happens "an implementation detail."\[71\]
- **Split point:** not public. Because split for heat can cut within an item collection, the cut is along sort-key order. It "will only execute when it determines the split would be sufficiently beneficial based on recent activity". It is skipped for ever-increasing sort keys, "because no matter the chosen cut point, all new writes … will be on the second partition."\[71\] Item collections are not split when the table has an LSI.\[72\]
- **Statistics and decision maker:** internal and service-managed. "There is no notice that indicates when a partition has split." **"Partitions today are only split, never merged."**\[71\]

### Azure Cosmos DB

- **Scheme and unit:** the hash of the partition key maps logical partitions (each at most 20 GB) onto *physical partitions* (at most 50 GB and 10,000 RU/s).\[73\]\[74\]
- **Triggers:** storage over 50 GB, or provisioned RU/s rising above the number of partitions × 10,000.\[75\]\[76\] Admins can also redistribute RU/s per physical partition (preview), and setting a partition above 10,000 RU/s triggers a split.\[77\]
- **Split point:** a split of the hash range such that "roughly half of the data is on one physical partition, and half is on the other".\[78\] A logical partition is never split, so a hot logical key moves intact.\[73\]\[79\] Hierarchical partition keys relax the 20 GB per-prefix limit.\[78\]
- **Statistics and decision maker:** service-managed. Per-partition RU metrics are exposed through Azure Monitor (`PhysicalPartitionThroughput`).\[77\] Merging was historically unsupported ("Cosmos DB doesn't currently supporting 'merging' partitions back together" — Microsoft Q&A), but Microsoft Learn now documents "Merge partitions in Azure Cosmos DB (preview)", which "allows you to reduce the number of physical partitions used for your container in place" for API for NoSQL or MongoDB ≥3.6 containers with provisioned throughput and a single write region.

### Citus (PostgreSQL)

- Citus was outside the scope of the primary research for this report, so treat this section as orientation only. Citus hash-distributes tables into a fixed number of shards chosen at distribution time. It has no automatic size or load splitting. Operators split shards explicitly (for example, isolating a hot tenant to its own shard, or splitting at chosen hash points) and use the shard rebalancer to move them. In that respect it resembles Vitess more than CockroachDB.

## Comparison Table

| System | Sharding scheme / unit | Split triggers | Split-point selection | Stats kept: durable vs in-memory | Decision maker | Merge support |
|---|---|---|---|---|---|---|
| Bigtable | Range / tablet | Size (~100–200 MB in paper); load and size in Cloud Bigtable | Not specified in paper; "in half" (Cloud) | Durable: METADATA table, SSTable block indexes; load: internal | Tablet server splits; master assigns and merges (Cloud: primary process) | Yes, master-initiated |
| Spanner | Range (key-prefix) / split (directory, fragment) | Size + load (CPU-based), after minutes of sustained load | Prefix boundaries respecting interleaving; load-distribution-based; avoids splitting scanned ranges | Durable metadata; hot-split stats in SPANNER_SYS (1-min) | Placement driver (central) + movedir | Yes, automatic |
| CockroachDB | Range / range | Size 512 MiB; load 2500 QPS or 500 ms CPU/s; manual SPLIT AT; schema/zone boundaries | Size: byte midpoint, row-safe; Load: reservoir sample with left/right/contained counters, balance + crossing | Durable: MVCCStats in range-local keys, meta2 descriptors, sticky bits; In-memory: QPS/CPU, samples (leaseholder) | Decentralised per-leaseholder split/merge queues | Yes (v19.1+), < range_min_bytes 128 MiB, sticky bits, load-merge delay |
| TiKV/PD | Range / Region | Size/keys (region-split-size, ×1.5 max); load 3000 QPS / 30 MiB/s / 0.25 CPU for 10 s; table; manual | Size: approx. keys from RocksDB table properties (scan or half); Load: sampled keys, balance + contained scores | Durable: SST properties, Region meta, PD storage; In-memory: approximate size cache, heartbeat flow stats | TiKV decides splits (PD allocates IDs); PD central for merge and hot scheduling | Yes, PD: < 54 MiB and < 540k keys, split-merge-interval 1h |
| HBase | Range / region | Store size via policy (Stepping default; max.filesize 10 GB); Busy policy; manual | Midkey of largest HFile in largest store (block index); prefix-truncation policies | Durable: HFile trailer/index, hbase:meta, procedure store; In-memory: sizes, request counts | RegionServer policy + master procedure | Yes, manual or Normalizer |
| Vitess | Hash vindex → keyspace-ID ranges / shard | Operator only | Operator-chosen keyrange boundaries | Durable: topo (VSchema, SrvKeyspace), _vt.vreplication; no split stats | Human/automation via Reshard workflow | Yes, Reshard to fewer shards |
| YugabyteDB | Hash (default) or range / tablet | Size, phased: 128 MiB (docs; 512 MiB older) below 8 tablets/node, 10 GiB below 24, force threshold above | Leader-computed approximate midpoint from SST metadata | Durable: SSTs, sys.catalog; heartbeated sizes | yb-master (central) | Not automatic |
| Cassandra | Hash token ring / vnode | None (topology changes only) | n/a | system tables | n/a | n/a |
| ScyllaDB | Hash tokens / tablet | Avg tablet size > 2× target (5 GB) → double count | Token-range halves | Durable: system.tablets (Raft); sizes from disk | Central Raft-based load balancer | Yes, < 0.5× target → halve |
| MongoDB | Range or hashed / chunk (range) | Pre-6.0.3: size (auto-split); 6.0.3+: no auto-split; balancer by data size (3× range size) | Manual splitAt / median (splitFind); moveRange | Durable: config.chunks / settings on config servers | Balancer on config server primary | Yes, AutoMerger (7.0+) |
| FoundationDB | Range / shard | Size (maxShardSize, ~125–500 MB); write bandwidth | Sampled byte/write histograms on storage servers | Durable: \xff/keyServers, \xff/serverKeys; byte samples on SS; in-memory bandwidth | Singleton data distributor | Yes, small shards merged |
| DynamoDB | Hash (+ sort-key order) / partition | ~10 GB; 3000 RCU / 1000 WCU; split for heat | Undisclosed; skips monotonic sort keys | Internal | Service | **No** ("only split, never merged") |
| Cosmos DB | Hash / physical partition | 50 GB; > 10,000 RU/s per partition; RU redistribution | Roughly half of the hash range; never splits a logical partition | Internal; Azure Monitor metrics | Service | Preview only (partition merge, per Microsoft Learn) |
| Citus | Hash / shard | Manual | Operator split points / tenant isolation | Postgres metadata tables | Operator + rebalancer | Manual |

## Cross-Cutting Patterns and Trade-offs

**1. Size vs load-based splitting.** Size splitting is deterministic, cheap (backed by persisted stats) and essential for rebalancing and recovery granularity. It is blind to skew, though: a 50 MB range taking 50% of cluster traffic will never split on size. Load splitting fixes that but is noisy, workload-dependent and easy to overdo. CockroachDB's own history shows this: the threshold rose from 250 to 2500 QPS, and when CPU-based splitting arrived, GitHub issue #96869 reported that "when running TPCE (50k), CPU splitting with a 250ms threshold performed 1 load based split whilst QPS splitting (2500) performed 12.5." The best designs:
- run both triggers and split on *either* (CockroachDB, TiKV, Spanner, FDB);
- measure load in the unit that actually saturates, which means CPU rather than QPS (CockroachDB 23.1+, TiKV 6.2+, Spanner's CPU_USAGE_SCORE);
- require the load to be *sustained* (TiKV 10 s, Spanner minutes, CockroachDB a threshold duration).

**2. Split-point selection: metadata for size, sampling for load.** Size midpoints come almost for free from LSM metadata: HFile block indexes, RocksDB table-property collectors, FDB byte samples, or CockroachDB's incremental MVCCStats plus a key search. Load split points need a bounded-memory estimate of the request-key distribution. Reservoir sampling of request start keys, with per-sample left/right/contained counters, gives a split key that both balances load and keeps cross-boundary requests to a minimum.\[2\] TiKV's balance and contained scores are the same idea. In every design, the guard conditions matter as much as the estimator: no split if one key dominates (≥25% of samples in CockroachDB), if access is monotonic, or if many requests would straddle the boundary. Splits must also respect logical atomicity: SQL rows and column families (CockroachDB), HBase rows and key prefixes, Spanner interleaved parents, MongoDB identical shard-key values (jumbo chunks), and Cosmos DB logical partitions.

**3. Durable vs ephemeral stats.** Persist only what can be maintained incrementally and must survive restarts: sizes and key counts (SST/HFile properties, MVCCStats) and the shard map (meta tables, system keyspaces, topo). Keep load in memory, because it is high-frequency, cheap to rebuild and only meaningful recently. The cost is that load history disappears on leaseholder or leader change. That is acceptable because load splits need *current* evidence anyway, but it means merge decisions must fetch remote load (CockroachDB's RangeStats RPC) or rely on a central collector (PD heartbeats).\[21\]

**4. Centralised vs decentralised.**
- *Decentralised* (CockroachDB leaseholder queues, HBase RegionServer policies, Bigtable tablet-server-initiated splits) reacts fastest and scales without a coordinator bottleneck. Its weakness is that cross-range decisions such as merges and replica alignment need extra coordination.
- *Centralised* (PD, yb-master, FDB data distributor, Scylla's balancer, MongoDB's balancer, Spanner's placement driver) has the global view needed for placement, hot-spot scheduling and merges, at the cost of a scaling and availability bottleneck and reliance on reported, slightly stale stats.
- The dominant hybrid is: *data owner proposes or executes splits; coordinator owns placement and merges* (Bigtable, TiKV, HBase 2.x).

**5. Avoiding split/merge thrash.** The patterns are:
- *Hysteresis bands:* merge thresholds well below split thresholds, such as CockroachDB's 128 MiB min vs 512 MiB max, TiKV's 54 MiB merge vs 256 MiB-class split, and ScyllaDB's 0.5×/2× around target.
- *Merge requires all conditions; split requires any one:* size *and* load (CockroachDB).
- *Cooldowns:* TiKV's `split-merge-interval` of 1h, which also applies after PD restart, and CockroachDB's load-split merge delay.
- *Sticky or explicit operator intent:* CockroachDB sticky bits with expiration, FDB `rangeconfig`.
- *Combined-size checks:* merge only if the result stays under the maximum.
- *Not merging at all:* DynamoDB and Cosmos DB, which avoid thrash at the cost of permanent partition inflation. On DynamoDB this dilutes per-partition provisioned throughput.

**6. Hotspots on sequential keys.** Splitting cannot spread an append-only tail, because every new split just creates a new hot right-hand range. Systems respond by *declining* to split: CockroachDB's "clear direction" diagnostic, DynamoDB skipping split for heat on increasing sort keys, and Spanner's documented anti-patterns. The fix is at the schema level: hash-sharded indexes or hash prefixes, bucket prefixes ({shard} before versionstamps in FDB), UUIDv4 keys, and avoiding low-cardinality GSI or partition keys. Hash-partitioned systems (Vitess hash vindexes, Cassandra, Cosmos DB, DynamoDB, YugabyteDB's default) avoid sequential hotspots by design but give up range-scan locality. Range-partitioned systems keep locality and depend on load splitting and schema discipline.

## Recommendations

- **Choosing a system for skewed or hot workloads:** prefer one with CPU-based load splitting plus size splitting and automatic merges (CockroachDB, TiDB, Spanner, Cloud Bigtable). Avoid relying on automatic splitting in Vitess, MongoDB 6.0.3+, Cassandra or Citus. Plan resharding as an operational project there.
- **Monotonic keys are a design bug in every system.** Hash- or bucket-prefix them before load testing. Load splitting will not save you.
- **Tune thresholds conservatively.** Raise size limits (TiKV 256 MiB, CockroachDB 512 MiB) rather than lowering them, to cut metadata and heartbeat overhead. Lower load thresholds only after confirming hot ranges with `kv.loadsplitter.*` metrics, TiKV hot-Region stats or `SPANNER_SYS.SPLIT_STATS_TOP_MINUTE`.
- **Pre-split before bulk loads** in range-sharded systems: CockroachDB `SPLIT AT`, TiDB `SPLIT TABLE`, HBase pre-split, MongoDB `moveRange` on empty collections. In Spanner, ramp up gradually instead of benchmarking freshly created tables. In CockroachDB, use split expirations so the merge queue can clean up later.
- **In DynamoDB and Cosmos DB, avoid temporary over-provisioning spikes.** Partitions created by a short-lived capacity or size burst are not merged back.

## Caveats

- Spanner's and Bigtable's size thresholds and internal split-key algorithms are not officially published. The 4 GB and 8 GB Spanner figures come from conflicting third-party blogs, and "Bigtable splits at the SSTable index midpoint" is inference, not a statement from the 2006 paper.
- Some CockroachDB constants (sample size 20, minimum counter 100) come from a secondary code walkthrough rather than the current source. The `kv.range_split.by_load_merge_delay` default could not be confirmed. The CPU threshold default changed from 250 ms to 500 ms between versions.
- Defaults change between releases: YugabyteDB's low-phase threshold (128 MiB vs 512 MiB), TiKV/PD Region and merge sizes (changed in v8.4.0), and MongoDB chunk size (64 → 128 MB). Always check your version's docs.
- DynamoDB and Cosmos DB split behaviour beyond the published limits is officially an implementation detail. The Citus section relies on general product knowledge rather than primary sources gathered here.

## Sources

1. <https://research.google.com/archive/bigtable-osdi06.pdf>
2. [storage: automatically split ranges based on load](https://github.com/cockroachdb/cockroach/pull/31413)
3. [storage: increase the load-based splitting QPS threshold from 250 to 2500 by ajwerner · Pull Request #39687 · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/pull/39687)
4. [storage: Load Based Splitting Experiments · Issue #31819 · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/issues/31819)
5. [Load-Based Splitting - CockroachDB](https://www.cockroachlabs.com/docs/stable/load-based-splitting)
6. [A (not so short) summary of the Google Bigtable paper](https://geekprompt.github.io/Paper_summary_big_table/)
7. [Bigtable overview](https://cloud.google.com/bigtable/docs/overview)
8. [CS 261 Notes on Bigtable](https://www.read.seas.harvard.edu/~kohler/class/cs261-f11/bigtable.html)
9. <https://research.google.com/pubs/archive/46103.pdf>
10. <https://research.google.com/archive/spanner-osdi2012.pdf>
11. [Schemas overview](https://docs.cloud.google.com/spanner/docs/schema-and-data-model)
12. [Bulk loading best practices](https://docs.cloud.google.com/spanner/docs/bulk-loading)
13. [Split statistics](https://docs.cloud.google.com/spanner/docs/introspection/hot-split-statistics)
14. [Cloud Spanner — Demystifying load-based Splitting](https://medium.com/google-cloud/cloud-spanner-desmitifying-load-based-splitting-af14042abb1c)
15. [Google Cloud Spanner — Critical Concepts](https://medium.com/google-cloud/google-cloud-spanner-critical-concepts-1bd9ec52abe3)
16. [Google Cloud Spanner — Splits, Foreign Keys, Secondary indexes and Interleaved tables](https://medium.com/@dipan.saha/google-cloud-spanner-splits-foreign-keys-secondary-indexes-and-interleaved-tables-d23e29df12c5)
17. [Wrong default value of range\_min\_bytes and range\_max\_bytes · Issue #7367 · cockroachdb/docs](https://github.com/cockroachdb/docs/issues/7367)
18. <https://docs.cockroachlabs.com/docs/v26.3/cluster-settings>
19. [kvserver: benchmark different cpu load based split thresholds · Issue #96869 · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/issues/96869)
20. [sql,storage: splits may split SQL rows · Issue #43094 · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/issues/43094)
21. [cockroach/docs/tech-notes/range-merges.md at master · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/range-merges.md)
22. [Distribution Layer - CockroachDB](https://www.cockroachlabs.com/docs/v26.2/architecture/distribution-layer)
23. [cockroach/docs/design.md at master · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)
24. [kvserver: merge queue busy-loops and starves on ranges whose RHS has a non-expiring sticky bit · Issue #171648 · cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/issues/171648)
25. [A Deep Dive into TiKV](https://pingcap.medium.com/a-deep-dive-into-tikv-b27989993d19)
26. [TiKV Configuration File](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)
27. [Load Base Split](https://docs.pingcap.com/tidb/stable/configure-load-base-split/)
28. [docs/dynamic-config.md at master · pingcap/docs](https://github.com/pingcap/docs/blob/master/dynamic-config.md)
29. [TiKV](https://tikv.org/deep-dive/key-value-engine/rocksdb/)
30. [TiKV](https://tikv.org/docs/3.0/reference/tools/tikv-ctl/)
31. [RocksDB - Deep Dive TiKV](https://tikv.github.io/deep-dive-tikv/key-value-engine/rocksdb.html)
32. [Thoughts of improving region approximate size accuracy and efficiency · Issue #6161 · tikv/tikv](https://github.com/tikv/tikv/issues/6161)
33. [rfcs/text/0082-dynamic-size-region.md at master · tikv/rfcs](https://github.com/tikv/rfcs/blob/master/text/0082-dynamic-size-region.md)
34. [TiKV](https://tikv.org/docs/4.0/tasks/configure/region-merge/)
35. [PD Configuration File](https://docs.pingcap.com/tidb/stable/pd-configuration-file/)
36. [TiKV](https://tikv.org/docs/3.0/tasks/configure/region-merge/)
37. [tidb\_docs/pd-control.md at master · jiong-nba/tidb\_docs](https://github.com/jiong-nba/tidb_docs/blob/master/pd-control.md)
38. [\[HBase\] Region Split Policy - Timetombs - GitHub Pages](https://linianhui.github.io/hbase/split-policy/)
39. [hbase-region split剖析- Demo\_zfs](https://www.jianshu.com/p/35c43e25ea07)
40. [Splits and compaction optimizing techniques tutorials](https://www.vskills.in/certification/tutorial/splits-and-compaction-optimizing-techniques/)
41. [IncreasingToUpperBoundRegionSplitPolicy (Apache HBase 4.0.0-alpha-1-SNAPSHOT API)](https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/regionserver/IncreasingToUpperBoundRegionSplitPolicy.html)
42. [HBase principle](https://www.programmersought.com/article/87271531132/)
43. [\[HBASE-16765\] New SteppingRegionSplitPolicy, avoid too aggressive spread of regions for small tables. - ASF JIRA](https://issues.apache.org/jira/browse/HBASE-16765)
44. [Uses of Class org.apache.hadoop.hbase.regionserver.RegionSplitPolicy (Apache HBase 4.0.0-alpha-1-SNAPSHOT API)](https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/regionserver/class-use/RegionSplitPolicy.html)
45. [\[HBASE-16097\] Flushes and compactions fail on getting split point - ASF JIRA](https://issues.apache.org/jira/browse/HBASE-16097)
46. [The Vitess Docs](https://vitess.io/docs/15.0/user-guides/configuration-advanced/resharding/)
47. [The Vitess Docs](https://vitess.io/docs/19.0/user-guides/configuration-advanced/region-sharding)
48. [The Vitess Docs](https://vitess.io/docs/25.0/reference/programs/vtctldclient/vtctldclient_reshard/vtctldclient_reshard_switchtraffic/)
49. [The Vitess Docs](https://vitess.io/docs/archive/14.0/reference/vreplication/movetables/)
50. [The Vitess Docs](https://vitess.io/docs/25.0/reference/vreplication/internal/cutover/)
51. [Tablet splitting](https://docs.yugabyte.com/stable/architecture/docdb-sharding/tablet-splitting/)
52. [Distributed SQL Sharding: How Many Tablets and at What Size?](https://www.yugabyte.com/blog/distributed-sql-sharding-how-many-tablets-size/)
53. [\[DocDB\] Provide guidance on tablet splitting gflags for customers that will be using automatic\_tablet\_splitting · Issue #13047 · yugabyte/yugabyte-db](https://github.com/yugabyte/yugabyte-db/issues/13047)
54. [DEV Community](https://dev.to/yugabyte/autonomous-sharding-in-yugabytedb-with-automatic-tablet-splitting-5ajh)
55. [Data Distribution with Tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html)
56. [Efficient Full Table Scans with ScyllaDB Tablets](https://www.scylladb.com/2025/05/13/efficient-full-table-scans-with-scylladb-tablets/)
57. [ScyllaDB Tablets: Answering Your Top Questions](https://www.scylladb.com/2025/06/24/scylladb-tablets-answering-your-top-questions/)
58. [Smooth Scaling: Why ScyllaDB Moved to “Tablets” Data Distribution - ScyllaDB](https://www.scylladb.com/2024/06/13/why-tablets/)
59. [ScyllaDB](https://www.scylladb.com/product/technology/elasticity/)
60. [Tablets split never concludes in the presence of heavy write workload · Issue #20054 · scylladb/scylladb](https://github.com/scylladb/scylladb/issues/20054)
61. [How to Balance Chunks in a MongoDB Sharded Cluster](https://oneuptime.com/blog/post/2026-03-31-mongodb-balance-chunks-sharding/view)
62. [Dealing With Chunks That "Lost Weight" in MongoDB](https://www.percona.com/blog/dealing-with-chunks-that-lost-weight-in-mongodb/)
63. [Database Manual v8.0 - MongoDB Docs](https://www.mongodb.com/docs/v8.0/reference/method/sh.disableautosplit/)
64. [Sharded Cluster Balancer - Database Manual - MongoDB Docs](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/)
65. [Create Ranges in a Sharded Cluster - Database Manual - MongoDB Docs](https://www.mongodb.com/docs/manual/tutorial/create-chunks-in-sharded-cluster/)
66. [foundationdb/design/data-distributor-internals.md at main · apple/foundationdb](https://github.com/apple/foundationdb/blob/main/design/data-distributor-internals.md)
67. [Data Distribution and Movement](https://github.com/apple/foundationdb/wiki/Data-Distribution-and-Movement)
68. [Keyspace partitions & performance - Using FoundationDB - FoundationDB](https://forums.foundationdb.org/t/keyspace-partitions-performance/168)
69. [Rebalancing not happening on read hot shards in 7.4.43? - Using FoundationDB - FoundationDB](https://forums.foundationdb.org/t/rebalancing-not-happening-on-read-hot-shards-in-7-4-43/4821)
70. [Can I force key prefixes into different shards? - Using FoundationDB - FoundationDB](https://forums.foundationdb.org/t/can-i-force-key-prefixes-into-different-shards/4440)
71. [AWS Database Blog](https://aws.amazon.com/blogs/database/part-3-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/)
72. [DynamoDB burst and adaptive capacity - Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/burst-adaptive-capacity.html)
73. [Partitioning and horizontal scaling in Azure Cosmos DB](https://learn.microsoft.com/azure/cosmos-db/partitioning-overview)
74. [hierarchical partition keys faq](https://learn.microsoft.com/ka-ge/azure/cosmos-db/hierarchical-partition-keys-faq)
75. [Deep Dive into Azure Cosmos DB Physical Partitions - DEV Community](https://dev.to/polymind/deep-dive-into-azure-cosmos-db-physical-partitions-49bb)
76. [Frequently Asked Questions on Distributing Throughput Across Partitions in (Preview) - Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/distribute-throughput-across-partitions-faq)
77. [Redistribute Throughput Across Partitions (Preview) - Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-redistribute-throughput-across-partitions)
78. [hierarchical partition keys](https://learn.microsoft.com/azure/cosmos-db/hierarchical-partition-keys)
79. [CosmosDB Partition Internals: Logical vs Physical Partitions Explained](https://www.abstractalgorithms.dev/cosmosdb-logical-vs-physical-partitions)
