# Mixed hash key range partitions

What if we took the hash-then-range technique from DynamoDB/CockroachDB/Vitess for the range partitions instead of our current hash key promotion flows.

Hash partitions are still more efficient for many things:
- leapfrogging to deep partitions with just a LOUDS encoding (few bytes) without having a Patricia tree which depends on keys values (could be many KBs).
- learning deep partitions is just a "hashDepth" number propagated from responses, whereas learning range partitions needs the full start/end range boundaries.
- more uniform splitting without much state needed.

Range partitions pros:
- allow more fine-grained splitting logic to calculate exact boundaries.
- faster data transferring or deletion of "out of range" rows after a split since ranges are continuous.

## Proposal

- N layers of hash partitions, always at least one layer, the root hash partitions so that we can route without any topology caching in the client. This also provides uniform distribution of the hash keys across partitions.
- After N depth the partitions switch to range partitions with mixed hash keys, instead of the key promotion flow we do now.
- Range partitions split as they do now, based on the hash+sort keys lexicographical order in utf-8 bytes.
- Patricia keys encode the topology of each range partition tree.
- Since the partition DOs have durable storage they can store the patricia trees even if larger and leapfrog splits during routing (not the client though).

Pros:
- No concept of "hash key promotion" to a range partition, although it's similar to a "hash partition converting into a range subtree".
- Easier statistics for split boundary calculations.

Cons:
- Mixed keys across partitions so depending on load it could still cause issues compared to the current key promotion that has the full range partitions for a single hash key.
- Larger encoded topology for the entire FokosDB instance needed in the client. A bloom filter could quickly tell us a hash key is in a range partition tree and we could have separate cached topology for heavy hash keys even, but with mixed key range partitions the bloom filter cannot tell us immediately something.
- Learning that a hash+sort key combination is in a range partition doesn't tell you anything about the other sort keys of that hash key, they could still be in a hash partition or a range partition, so we need to do proper routing from the root or with an encoded topology. Currently we know that the hash key would at least have its own root range partition.

## When is it better?

TODO
