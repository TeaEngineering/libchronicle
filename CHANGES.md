CHANGES
====

To track changes in the protocol, or defaults, rather than differences in any particular implementation

Chronicle Queue v5
----

* FAST_XXX RollSchemes see https://github.com/OpenHFT/Chronicle-Queue/blob/ea/src/main/java/net/openhft/chronicle/queue/RollCycles.java

* Default RollScheme (e.g. for SingleChronicleQueueBuilder) changes from `DAILY` to `FAST_DAILY`
* Since schemes make reduced bits available for cycle value, new concept of "epoch" which appears to be a constant offset to the cycle value, ie. we now longer assume epoch is midnight 1970.01.01
* New per-queue `metadata.cq4t` file replaces `directory-listing.cq4t` file.
  * STStore -> SCQMeta -> SCQSRoll fields need to be parsed since they are removed from queue files:
  * length (uint32)
  * epoch (uint8)
  * format (text)
* queue metatdata wire encoder changes:
  * `SCQSIndexing.indexSpacing` changes from `uint8` to `uint16`
  *
* SingleChronicleQueueBuilder writeText() seems to be writing a one-byte prefix to the data


Chronicle Queue v4
----
* Everything
