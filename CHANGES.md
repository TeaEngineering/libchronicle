CHANGES
====

To track changes in the protocol, or defaults, rather than differences in any particular implementation

Chronicle Queue v5
----

* FAST_XXX RollSchemes see https://github.com/OpenHFT/Chronicle-Queue/blob/ea/src/main/java/net/openhft/chronicle/queue/RollCycles.java

* Default RollScheme (e.g. for SingleChronicleQueueBuilder) changes from `DAILY` to `FAST_DAILY`
* New per-queue `metadata.cq4t` file replaces `directory-listing.cq4t` file.


Chronicle Queue v4
----
* Everything
