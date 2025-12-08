package utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * StreamStatsWindow keeps lightweight statistics for a fixed set of streams
 * over a fixed timestamp window [minTimestamp, maxTimestamp], inclusive.
 *
 * For each stream and each timestamp in the window, two integer counters exist:
 *  - tupleCounts: number of tuples observed
 *  - keyCounts:   number of keys observed
 *
 * CONCURRENCY ASSUMPTION:
 *  ---------------------------------------
 *  The increment methods are thread-safe *only under the assumption* that
 *  different threads NEVER update the same (stream, timestamp) cell.
 *  Under this assumption, no synchronization or atomics are required, and
 *  performance is maximized.
 *
 *  If this assumption is violated, updates may be lost.
 *  ---------------------------------------
 *
 * Typical use case: each thread processes a disjoint subset of timestamps.
 */
public final class StreamStatsWindow {

    private final Set<String> streamNames;
    private final int minTimestamp;
    private final int maxTimestamp;
    private final int size; // number of timestamps represented

    // For each stream, we keep an array of ints for counts and keys.
    private final Map<String, int[]> tupleCounts;
    private final Map<String, int[]> keyCounts;

    /**
     * Create a new stats window.
     *
     * @param streamNames     Set of stream names to track.
     * @param minTimestamp    Inclusive minimum timestamp.
     * @param maxTimestamp    Inclusive maximum timestamp.
     */
    public StreamStatsWindow(Set<String> streamNames, int minTimestamp, int maxTimestamp) {
        if (streamNames == null || streamNames.isEmpty()) {
            throw new IllegalArgumentException("streamNames cannot be null or empty");
        }
        if (minTimestamp > maxTimestamp) {
            throw new IllegalArgumentException("minTimestamp > maxTimestamp");
        }

        this.streamNames = Collections.unmodifiableSet(Set.copyOf(streamNames));
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.size = maxTimestamp - minTimestamp + 1;

        this.tupleCounts = new HashMap<>();
        this.keyCounts = new HashMap<>();

        for (String s : this.streamNames) {
            tupleCounts.put(s, new int[size]);
            keyCounts.put(s, new int[size]);
        }
    }

    /** Convert timestamp to array index. */
    private int idx(int timestamp) {
        if (timestamp < minTimestamp || timestamp > maxTimestamp) {
            throw new IllegalArgumentException(
                "Timestamp " + timestamp + " outside of [" + minTimestamp + "," + maxTimestamp + "]");
        }
        return timestamp - minTimestamp;
    }

    /** Validate stream name. */
    private int[] getArrayFor(Map<String, int[]> map, String streamName) {
        int[] arr = map.get(streamName);
        if (arr == null) {
            throw new IllegalArgumentException("Unknown stream: " + streamName);
        }
        return arr;
    }

    // ------------------------------------------------------------------------
    // UPDATE METHODS (Assuming no two threads touch same (stream, timestamp))
    // ------------------------------------------------------------------------

    /**
     * Increase tuple count for a given stream and timestamp.
     * Safe under the assumption of no concurrent updates to the same cell.
     */
    public void addTuples(String streamName, int timestamp, int amount) {
        if (amount < 0) throw new IllegalArgumentException("amount < 0");
        int[] arr = getArrayFor(tupleCounts, streamName);
        arr[idx(timestamp)] += amount;
    }

    /**
     * Increase key count for a given stream and timestamp.
     * Same concurrency assumption applies.
     */
    public void addKeys(String streamName, int timestamp, int amount) {
        if (amount < 0) throw new IllegalArgumentException("amount < 0");
        int[] arr = getArrayFor(keyCounts, streamName);
        arr[idx(timestamp)] += amount;
    }

    // ------------------------------------------------------------------------
    // DIFFERENCE COMPUTATION
    // ------------------------------------------------------------------------

    /**
     * Compute a per-cell difference between this object and another.
     * The two objects must have identical stream sets and timestamp windows.
     *
     * Result = this - other (element-wise).
     */
    public StreamStatsWindow diff(StreamStatsWindow other) {
        if (!this.streamNames.equals(other.streamNames)) {
            throw new IllegalArgumentException("Stream name sets differ");
        }
        if (this.minTimestamp != other.minTimestamp || this.maxTimestamp != other.maxTimestamp) {
            throw new IllegalArgumentException("Timestamp windows differ");
        }

        StreamStatsWindow result = new StreamStatsWindow(this.streamNames, this.minTimestamp, this.maxTimestamp);

        for (String stream : streamNames) {
            int[] thisTuples = this.tupleCounts.get(stream);
            int[] thisKeys   = this.keyCounts.get(stream);
            int[] otherTuples = other.tupleCounts.get(stream);
            int[] otherKeys   = other.keyCounts.get(stream);

            int[] rTuples = result.tupleCounts.get(stream);
            int[] rKeys   = result.keyCounts.get(stream);

            for (int i = 0; i < size; i++) {
                rTuples[i] = thisTuples[i] - otherTuples[i];
                rKeys[i]   = thisKeys[i]   - otherKeys[i];
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------
    // GETTERS
    // ------------------------------------------------------------------------

    public Set<String> streamNames() {
        return streamNames;
    }

    public int minTimestamp() {
        return minTimestamp;
    }

    public int maxTimestamp() {
        return maxTimestamp;
    }

    public int[] getTupleArray(String stream) {
        return getArrayFor(tupleCounts, stream);
    }

    public int[] getKeyArray(String stream) {
        return getArrayFor(keyCounts, stream);
    }
}
