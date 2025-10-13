package evaluation;

import java.util.List;
import java.util.Set;

// Data structure to represent a single sequence in the target dataset
public record Sequence(Set<Long> tupleIds) {

    public Sequence(List<Long> ids) {
        this(Set.copyOf(ids)); // Convert the list to an immutable set
    }

    // Method to calculate the intersection with another sequence
    public Set<Long> intersection(Sequence other) {
        Set<Long> intersection = new java.util.HashSet<>(this.tupleIds);
        intersection.retainAll(other.tupleIds());
        return intersection;
    }

    // Method to calculate the union with another sequence
    public Set<Long> union(Sequence other) {
        Set<Long> union = new java.util.HashSet<>(this.tupleIds);
        union.addAll(other.tupleIds());
        return union;
    }
}
