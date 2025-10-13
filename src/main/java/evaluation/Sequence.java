package evaluation;

import java.util.List;
import java.util.Set;

// Data structure to represent a single sequence in the target dataset
public record Sequence(Set<Long> tupleIds) {

    public Sequence(List<Long> ids) {
        this(Set.copyOf(ids)); // Converte la lista in un set immutabile
    }

    // Metodo per calcolare l'intersezione con un'altra sequenza
    public Set<Long> intersection(Sequence other) {
        Set<Long> intersection = new java.util.HashSet<>(this.tupleIds);
        intersection.retainAll(other.tupleIds());
        return intersection;
    }

    // Metodo per calcolare l'unione con un'altra sequenza
    public Set<Long> union(Sequence other) {
        Set<Long> union = new java.util.HashSet<>(this.tupleIds);
        union.addAll(other.tupleIds());
        return union;
    }

}
