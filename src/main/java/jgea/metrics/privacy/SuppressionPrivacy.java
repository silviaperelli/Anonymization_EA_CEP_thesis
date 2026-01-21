package jgea.metrics.privacy;

import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SuppressionPrivacy implements Distance<List<GenericEvent>> {

    // Calculate the Suppression privacy score that measures how many original tuple ID have been removed in the modified stream
    // A score of 1.0 represents maximum privacy (no original ID remain), while a score
    // of 0.0 represents minimum privacy (all original ID are still present)
    @Override
    public Double apply(List<GenericEvent> originalStream, List<GenericEvent> modifiedStream) {

        // Extract the tuple ID from the original stream
        Set<Long> originalTuples = originalStream.stream()
                .map(event -> event.getAttribute("ID").longValue())
                .collect(Collectors.toSet());

        if (modifiedStream == null || modifiedStream.isEmpty()) {
            return 1.0; // If the modified stream is empty or null, there is maximum privacy
        }

        // Extract the tuple ID from the modified stream
        Set<Long> modifiedTuples = modifiedStream.stream()
                .map(event -> event.getAttribute("ID").longValue())
                .collect(Collectors.toSet());

        // Calculate the intersection of the two sets
        Set<Long> intersection = new HashSet<>(originalTuples);
        intersection.retainAll(modifiedTuples);

        double retentionRatio = (double) intersection.size() / originalTuples.size();
        return 1.0 - retentionRatio;
    }
}
