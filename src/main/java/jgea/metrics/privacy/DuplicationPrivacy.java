package jgea.metrics.privacy;

import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DuplicationPrivacy implements Distance<List<GenericEvent>> {

    // Calculate the Duplication privacy score that measures the fraction of tuples in the modified stream that are duplicates
    @Override
    public Double apply(List<GenericEvent> originalStream, List<GenericEvent> modifiedStream) {

        if (modifiedStream == null || modifiedStream.isEmpty()) {
            return 0.0;
        }

        // Get the total number of tuples, original and duplicated
        double totalTuples = modifiedStream.size();

        // Get the number of unique tuples
        Set<Long> uniqueTupleIds = modifiedStream.stream()
                .map(event -> event.getAttribute("ID").longValue())
                .collect(Collectors.toSet());
        double uniqueTuples = uniqueTupleIds.size();

        if (totalTuples == 0) {
            return 0.0;
        }

        // Calculate the number of duplicated tuples and return the fraction
        double duplicateCount = totalTuples - uniqueTuples;
        return duplicateCount / totalTuples;
    }
}
