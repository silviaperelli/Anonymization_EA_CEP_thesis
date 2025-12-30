package jgea.metrics.privacy;

import event.AirQualityEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DuplicationPrivacy implements Distance<List<AirQualityEvent>> {

    // Calculate the Duplication privacy score that measures the fraction of tuples in the modified stream that are duplicates
    @Override
    public Double apply(List<AirQualityEvent> originalStream, List<AirQualityEvent> modifiedStream) {

        if (modifiedStream == null || modifiedStream.isEmpty()) {
            return 0.0;
        }

        // Get the total number of tuples, original and duplicated
        double totalTuples = modifiedStream.size();

        // Get the number of unique tuples
        Set<Long> uniqueTupleIds = modifiedStream.stream()
                .map(AirQualityEvent::getTupleId)
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
