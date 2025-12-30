package jgea.metrics.privacy;

import event.AirQualityEvent;
import jgea.mappers.QueryRepresentation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ModificationPrivacy {

    // Number of attributes that can be modified
    private static final int NUM_MODIFIABLE_ATTRIBUTES = 13;

    // Calculate the Modification privacy score that measures the number of attributes modified
    // out of all those that are potentially modifiable
    public Double apply(List<AirQualityEvent> originalStream, List<AirQualityEvent> modifiedStream, QueryRepresentation representation) {

        if (originalStream.isEmpty() || modifiedStream.isEmpty() || representation == null) {
            return 0.0;
        }

        // Identify comparable tuples that are also in the original stream
        Set<Long> originalIds = originalStream.stream()
                .map(AirQualityEvent::getTupleId)
                .collect(Collectors.toSet());

        List<AirQualityEvent> comparableTuples = modifiedStream.stream()
                .filter(e -> originalIds.contains(e.getTupleId()))
                .toList();

        if (comparableTuples.isEmpty()) {
            return 0.0;
        }

        // Find all unique attributes targeted by modification operators
        Set<String> modifiedAttributesSet = new HashSet<>();

        // Add attributes from map noise operators
        representation.operators().stream()
                .filter(op -> op.type() == QueryRepresentation.Operator.MAP_NOISE)
                .forEach(op -> {
                    QueryRepresentation.MapNoiseArgs args =
                            (QueryRepresentation.MapNoiseArgs) op.arguments();
                    modifiedAttributesSet.add(args.attribute());
                });

        // Add attributes from map aggregate operators
        representation.operators().stream()
                .filter(op -> op.type() == QueryRepresentation.Operator.MAP_AGGREGATE)
                .forEach(op -> {
                    QueryRepresentation.MapAggregateArgs args = (QueryRepresentation.MapAggregateArgs) op.arguments();
                    modifiedAttributesSet.add(args.attribute());
                });

        if (modifiedAttributesSet.isEmpty()) {
            return 0.0;
        }


        double totalAttributes = (double) comparableTuples.size() * NUM_MODIFIABLE_ATTRIBUTES;
        double modifiedAttributes = (double) comparableTuples.size() * modifiedAttributesSet.size();

        return modifiedAttributes / totalAttributes;
    }
}
