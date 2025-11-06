package jgea.metrics;

import event.AirQualityEvent;
import jgea.mappers.QueryRepresentation;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static jgea.query.utils.OperatorUtils.getAttributeValue;


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

        // Find all unique attributes targeted by map noise operators
        Set<String> noisyAttributes = new HashSet<>();
        representation.operators().stream()
                .filter(op -> op.type() == QueryRepresentation.Operator.MAP_NOISE)
                .forEach(op -> {
                    QueryRepresentation.MapNoiseArgs args =
                            (QueryRepresentation.MapNoiseArgs) op.arguments();
                    noisyAttributes.add(args.attribute());
                });

        if (noisyAttributes.isEmpty()) {
            return 0.0;
        }


        double totalAttributes = (double) comparableTuples.size() * NUM_MODIFIABLE_ATTRIBUTES;
        double modifiedAttributes = (double) comparableTuples.size() * noisyAttributes.size();

        return modifiedAttributes / totalAttributes;
    }

    /*

    //Same method but with a direct comparison between anonymized attributes and original attributes
    public Double apply(List<AirQualityEvent> originalStream,
                        List<AirQualityEvent> modifiedStream,
                        QueryRepresentation representation) {

        if (originalStream.isEmpty() || modifiedStream.isEmpty() || representation == null) {
            return 0.0;
        }

        // Map from tupleId to original event
        Map<Long, AirQualityEvent> originalMap = originalStream.stream()
                .collect(Collectors.toMap(AirQualityEvent::getTupleId, e -> e, (a, b) -> a));

        // Find all unique attributes targeted by map noise operators
        Set<String> noisyAttributes = representation.operators().stream()
                .filter(op -> op.type() == QueryRepresentation.Operator.MAP_NOISE)
                .map(op -> ((QueryRepresentation.MapNoiseArgs) op.arguments()).attribute())
                .collect(Collectors.toSet());

        if (noisyAttributes.isEmpty()) {
            return 0.0;
        }

        // Count actual changes
        double modifiedAttributes = 0.0;
        double totalAttributes = 0.0;

        for (AirQualityEvent mod : modifiedStream) {
            AirQualityEvent orig = originalMap.get(mod.getTupleId());
            if (orig == null) continue; // new tuple in the anonymized datastream not comparable

            // Each tuple has NUM_MODIFIABLE_ATTRIBUTES potentially modifiable
            totalAttributes += NUM_MODIFIABLE_ATTRIBUTES;

            for (String attr : noisyAttributes) {
                double origVal = getAttributeValue(orig, attr);
                double modVal = getAttributeValue(mod, attr);

                if (!Double.isNaN(origVal) && !Double.isNaN(modVal)) {
                    // Consider a change only if the value is different
                    if (Math.abs(origVal - modVal) > 1e-9) {
                        modifiedAttributes++;
                    }
                }
            }
        }

        if (totalAttributes == 0) return 0.0;
        return modifiedAttributes / totalAttributes;
    }

     */

}
