package jgea.representation;

import event.AirQualityEvent;
import query.Query;
import component.operator.Operator;
import component.source.Source;
import component.sink.Sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

// Translate an abstract PipelineRepresentation into an executable Liebre Query
public class RepresentationToLiebreQuery {

    public Query translate(PipelineRepresentation representation, String inputFile, String outputFile) throws IOException {


        inputFile = RepresentationToLiebreQuery.class.getClassLoader().getResource(inputFile).getPath();

        Files.createDirectories(Paths.get(outputFile).getParent());

        Query query = new Query();

        // Define Source and CSV Reader (fixed part of the pipeline)
        Source<String> source = query.addTextFileSource("input-source", inputFile);
        Operator<String, AirQualityEvent> reader = query.addMapOperator(
                "csv-reader",
                line -> {
                    if (line.startsWith("Date;Time;CO(GT)")) return null;
                    return AirQualityEvent.eventCreation(line);
                }
        );
        query.connect(source, reader);

        // Build the operator chain by iterating through the representation's nodes
        Operator<?, AirQualityEvent> lastOperatorInChain = reader;
        // Loop through each abstract operator node
        for (int i = 0; i < representation.operators().size(); i++) {
            PipelineRepresentation.OperatorNode node = representation.operators().get(i);
            // Create a unique id for the Liebre operator (filter-0)
            String operatorId = node.type().toLowerCase() + "-" + i;

            if ("FILTER".equals(node.type())) {
                PipelineRepresentation.Condition condition = node.condition();
                // Create a Filter Operator in Liebre
                Operator<AirQualityEvent, AirQualityEvent> filterOperator = query.addFilterOperator(
                        operatorId,
                        event -> {
                            boolean keep = evaluateCondition(event, condition);
                            // Just a print for debug
                            if (event != null) {
                                double eventValueForDebug;
                                try {
                                    switch (condition.variable()) {
                                        case "CO(GT)": eventValueForDebug = event.getCoLevel(); break;
                                        case "PT08.S1(CO)": eventValueForDebug = event.getPt08s1(); break;
                                        case "NMHC(GT)": eventValueForDebug = event.getNmhc(); break;
                                        case "C6H6(GT)": eventValueForDebug = event.getC6h6(); break;
                                        case "PT08.S2(NMHC)": eventValueForDebug = event.getPt08s2(); break;
                                        case "NOx(GT)": eventValueForDebug = event.getNox(); break;
                                        case "PT08.S3(NOx)": eventValueForDebug = event.getPt08s3(); break;
                                        case "NO2(GT)": eventValueForDebug = event.getNo2(); break;
                                        case "PT08.S4(NO2)": eventValueForDebug = event.getPt08s4(); break;
                                        case "PT08.S5(O3)": eventValueForDebug = event.getPt08s5(); break;
                                        case "T": eventValueForDebug = event.getT(); break;
                                        case "RH": eventValueForDebug = event.getRh(); break;
                                        case "AH": eventValueForDebug = event.getAh(); break;
                                        default: eventValueForDebug = Double.NaN;
                                    }
                                } catch (Exception e) {
                                    eventValueForDebug = Double.NaN;
                                }
                                System.out.printf(
                                        "[DEBUG-FILTER %-9s] Condition: %-25s | Event Value for '%s': %8.2f | Decision: %s%n",
                                        operatorId,                 // es. "filter-0"
                                        condition.toString(),       // es. "T > 20.0000"
                                        condition.variable(),       // es. "T"
                                        eventValueForDebug,         // es. 13.60
                                        keep ? "KEEP" : "DROP"      // es. "DROP"
                                );
                            } else {
                                System.out.printf("[DEBUG-FILTER %s] Received a null event. Decision: DROP%n", operatorId);
                            }

                            return keep;
                        }
                );

                // Connect the output of the previous operator to the new filter
                query.connect(lastOperatorInChain, filterOperator);
                lastOperatorInChain = filterOperator;
            }
            // In the future we can add: else if ("AGGREGATE".equals(node.type())) { ... }
        }

        // Define the final Sink
        Sink<AirQualityEvent> sink = query.addTextFileSink("output-sink", outputFile, true);
        query.connect(lastOperatorInChain, sink);

        return query;
    }


    // Helper method that evaluates if an event satisfies a given Condition
    private boolean evaluateCondition(AirQualityEvent event, PipelineRepresentation.Condition condition) {
        if (event == null) return false;

        double eventValue;
        try {
            // Switch that maps the string from the Condition to the actual event getter
            switch (condition.variable()) {
                case "CO(GT)": eventValue = event.getCoLevel(); break;
                case "PT08.S1(CO)": eventValue = event.getPt08s1(); break;
                case "NMHC(GT)": eventValue = event.getNmhc(); break;
                case "C6H6(GT)": eventValue = event.getC6h6(); break;
                case "PT08.S2(NMHC)": eventValue = event.getPt08s2(); break;
                case "NOx(GT)": eventValue = event.getNox(); break;
                case "PT08.S3(NOx)": eventValue = event.getPt08s3(); break;
                case "NO2(GT)": eventValue = event.getNo2(); break;
                case "PT08.S4(NO2)": eventValue = event.getPt08s4(); break;
                case "PT08.S5(O3)": eventValue = event.getPt08s5(); break;
                case "T": eventValue = event.getT(); break;
                case "RH": eventValue = event.getRh(); break;
                case "AH": eventValue = event.getAh(); break;
                default: return false;
            }
            if(Double.isNaN(eventValue)) return false;

        } catch (Exception e) {
            return false;
        }

        double conditionValue = (Double) condition.value();

        return switch (condition.operator()) {
            case LESS_THAN -> eventValue < conditionValue;
            case GREATER_THAN -> eventValue > conditionValue;
            case LESS_OR_EQUAL -> eventValue <= conditionValue;
            case GREATER_OR_EQUAL -> eventValue >= conditionValue;
            case EQUAL -> eventValue == conditionValue;
        };
    }

}