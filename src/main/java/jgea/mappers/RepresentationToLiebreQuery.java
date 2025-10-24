package jgea.mappers;

import common.util.Util;
import event.AirQualityEvent;
import query.Query;
import component.operator.Operator;
import component.source.Source;
import component.sink.Sink;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RepresentationToLiebreQuery {

    // Translate an intermediate QueryRepresentation into an executable Liebre Query and execute it
    public List<AirQualityEvent> processAnonymizationQuery(QueryRepresentation representation, String inputFile) throws IOException {

        final List<AirQualityEvent> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        inputFile = RepresentationToLiebreQuery.class.getClassLoader().getResource(inputFile).getPath();
        Query query = new Query();

        // Define Source and CSV Reader (fixed part of the pipeline)
        Source<String> source = query.addTextFileSource("input-source", inputFile);
        Operator<String, AirQualityEvent> reader = query.addMapOperator(
                "csv-reader",
                line -> {
                    if (line.startsWith("ID;Date;Time;CO(GT)")) return null;
                    return AirQualityEvent.eventCreation(line);
                }
        );
        query.connect(source, reader);

        // Build the operator chain by iterating through the representation's nodes
        Operator<?, AirQualityEvent> lastOperatorInChain = reader;
        // Loop through each abstract operator node
        for (int i = 0; i < representation.operators().size(); i++) {
            QueryRepresentation.OperatorNode node = representation.operators().get(i);
            // Create a unique id for the Liebre operator (filter-0)
            String operatorId = node.type().toLowerCase() + "-" + i;

            if ("FILTER".equals(node.type())) {
                QueryRepresentation.Condition condition = node.condition();
                // Create a Filter Operator in Liebre
                Operator<AirQualityEvent, AirQualityEvent> filterOperator = query.addFilterOperator(
                        operatorId,
                        event -> evaluateCondition(event, condition)
                );

                // Connect the output of the previous operator to the new filter
                query.connect(lastOperatorInChain, filterOperator);
                lastOperatorInChain = filterOperator;
            }
        }

        // Define the final Sink
        Sink<AirQualityEvent> sink = query.addBaseSink("output-sink", event -> {
            if (event != null) {
                collectedEvents.add(event);
            }
        });
        query.connect(lastOperatorInChain, sink);
        query.activate();
        Util.sleep(20000);
        query.deActivate();


        /*
        int waitCycles = 0;

        while(sink.isEnabled()) {
            try {
                System.out.printf("[DEBUG MainQuery]    -> Ciclo di attesa %d: sink.isEnabled() è VERO. Attendo 1 secondo...%n", waitCycles + 1);
                Thread.sleep(1000);
                waitCycles++;
            } catch (InterruptedException e) {
                System.err.println("[DEBUG MainQuery] Ciclo di attesa interrotto!");
                e.printStackTrace();
            }
        }

         */


        return collectedEvents;
    }

    // Helper method that evaluates if an event satisfies a given Condition
    private boolean evaluateCondition(AirQualityEvent event, QueryRepresentation.Condition condition) {
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