package cep;

import event.AirQualityEvent;
import event.StreamFactory;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Writer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static cep.PollutionAlertQuery.createHighCoPattern;

public class PollutionAlertMain {


    private static final Logger logger = LoggerFactory.getLogger(PollutionAlertMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Apply CEP query to the original Dataset
        processDataset(env, "datasets/airQuality.csv", "/results/targetDataset.csv");

        // Apply CEP query to the anonymized Dataset
        processDataset(env, "datasets/anonymizedDataset.csv", "/results/targetAnonymizedDataset.csv");

        // Apply CEP query to the anonymized Dataset with noise
        processDataset(env, "datasets/anonymizedDatasetNoise.csv", "/results/targetAnonymizedDatasetNoise.csv");
    }

    private static void processDataset(StreamExecutionEnvironment env, String datasetPath, String filePath) throws Exception {

        DataStream<AirQualityEvent> stream = StreamFactory.createStreamfromFile(env, datasetPath);
        List<List<AirQualityEvent>> sequences = PollutionAlertQuery.processAlerts(stream);

        if (sequences.isEmpty()) {
            logger.info("No pollution sequences detected for dataset: {}", datasetPath);
        } else {
            saveSequencesToFile(sequences, filePath);
        }
    }

    private static void saveSequencesToFile(List<List<AirQualityEvent>> sequences, String outputFilePath) {

        String outputDir = "src/main/resources/datasets";
        String filePath = outputDir + outputFilePath;
        new File(outputDir).mkdirs();

        int sequenceCount = 0;
        try (FileWriter writer = new FileWriter(filePath)) {
            for (List<AirQualityEvent> sequence : sequences) {
                String line = sequence.stream()
                        .map(Writer::writeToCSV)
                        .collect(Collectors.joining("|"));
                writer.write(line + "\n");

                logger.info(PollutionAlertQuery.formatAlertInfo(sequence));
                sequenceCount++;
            }
        } catch (IOException e) {
            logger.error("Error writing output file: {}", outputFilePath, e);
        }
        logger.info("Found and saved {} sequence in the file {}.\n", sequenceCount, outputFilePath);
    }

}
