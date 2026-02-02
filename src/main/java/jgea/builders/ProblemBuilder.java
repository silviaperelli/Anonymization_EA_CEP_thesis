package jgea.builders;

import io.github.ericmedvet.jnb.core.Cacheable;
import io.github.ericmedvet.jnb.core.Discoverable;
import io.github.ericmedvet.jnb.core.Param;
import jgea.problem.StreamAnonymizationProblem_2Objectives;
import jgea.problem.StreamAnonymizationProblem_2ObjectivesPerf;
import jgea.problem.utils.PrivacyMetricChoice;
import jgea.problem.StreamAnonymizationProblem;

@Discoverable(prefixTemplate = "silvia.problem")
public class ProblemBuilder {

    private ProblemBuilder() {
    }

    // Create a problem with 3 objectives
    @SuppressWarnings("unused")
    @Cacheable
    public static StreamAnonymizationProblem anonymizationProblem(
            @Param("inputCsvPath") String inputCsvPath,
            @Param("grammarPath") String grammarPath,
            @Param(value = "privacyMetric", dS = "K_ANONYMITY_CARDINALITY") PrivacyMetricChoice privacyMetric,
            @Param(value = "keyColumn", dS = "") String keyColumn,
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        boolean isFilterOnly = grammarPath.toLowerCase().contains("filters");
        return new StreamAnonymizationProblem(inputCsvPath, keyColumn, privacyMetric, isFilterOnly);
    }

    // Create a problem with 2 objectives: results similarity and privacy
    @SuppressWarnings("unused")
    @Cacheable
    public static StreamAnonymizationProblem_2Objectives anonymizationProblem2O(
            @Param("inputCsvPath") String inputCsvPath,
            @Param(value = "privacyMetric", dS = "K_ANONYMITY_CARDINALITY") PrivacyMetricChoice privacyMetric,
            @Param(value = "keyColumn", dS = "") String keyColumn,
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        return new StreamAnonymizationProblem_2Objectives(inputCsvPath, keyColumn, privacyMetric);
    }

    // Create a problem with 2 objectives: performance similarity and privacy
    @SuppressWarnings("unused")
    @Cacheable
    public static StreamAnonymizationProblem_2ObjectivesPerf anonymizationProblem2OPerf(
            @Param("inputCsvPath") String inputCsvPath,
            @Param("grammarPath") String grammarPath,
            @Param(value = "privacyMetric", dS = "K_ANONYMITY_CARDINALITY") PrivacyMetricChoice privacyMetric,
            @Param(value = "keyColumn", dS = "") String keyColumn,
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        boolean isFilterOnly = grammarPath.toLowerCase().contains("filters");
        return new StreamAnonymizationProblem_2ObjectivesPerf(inputCsvPath, keyColumn, privacyMetric, isFilterOnly);
    }
}
