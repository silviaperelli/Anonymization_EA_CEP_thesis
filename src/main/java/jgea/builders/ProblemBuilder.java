package jgea.builders;

import io.github.ericmedvet.jnb.core.Cacheable;
import io.github.ericmedvet.jnb.core.Discoverable;
import io.github.ericmedvet.jnb.core.Param;
import jgea.problem.AnonymizationProblem_2Objectives;
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
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        boolean isFilterOnly = grammarPath.toLowerCase().contains("filters");
        return new StreamAnonymizationProblem(inputCsvPath, privacyMetric, isFilterOnly);
    }

    // Create a problem with 2 objectives
    @SuppressWarnings("unused")
    @Cacheable
    public static AnonymizationProblem_2Objectives anonymizationProblem2O(
            @Param("inputCsvPath") String inputCsvPath,
            @Param(value = "privacyMetric", dS = "K_ANONYMITY_CARDINALITY") PrivacyMetricChoice privacyMetric,
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        return new AnonymizationProblem_2Objectives(inputCsvPath, privacyMetric);
    }
}
