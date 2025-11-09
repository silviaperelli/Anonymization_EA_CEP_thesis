package jgea.builders;

import io.github.ericmedvet.jnb.core.Cacheable;
import io.github.ericmedvet.jnb.core.Discoverable;
import io.github.ericmedvet.jnb.core.Param;
import jgea.problem.StreamAnonymizationProblem;

@Discoverable(prefixTemplate = "silvia.problem")
public class ProblemBuilder {

    private ProblemBuilder() {
    }

    // Create a problem
    @SuppressWarnings("unused")
    @Cacheable
    public static StreamAnonymizationProblem anonymizationProblem(
            @Param("inputCsvPath") String inputCsvPath,
            @Param(value = "name", iS = "{inputCsvPath}") String name
    ) throws Exception {
        return new StreamAnonymizationProblem(inputCsvPath);
    }
}
