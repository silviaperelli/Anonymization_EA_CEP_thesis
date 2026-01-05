## Evolutionary Query Anonymization for Privacy-preserving Stream Processing

This project uses a **multi-objective evolutionary algorithm**, implemented with the **JGEA** framework, to automatically discover optimal "data modifiers". A data modifier is a streaming query that transforms an original data stream (`S`) into a modified, privacy-preserving stream (`S'`). The quality of each data modifier is determined by running a fixed Liebre analysis query (Q) on the modified stream (S') and evaluating its impact on the final results and the overall performance profile.

The core idea is to find the best possible trade-offs between three competing objectives:
1. **Privacy**: How well the data are anonymized or the modified data protects against re-identification.
2. **Results Similarity**: The fidelity of the results produced by a fixed analysis query when run on the modified stream versus the original stream.
3. **Performance Similarity**: The fidelity of the performance profile (tuple/key counts over time) of the analysis query.

The evolutionary process explores different anonymization pipelines to find a set of non-dominated solutions representing the best possible compromises between these objectives. The anonymization query can be composed of operators like `filter`, `map_noise`, `map_duplicate`, and `map_aggregate`.

### Project Structure
Here is an overview of the key packages and classes:

*   **/src/main/java/jgea/problem**:
    *   `StreamAnonymizationProblem.java`: The core class that defines the 3-objective problem. It initializes the baseline, sets up the objectives and contains the `qualityFunction` responsible for evaluating the fitness of each candidate solution.
    *   `AnonymizationProblem_2Objectives.java`: A simplified version of the problem for experiments focused only on 2 objectives, Privacy and Results Similarity.

*   **/src/main/java/jgea/query**:
    *   `MainQueryKeys.java`: Implements the fixed Liebre analysis query (`Q`) that is used to evaluate the quality of an anonymized stream. It captures performance statistics (tuple and key counts per time bucket) into a `StreamStatsWindow` object.
    *   `LiebreAnonymizationQuery.java`: Translates an evolved phenotype (`QueryRepresentation`) into an executable Liebre query.

*   **/src/main/java/jgea/grammar**:
    *    `GrammarGenerator*.java`: A set of classes to automatically generate the grammar file (`.bnf`). The different classes define grammars with different sets of operators (e.g., only filters, filters + maps, filters + map + aggregate).    

*   **/src/main/java/jgea/metrics**:
    *   This package and its sub-packages (`privacy`, `performance`, `results`) contain all the implementations of the metrics used for fitness evaluation.

*   **/src/main/java/jgea/mappers**:
    *   `QueryRepresentation.java`: Defines the phenotype, a structured representation of the query pipeline.
    *   `Mapper.java`: The primary mapper class responsible for the actual transformation. It's invoked by the evolutionary algorithm to translate a genotype into a phenotype.
    *   `TreeToRepresentation.java`: A component used by `Mapper.java` to translate the genotype (a Tree<String> derived from the grammar) into the phenotype (a query representation).

*   **/src/main/java/jgea/builders**:
    *   This package contains the builders classes that make custom components of this project (like problems and mappers) available to the JGEA experimenter framework.
    *   `ProblemBuilder.java`: Makes the `StreamAnonymizationProblem` accessible from the experiment file using the `silvia.problem.anonymizationProblem(...)` or `silvia.problem.anonymizationProblem2O(...)` builder.
    *   `MapperBuilder.java`: Makes the custom `Mapper` class accessible from the experiment file using the `silvia.mapper.treeToQueryMapper()` builder.

### How to Run an Experiment

This project is designed to be configured and run entirely from an experiment file (`.txt`) using the JGEA experimenter.

#### 1. Generate the Grammar

Before running an experiment, you must generate the grammar file that defines the search space. Run the `main` method of the appropriate class to generate a grammar file for the example dataset **AirQuality**:

*   `GrammarGeneratorOnlyFilters.java`: For experiments with filter operators only. 
*   `GrammarGeneratorMap.java`: For experiments with filter and map operators.
*   `GrammarGeneratorAggregate.java`: For experiments with filter, map, and aggregate operators.

This will create/update the corresponding `.bnf` file in `src/main/resources`, where three grammar files for the example dataset are already provided.

#### 2. Configure the Experiment File

The experiment file (e.g., experiment.txt) allows to configure the entire experiment. Here you define which problem, grammar, and metrics to use.

For the example dataset **AirQuality**, the file `experiment.txt` is provided to run the problem with three objectives, while `experiment_2objectives.txt` is used to run it with two objectives.

In these files the `representation` block must point to the correct grammar file and the `problem` block must point to the correct input file and grammar file and must select the privacy metric to use.

**Example 1: 3-Objective Experiment `experiment.txt`**
```
...
  representation = ea.r.cfgTree(
    grammar = ea.grammar.fromFile(path = "src/main/resources/generated-grammar-aggregate.bnf")
  );
...
problem = silvia.problem.anonymizationProblem(
  inputCsvPath = "datasets/airQuality_withSensorID.csv";
  grammarPath = "src/main/resources/generated-grammar-aggregate.bnf";
  privacyMetric = K_ANONYMITY_CARDINALITY
)
```

**Example 2: 2-Objective Experiment `experiment_2objectives.txt`**

**Note**: In the 2-objective version, it is not necessary to specify the grammar path in the problem definition.
```
...
  representation = ea.r.cfgTree(
    grammar = ea.grammar.fromFile(path = "src/main/resources/generated-grammar-filters.bnf")
  );
...
problem = silvia.problem.anonymizationProblem2O(
  inputCsvPath = "datasets/airQuality_withSensorID.csv";
  privacyMetric = SUPPRESSION_ONLY
)
```

For **privacy evaluation**, the following metrics are available and can be selected in the experiment configuration `.txt` file as follows:
* `K_ANONYMITY`: A k-anonymity metric based on the average standard deviation of the distances between quasi-identifiers.
* `K_ANONYMITY_CARDINALITY`: An advanced metric that combines k-anonymity with a cardinality penalty.
* `WEIGHTED_AVERAGE`: A weighted average of suppression, duplication, and modification.
* `SUPPRESSION_ONLY`: A simple metric measuring only the fraction of suppressed tuples.

**Important Note**: 
- When running a **filters-only** experiment, it is recommended to set `privacyMetric = SUPPRESSION_ONLY` for consistency. While `K_ANONYMITY` and `K_ANONYMITY_CARDINALITY` may still be selected, `WEIGHTED_AVERAGE` is not meaningful in this context.
- The **Performance Similarity** metric is based on event time, not wall-clock time. The `minTimestamp`, `maxTimestamp` and `resolution` are dataset-dependent and are currently hardcoded in `MainQueryKeys.java`. If you change the dataset, you must update these values to match the new time range.

#### 3. Build the Project

Compile the project and package it into an executable JAR with all dependencies. Run from the project’s root directory:

```
mvn clean install
```

**Java version**
**Java 21** is required to build and run the project due to compatibility constraints of the JGEA framework, which requires JDK 21.

#### 4. Run the Experiment

Execute the JAR from your terminal, pointing to your experiment file:

```
java -jar target/Anonymization_EA_CEP_thesis-1.0-SNAPSHOT-jar-with-dependencies.jar -v -nt 10 -f experiment.txt
```
##### Key Command-Line Arguments:

* `-f experiment.txt` — (Required) Specifies the path to the experiment definition file.

* `-nt 10` — (Concurrency) Sets the number of threads for parallel fitness evaluations (e.g., 10).

* `-v` — (Verbose) Enables detailed output and progress information in the console.