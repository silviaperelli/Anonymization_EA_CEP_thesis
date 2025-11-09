## Stream Anonymization using Genetic Algorithm

This project uses a multi-objective evolutionary algorithm (implemented with JGEA) to discover "data modifiers". A data modifier is an anonymization query that alters an original data stream (`S`) to produce a modified, privacy-preserving stream (`S'`). The quality of each data modifier is determined by running a fixed analysis query (Q) on the modified stream (S') and evaluating its impact on the final results and the overall performance profile.

### Project Structure

*   **/src/main/java/jgea/problem**:
    *   `StreamAnonymizationProblem.java`: The core class that defines the multi-objective problem. It sets up the objectives and contains the `qualityFunction` responsible for evaluating the fitness of each candidate solution.

*   **/src/main/java/jgea/query**:
    *   `MainQuery.java`: Defines the fixed Liebre analysis query (`Q`) that is used to evaluate the quality of an anonymized stream.

*   **/src/main/java/jgea/grammar**:
    *    `GrammarGenerator.java`: A class that generates the grammar file (.bnf). This grammar defines the set of rules used by the evolutionary algorithm to create valid Tree<String> genotypes, which represent the candidate solutions.

*   **/src/main/java/jgea/metrics**:
    *   `MetricsConsumer.java`: A custom collector that gather performance metrics (tuple counts) during a query execution.
    *   `F1Score.java` / `EuclideanDistance.java` / `ModificationPrivacy.java` / `SuppressionPrivacy.java` / `DuplicationPrivacy.java`: Classes that implement the distance functions used to calculate the fitness scores for the objectives.

*   **/src/main/java/jgea/mappers**:
    *   `Mapper.java`: The primary mapper class responsible for the actual transformation. It's invoked by the evolutionary algorithm to translate a genotype into a phenotype.
    *   `RepresentationToLiebreQuery.java`: A component used by `Mapper.java` to translate the genotype (a Tree<String> derived from the grammar) into the phenotype (a query representation).

*   **/src/main/java/jgea/builders**:
    *   This package contains the builders classes that make custom components of this project (like problems and mappers) available to the JGEA experimenter framework. They use jnb annotations (@Discoverable, @Param) to expose Java methods to the experiment definition file (experiment.txt).
    *   `ProblemBuilder.java`: Makes the `StreamAnonymizationProblem` accessible from the experiment file using the `silvia.problem.anonymizationProblem(...)` builder.
    *   `MapperBuilder.java`: Makes the custom `Mapper` class accessible from the experiment file using the `silvia.mapper.treeToQueryMapper()` builder.

### How to Run an Experiment

This project uses the `jgea.experimenter` module, which allows defining and executing experiments from a text file (e.g., experiment.txt).
The process consists of three main steps:

#### 1. Define the Experiment

The entire experiment — including the algorithm, problem, parameters, and listeners — is configured in a dedicated experiment definition file (e.g., experiment.txt).

#### 2. Build the Project

Compile the project, from the project’s root directory, run:

```
mvn clean install
```

This command creates the executable JAR file with all dependencies in the target/ directory:

```
Anonymization_EA_CEP_thesis-1.0-SNAPSHOT-jar-with-dependencies.jar
```

#### 3. Run the Experiment from the Command Line

Launch the experiment using the JAR file created in the previous step:

```
java -jar target/Anonymization_EA_CEP_thesis-1.0-SNAPSHOT-jar-with-dependencies.jar -v -nt 10 -f experiment.txt
```
##### Key Command-Line Arguments:

* `-f experiment.txt` — (Required) Specifies the path to the experiment definition file.

* `-nt 10` — (Concurrency) Sets the number of threads for parallel fitness evaluations (e.g., 10).

* `-v` — (Verbose) Enables detailed output and progress information in the console.