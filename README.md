## Stream Anonymization using Genetic Algorithm

This project uses a multi-objective evolutionary algorithm (implemented with JGEA) to discover "data modifiers". A data modifier is an anonymization query that alters an original data stream (`S`) to produce a modified, privacy-preserving stream (`S'`). The quality of each data modifier is determined by running a fixed analysis query (Q) on the modified stream (S') and evaluating its impact on the final results and the overall performance profile.

### Project Structure

*   **/src/main/java/jgea/problem**:
    *   `StreamAnonymizationProblem.java`: The core class that defines the multi-objective problem. It sets up the objectives and contains the `qualityFunction` responsible for evaluating the fitness of each candidate solution.

*   **/src/main/java/jgea/solver**:
    *   `NewExperimentEA.java`: The main executable class. This is the entry point to configure and launch a new evolutionary experiment.

*   **/src/main/java/jgea/query**:
    *   `MainQuery.java`: Defines the fixed Liebre analysis query (`Q`) that is used to evaluate the quality of an anonymized stream.

*   **/src/main/java/jgea/metrics**:
    *   `MetricsConsumer.java`: A custom collector that gather performance metrics (tuple counts) during a query execution.
    *   `F1Score.java` / `EuclideanDistance.java`: Classes that implement the distance functions used to calculate the fitness scores for the objectives.

*   **/src/main/java/jgea/mappers**:
    *   `RepresentationToLiebreQuery.java`: A component that translates the genotype (a Tree<String> derived from the grammar) into a phenotype (a representation of a query).

### How to Run an Experiment

The main entry point is the `jgea.solver.NewExperimentEA` class.

### Single-Thread vs. Multi-Thread Execution

You can switch between sequential (single-thread) and parallel (multi-thread) fitness evaluations by editing the `jgea.solver.NewExperimentEA.java` file.

To configure the execution mode, find these lines in the `main` method of `NewExperimentEA.java`:

```java
// --- To run in MULTI-THREAD mode, uncomment these lines ---
// int nThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
// System.out.println("Starting evolution with " + nThreads + " threads.");
// ExecutorService executor = Executors.newFixedThreadPool(nThreads);

// --- To run in SINGLE-THREAD mode, use this line ---
ExecutorService executor = Executors.newSingleThreadExecutor();

