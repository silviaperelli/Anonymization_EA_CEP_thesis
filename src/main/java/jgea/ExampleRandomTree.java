package jgea;

import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrowGrammarTreeFactory;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import jgea.utils.CSVHeaderReader;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class ExampleRandomTree {

    public static void main(String[] args) {
        try {
            // Grammar generation and loading
            final String CsvPath = "datasets/airQuality.csv";
            final String GrammarPath = "generated-grammar.bnf";
            List<String> CsvAttributes = CSVHeaderReader.extractAttributes(CsvPath);
            GrammarGenerator.generateGrammar(CsvAttributes, GrammarPath);

            // Loading grammar from file
            StringGrammar<String> grammar;
            try (InputStream grammarStream = new FileInputStream(GrammarPath)) {
                grammar = StringGrammar.load(grammarStream);
            }

            // Parameters for the tree generation
            final int MAX_TREE_DEPTH = 12;
            int TARGET_DEPTH = 3;

            System.out.println("Loaded Grammar: ");
            System.out.println(grammar);

            // Tree generation with a factory
            GrowGrammarTreeFactory<String> treeFactory = new GrowGrammarTreeFactory<>(MAX_TREE_DEPTH, grammar);
            Tree<String> randomTree = treeFactory.build(new Random(), TARGET_DEPTH);

            if (randomTree != null) {
                System.out.println("\n********** Generated Random Tree (Genotype) **********");
                randomTree.prettyPrint(System.out);

                System.out.println("\n********** Applying Tree-to-Representation mapper... **********");
                // Mapping from a tree to a Pipeline Representation
                Problem problem = new Problem(GrammarPath);
                Function<Tree<String>, PipelineRepresentation> toRepresentationMapper = problem.solutionMapper();
                PipelineRepresentation pipelineRepresentation = toRepresentationMapper.apply(randomTree);

                System.out.println("\n********** Mapped PipelineRepresentation (Phenotype) **********");
                System.out.println(pipelineRepresentation);

                // TO DO: Mapping from a Pipeline Representation to an executable Liebre object

            } else {
                System.out.println("Random Tree generation returned null.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}