package jgea.utils;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class TreeUtils {

    // Find first terminal node in a subtree recursively
    public static String findFirstTerminal(Tree<String> tree) {
        if (tree.isLeaf()) {
            return tree.content();
        }
        for (Tree<String> child : tree) {
            String terminal = findFirstTerminal(child);
            if (terminal != null) {
                return terminal;
            }
        }
        return null;
    }

    // Find all nodes that match a condition
    public static List<Tree<String>> findAllNodes(Tree<String> tree, Predicate<Tree<String>> condition) {
        List<Tree<String>> foundNodes = new ArrayList<>();
        if (condition.test(tree)) {
            foundNodes.add(tree);
        }
        for (Tree<String> child : tree) {
            foundNodes.addAll(findAllNodes(child, condition));
        }
        return foundNodes;
    }
}
