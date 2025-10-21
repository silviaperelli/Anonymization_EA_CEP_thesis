package jgea.newProblem;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import jgea.representation.QueryRepresentation;
import jgea.representation.TreeToRepresentation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Questa classe è un "mapper" di JGEA.
 * Il suo compito è implementare la logica di trasformazione da un genotipo
 * (un albero di derivazione Tree<String> prodotto dalla grammatica)
 * a un fenotipo (una QueryRepresentation, che è la nostra soluzione concreta).
 */
public class TreeToQueryRepresentationMapper implements Function<Tree<String>, QueryRepresentation> {

    // Questo mapper non ha bisogno di uno stato interno (come una grammatica),
    // quindi il suo costruttore può essere vuoto.
    public TreeToQueryRepresentationMapper() {
        // costruttore vuoto
    }

    @Override
    public QueryRepresentation apply(Tree<String> tree) {
        // Questa è la logica di mapping che prima si trovava nel solutionMapper.
        try {
            List<QueryRepresentation.OperatorNode> operators = new ArrayList<>();

            // Usiamo la classe di utilità che sa come navigare l'albero.
            TreeToRepresentation treeParser = new TreeToRepresentation();
            treeParser.parsePipelineNode(tree, operators);

            // Restituiamo la rappresentazione della soluzione concreta.
            return new QueryRepresentation(operators);

        } catch (Exception e) {
            System.err.println("Errore critico durante il mapping da albero a QueryRepresentation: " + e.getMessage());
            e.printStackTrace();

            // In caso di errore di mapping, è più sicuro restituire una soluzione "vuota"
            // (che avrà fitness molto bassa) piuttosto che 'null', per evitare NullPointerExceptions
            // nel motore di JGEA.
            return null;
            //return new QueryRepresentation(List.of());
        }
    }
}
