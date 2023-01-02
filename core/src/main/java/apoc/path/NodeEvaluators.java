package apoc.path;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Static factory methods for obtaining node evaluators
 */
public final class NodeEvaluators {
    // non-instantiable
    private NodeEvaluators() {}

    /**
     * Returns an evaluator which handles end nodes and terminator nodes
     * Returns null if both lists are empty
     */
    public static Evaluator endAndTerminatorNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> endNodes, List<Node> terminatorNodes) {
        Evaluator endNodeEvaluator = null;
        Evaluator terminatorNodeEvaluator = null;

        if (!endNodes.isEmpty()) {
            Node[] nodes = endNodes.toArray(new Node[endNodes.size()]);
            endNodeEvaluator = Evaluators.includeWhereEndNodeIs(nodes);
        }

        if (!terminatorNodes.isEmpty()) {
            Node[] nodes = terminatorNodes.toArray(new Node[terminatorNodes.size()]);
            terminatorNodeEvaluator = Evaluators.pruneWhereEndNodeIs(nodes);
        }

        if (endNodeEvaluator != null || terminatorNodeEvaluator != null) {
            return new EndAndTerminatorNodeEvaluator(filterStartNode, minLevel, endNodeEvaluator, terminatorNodeEvaluator);
        }

        return null;
    }

    public static Evaluator allowlistNodeEvaluator(boolean filterStartNode, List<Node> allowlistNodes) {
        return new AllowlistNodeEvaluator(filterStartNode, allowlistNodes);
    }

    public static Evaluator denylistNodeEvaluator(boolean filterStartNode, List<Node> denylistNodes) {
        return new DenylistNodeEvaluator(filterStartNode, denylistNodes);
    }

    // The evaluators from pruneWhereEndNodeIs and includeWhereEndNodeIs interfere with each other, this makes them play nice
    private static class EndAndTerminatorNodeEvaluator implements Evaluator {
        private boolean filterStartNode;
        private int minLevel;
        private Evaluator endNodeEvaluator;
        private Evaluator terminatorNodeEvaluator;

        public EndAndTerminatorNodeEvaluator(boolean filterStartNode, int minLevel, Evaluator endNodeEvaluator, Evaluator terminatorNodeEvaluator) {
            this.filterStartNode = filterStartNode;
            this.minLevel = minLevel;
            this.endNodeEvaluator = endNodeEvaluator;
            this.terminatorNodeEvaluator = terminatorNodeEvaluator;
        }

        @Override
        public Evaluation evaluate(Path path) {
            if ((path.length() == 0 && !filterStartNode) || path.length() < minLevel) {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }

            // at least one has to give a thumbs up to include
            boolean includes = evalIncludes(endNodeEvaluator, path) || evalIncludes(terminatorNodeEvaluator, path);
            // prune = terminatorNodeEvaluator != null && !terminatorNodeEvaluator.evaluate(path).continues()
            // negate this to get continues result
            boolean continues = terminatorNodeEvaluator == null || terminatorNodeEvaluator.evaluate(path).continues();

            return Evaluation.of(includes, continues);
        }

        private boolean evalIncludes(Evaluator eval, Path path) {
            return eval != null && eval.evaluate(path).includes();
        }
    }

    private static class DenylistNodeEvaluator extends PathExpanderNodeEvaluator {
        private Set<Node> denylistSet;

        public DenylistNodeEvaluator(boolean filterStartNode, List<Node> denylistNodes) {
            super(filterStartNode);
            denylistSet = new HashSet<>(denylistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return path.length() == 0 && !filterStartNode ? Evaluation.INCLUDE_AND_CONTINUE :
                    denylistSet.contains(path.endNode()) ? Evaluation.EXCLUDE_AND_PRUNE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    }

    private static class AllowlistNodeEvaluator extends PathExpanderNodeEvaluator {
        private Set<Node> allowlistSet;

        public AllowlistNodeEvaluator(boolean filterStartNode, List<Node> allowlistNodes) {
            super(filterStartNode);
            allowlistSet = new HashSet<>(allowlistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return (path.length() == 0 && !filterStartNode) ? Evaluation.INCLUDE_AND_CONTINUE :
            allowlistSet.contains(path.endNode()) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

    private static abstract class PathExpanderNodeEvaluator implements Evaluator {
        public final boolean filterStartNode;

        private PathExpanderNodeEvaluator(boolean filterStartNode) {
            this.filterStartNode = filterStartNode;
        }
    }
}
