package jgea.problem.utils;

// Defines the set of available privacy metrics that can be used in the optimization problem
public enum PrivacyMetricChoice {

    // The k-anonymity metric based on average std dev
    K_ANONYMITY,

    // The advanced metric combining k-anonymity with a cardinality penalty
    K_ANONYMITY_CARDINALITY,

    // A weighted average of suppression, duplication, and modification
    WEIGHTED_AVERAGE,

    // A simple metric measuring only the fraction of suppressed tuples
    SUPPRESSION_ONLY
}