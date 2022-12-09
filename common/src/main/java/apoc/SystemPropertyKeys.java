package apoc;

public enum SystemPropertyKeys  {
    database,
    name,

    // cypher stored procedures/functions
    lastUpdated,
    statement,

    // triggers
    selector,
    params,
    paused,
}
