package apoc.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.MapUtil.map;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TransactionTestUtil {
    public static final String TRANSACTION_LIST = "SHOW TRANSACTIONS";
    
    public static void checkTerminationGuard(GraphDatabaseService db, String query) {
        checkTerminationGuard(db, query, emptyMap());
    }
    
    public static void checkTerminationGuard(GraphDatabaseService db, long timeout, String query) {
        checkTerminationGuard(db, timeout, query, emptyMap());
    }
    
    public static void checkTerminationGuard(GraphDatabaseService db, String query, Map<String, Object> params) {
        checkTerminationGuard(db, 10L, query, params);
    }
    
    public static void checkTerminationGuard(GraphDatabaseService db, long timeout, String query, Map<String, Object> params) {
        terminateTransactionAsync(db, query);

        // check that the procedure/function fails with TransactionFailureException when transaction is terminated
        try(Transaction transaction = db.beginTx(timeout, TimeUnit.SECONDS)) {
            transaction.execute(query, params).resultAsString();
            transaction.commit();
            fail("Should fail because of TransactionFailureException");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            final String expected = "The transaction has been terminated. " +
                    "Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ";
            assertEquals(expected, rootCause.getMessage());
        }
        
        checkTransactionNotInList(db, query);
    }

    public static void checkTransactionNotInList(GraphDatabaseService db, String query) {
        // checking for query cancellation from transaction list command
        TestUtil.testResult(db, TRANSACTION_LIST,
                map("query", query),
                result -> {
                    final boolean currentQuery = result.columnAs("currentQuery")
                            .stream()
                            .noneMatch(currQuery -> currQuery.equals(query));
                    assertTrue(currentQuery);
                });
    }

    public static void terminateTransactionAsync(GraphDatabaseService db, String query) {
        new Thread(() -> {
            // waiting for apoc query to cancel when it is found
            final String[] transactionId = new String[1];
            
            assertEventually(() -> db.executeTransactionally(TRANSACTION_LIST + " YIELD currentQuery, transactionId " + 
                            "WHERE currentQuery CONTAINS $query AND NOT currentQuery STARTS WITH $transactionList " +
                            "RETURN transactionId",
                    map("query", query, "transactionList", TRANSACTION_LIST),
                    result -> {
                        final ResourceIterator<String> msgIterator = result.columnAs("transactionId");
                        if (!msgIterator.hasNext()) {
                            return false;
                        }
                        transactionId[0] = msgIterator.next();
                        return transactionId[0] != null;
                    }), (value) -> value, 5L, TimeUnit.SECONDS);
    
            TestUtil.testCall(db, "TERMINATE TRANSACTION $transactionId",
                    map("transactionId", transactionId[0]),
                    result -> assertEquals("Transaction terminated.", result.get("message")));
        }).start();

    }
}
