package apoc.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TransactionTestUtil {
    public static final String TRANSACTION_LIST = "SHOW TRANSACTIONS";
    
    public static void checkTerminationGuard(GraphDatabaseService db, String query) {
        checkTerminationGuard(db, query, Collections.emptyMap());
    }
    
    public static void checkTerminationGuard(GraphDatabaseService db, String query, Map<String, Object> params) {
        terminateTransactionAsync(db, query);

        // check that the procedure/function fails with TransactionFailureException when transaction is terminated
        // todo 5, TimeUnit.SECONDS as parameter
        
//        db.executeTransactionally(query, params, r -> r.resultAsString());

        final long l = System.currentTimeMillis();
        try(Transaction transaction = db.beginTx(5, TimeUnit.SECONDS)) {
            transaction.execute(query, params).resultAsString();
//            System.out.println("s = " + s);
            transaction.commit();
            fail("Should fail because of TransactionFailureException");// todo - necessary this row?  fails with timeboxed
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            System.out.println("TransactionTestUtil.checkTerminationGuard");
            final String expected = "The transaction has been terminated. " +
                    "Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ";
            assertEquals(expected, rootCause.getMessage());
        }
        final long l1 = System.currentTimeMillis() - l;
        System.out.println("l - System.currentTimeMillis() = " + l1);
        
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
            System.out.println("TransactionTestUtil.terminateAndCheckTransaction");
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
    
            // todo - delete
            System.out.println("transactionId = " + transactionId[0]);
            final long l = System.currentTimeMillis();
            TestUtil.testCall(db, "TERMINATE TRANSACTION $transactionId",
                    map("transactionId", transactionId[0]),
                    result -> assertEquals("Transaction terminated.", result.get("message")));
            // todo - delete
            System.out.println("time=" + (System.currentTimeMillis() - l));
        }).start();

    }
}
