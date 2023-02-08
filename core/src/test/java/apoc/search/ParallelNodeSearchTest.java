package apoc.search;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

public class ParallelNodeSearchTest {

	@ClassRule
	public static DbmsRule db = new ImpermanentDbmsRule();

	@BeforeClass
    public static void initDb() {
		TestUtil.registerProcedure(db, ParallelNodeSearch.class);

		db.executeTransactionally(Util.readResourceFile("movies.cypher"));
    }

    @AfterClass
    public static void teardown() {
       db.shutdown();
    }

    @Test
    public void testMultiSearchNode() {
    	String query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAll() {
    	String query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    
    @Test 
    public void testMultiSearchNodeReduced() {
    	String query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAllReduced() {
    	String query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeAllReducedMapParam() {
    	String query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeNumberComparison() {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'>',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(12L,row.get("c")));
    }

    @Test
    public void testMultiSearchNodeNumberExactComparison() {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'=',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'exact',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    }
}
