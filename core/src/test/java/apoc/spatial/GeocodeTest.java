package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import inet.ipaddr.IPAddressString;
import org.junit.*;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;


public class GeocodeTest {

    private static final String BLOCKED_ADDRESS = "127.168.0.0";
    private static final String NON_BLOCKED_ADDRESS = "123.456.7.8";
    private static final String BLOCKED_ERROR = "access to /" + BLOCKED_ADDRESS + " is blocked via the configuration property internal.dbms.cypher_ip_blocklist";
    private static final String SOCKED_TIMEOUT_ERROR = "java.net.SocketTimeoutException: Connect timed out";
    private static final String URL_FORMAT = "%s://%s/geocode/v1/json?q=PLACE&key=KEY";
    private static final String REVERSE_URL_FORMAT = "%s://%s/geocode/v1/json?q=LAT+LNG&key=KEY";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString(BLOCKED_ADDRESS)) );

    @BeforeClass
    public static void initDb() {
        TestUtil.registerProcedure(db, Geocode.class);
    }

    @AfterClass
    public static void teardown() {
       db.shutdown();
    }

    // -- with config map
    @Test
    public void testWrongUrlButViaOtherProvider() throws Exception {
        // wrong url but doesn't fail because provider is osm, not opencage
        testGeocodeWithThrottling("osm", false, 
                map("url", "https://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY111"));
    }

    @Test
    public void testGeocodeWithBlockedAddressWithApocConf() {
        final String geocodeConfig = Geocode.PREFIX + "." + "opencage";
        apocConfig().setProperty(geocodeConfig + ".key", "myKey");
        
        Stream.of("https", "http", "ftp").forEach(protocol -> {
            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            
            // check that if either url or reverse address are blocked 
            // respectively the apoc.spatial.geocode and the apoc.spatial.reverseGeocode procedure fails
            apocConfig().setProperty(geocodeConfig + ".url", nonBlockedUrl);
            apocConfig().setProperty(geocodeConfig + ".reverse.url", String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS));

            assertGeocodeFails(true, BLOCKED_ERROR);
            
            apocConfig().setProperty(geocodeConfig + ".url", String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS));
            apocConfig().setProperty(geocodeConfig + ".reverse.url", nonBlockedReverseUrl);
            
            assertGeocodeFails(false, BLOCKED_ERROR);

            // check that if neither url nor reverse url are blocked 
            // the procedures continue the execution (in this case by throwing a SocketTimeoutException)
            apocConfig().setProperty(geocodeConfig + ".url", nonBlockedUrl);
            apocConfig().setProperty(geocodeConfig + ".reverse.url", nonBlockedReverseUrl);
            
            assertGeocodeFails(false, SOCKED_TIMEOUT_ERROR);
            assertGeocodeFails(true, SOCKED_TIMEOUT_ERROR);
        });
    }
    
    @Test
    public void testGeocodeWithBlockedAddressWithConfigMap() {
        Stream.of("https", "http", "ftp").forEach(protocol -> {

            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            
            // check that if either url or reverse address are blocked 
            // respectively the apoc.spatial.geocode and the apoc.spatial.reverseGeocode procedure fails
            assertGeocodeFails(true, BLOCKED_ERROR, 
                    nonBlockedUrl,
                    String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS)
            );

            assertGeocodeFails(false, BLOCKED_ERROR, 
                    String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS),
                    nonBlockedReverseUrl
            );

            // check that if neither url nor reverse url are blocked 
            // the procedures continue the execution (in this case by throwing a SocketTimeoutException)
            assertGeocodeFails(false, SOCKED_TIMEOUT_ERROR,
                    nonBlockedUrl,
                    nonBlockedReverseUrl
            );
            
            assertGeocodeFails(true, SOCKED_TIMEOUT_ERROR,
                    nonBlockedUrl,
                    nonBlockedReverseUrl
            );
        });
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, String url, String reverseUrl) {
        assertGeocodeFails(reverseGeocode, expectedMsgError, 
                Map.of("key", "myOwnKey", 
                        "url", url, 
                        "reverseUrl", reverseUrl)
        );
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError) {
        assertGeocodeFails(reverseGeocode, expectedMsgError, Collections.emptyMap());
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, Map<String, Object> conf) {
        QueryExecutionException e = assertThrows(QueryExecutionException.class,
                () -> testGeocodeWithThrottling( "opencage", reverseGeocode, conf )
        );

        final String actualMsgErr = e.getMessage();
        assertTrue("Actual err. message is " + actualMsgErr, actualMsgErr.contains(expectedMsgError));
    }

    @Test
    public void testGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", false);
    }

    @Test
    public void testReverseGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", true);
    }

    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode) throws Exception {
        testGeocodeWithThrottling(supplier, reverseGeocode, Collections.emptyMap());
    }
    
    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode, Map<String, Object> config) throws Exception {
        long fast = testGeocode(supplier, 100, reverseGeocode, config);
        long slow = testGeocode(supplier, 2000, reverseGeocode, config);
        assertTrue("Fast " + supplier + " took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 1.2);
    }

    private long testGeocode(String provider, long throttle, boolean reverseGeocode, Map<String, Object> config) throws Exception {
        setupSupplier(provider, throttle);
        InputStream is = getClass().getResourceAsStream("/spatial.json");
        Map tests = JsonUtil.OBJECT_MAPPER.readValue(is, Map.class);
        long start = System.currentTimeMillis();

        if(reverseGeocode) {
            for(Object address : (List) tests.get("events")) {
                testReverseGeocodeAddress(((Map)address).get("lat"), ((Map)address).get("lon"), config);
            }
        } else {
            for (Object address : (List) tests.get("addresses")) {
                testGeocodeAddress((Map) address, (String) config.getOrDefault("provider", provider), config);
            }
        }

        return System.currentTimeMillis() - start;
    }

    private void testReverseGeocodeAddress(Object latitude, Object longitude, Map<String, Object> config) {
        testResult(db, "CALL apoc.spatial.reverseGeocode($latitude, $longitude, false, $config)",
                map("latitude", latitude, "longitude", longitude, "config", config), (row) -> {
                    assertTrue(row.hasNext());
                    row.forEachRemaining((r)->{
                        assertNotNull(r.get("description"));
                        assertNotNull(r.get("location"));
                        assertNotNull(r.get("data"));
                    });
                });
    }


    private void setupSupplier(String providerName, long throttle) {
        apocConfig().setProperty(Geocode.PREFIX + ".provider", providerName);
        apocConfig().setProperty(Geocode.PREFIX + "." + providerName + ".throttle", Long.toString(throttle));
    }

    private void testGeocodeAddress(Map map, String provider, Map<String, Object> config) {
        testResult(db,"CALL apoc.spatial.geocode('FRANCE',1,true,$config)",
                map("config", config), (row)->{
                    row.forEachRemaining((r)->{
                        assertNotNull(r.get("description"));
                        assertNotNull(r.get("location"));
                        assertNotNull(r.get("data"));
                    });
                });
        
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                checkJsonFields(map, field);
            }
            System.out.println("map = " + map);
            testCallEmpty(db, "CALL apoc.spatial.geocode($url,0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    checkJsonFields(map, field);
                }
                testCallCount(db, "CALL apoc.spatial.geocode($url,0)",
                        map("url", map.get("address").toString()),
                        ((Number) ((Map) map.get("count")).get(provider)).intValue());
            }
        } else {
            for (String field : new String[]{"address", "osm"}) {
                checkJsonFields(map, field);
            }
            testGeocodeAddress(map.get("address").toString(),
                    getCoord(map, provider, "latitude"),
                    getCoord(map, provider, "longitude"), 
                    config);
        }
    }

    private double getCoord(Map<String, Map<String, Double>> map, String provider, String coord) {
        final Map<String, Double> providerKey = map.getOrDefault(provider.toLowerCase(), map.get("osm"));
        checkJsonFields(providerKey, coord);
        return providerKey.get(coord);
    }

    private void checkJsonFields(Map map, String field) {
        assertTrue("Expected " + field + " field", map.containsKey(field));
    }

    private void testGeocodeAddress(String address, double lat, double lon, Map<String, Object> config) {
        testResult(db, "CALL apoc.spatial.geocodeOnce($url, $config)", 
                map("url", address, "config", config),
                (result) -> {
                    if (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        Map value = (Map) row.get("location");
                        assertNotNull("location found", value);
                        assertEquals("Incorrect latitude found", lat, Double.parseDouble(value.get("latitude").toString()),
                                0.1);
                        assertEquals("Incorrect longitude found", lon, Double.parseDouble(value.get("longitude").toString()),
                                0.1);
                        assertFalse(result.hasNext());
                    } else {
                        // over request limit
                    }
                });
    }
}
