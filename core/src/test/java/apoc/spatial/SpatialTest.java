/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.spatial;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import apoc.date.Date;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class SpatialTest {

    private static final String WRONG_PROVIDER_ERR = "wrong provider";
    private static final String URL = "http://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY";
    private static final String REVERSE_URL = "http://api.opencagedata.com/geocode/v1/json?q=LAT+LNG&key=KEY";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Map<String, Map<String, Object>> eventNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceTimeNodes = new LinkedHashMap<>();

    // we extend Geocode and override methods to make sure signatures are equivalents
    public static class MockGeocode extends Geocode {
        public static Map<String, Map> geocodeResults = null;
        public static Map<String, Map> reverseGeocodeResults = null;

        @Override
        @Procedure("apoc.spatial.geocodeOnce")
        public Stream<Geocode.GeoCodeResult> geocodeOnce(
                @Name("location") String address,
                @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
            return geocode(address, 1, false, config);
        }

        @Override
        @Procedure("apoc.spatial.geocode")
        public Stream<Geocode.GeoCodeResult> geocode(
                @Name("location") String address,
                @Name("maxResults") long maxResults,
                @Name(value = "quotaException", defaultValue = "false") boolean quotaException,
                @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

            if (address == null || address.isEmpty()) return Stream.empty();
            else {
                if (geocodeResults != null && geocodeResults.containsKey(address)) {
                    // mocked GeocodeSupplier.geocode(...)
                    Map data = getGeocodeData(config, address, geocodeResults);
                    return Stream.of(new Geocode.GeoCodeResult(
                            Util.toDouble(data.get("lat")),
                            Util.toDouble(data.get("lon")),
                            (String) data.get("display_name"),
                            data));
                } else {
                    return Stream.empty();
                }
            }
        }

        @Override
        @Procedure("apoc.spatial.reverseGeocode")
        public Stream<Geocode.GeoCodeResult> reverseGeocode(
                @Name("latitude") double latitude,
                @Name("longitude") double longitude,
                @Name(value = "quotaException", defaultValue = "false") boolean quotaException,
                @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
            String key = latitude + "," + longitude;
            if (reverseGeocodeResults != null && reverseGeocodeResults.containsKey(key)) {
                // mocked GeocodeSupplier.reverseGeocode(...)
                Map data = getGeocodeData(config, key, reverseGeocodeResults);
                return Stream.of(
                        new Geocode.GeoCodeResult(latitude, longitude, (String) data.get("display_name"), data));
            } else {
                return Stream.empty();
            }
        }

        private Map getGeocodeData(Map<String, Object> config, String key, Map<String, Map> geocodeResults) {
            // we get the supplier name
            final String supplier = getSupplier(config);
            // from here we mock GeocodeSupplier.reverseGeocode/geocode(...)
            final Map<String, Map> geocodeResult = geocodeResults.get(key);
            // we get mock data by supplier, currently "osm", "opencage" or "google"
            Map data = geocodeResult.get(supplier);
            // this condition serves to ensure the implementation works correctly
            if (data == null) {
                throw new RuntimeException(WRONG_PROVIDER_ERR);
            }
            return data;
        }

        private String getSupplier(Map<String, Object> config) {
            // to make sure that the config is correctly formatted we call the correct GeocodeSupplier constructor
            final AbstractMap.SimpleEntry<Geocode.GeocodeSupplier, String> entry = getSupplierEntry(() -> {}, config);
            return entry.getValue();
        }
    }

    @Before
    public void setUp() {
        URLAccessChecker urlAccessChecker = new URLAccessChecker() {
            @Override
            public java.net.URL checkURL(java.net.URL url) throws URLAccessValidationError {
                return url;
            }
        };

        TestUtil.registerProcedure(db, Date.class);
        TestUtil.registerProcedure(db, MockGeocode.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map)
                JsonUtil.loadJson(url.toString(), urlAccessChecker).findFirst().orElse(null);
        for (Object event : (List) tests.get("events")) {
            addEventData((Map) event);
        }
        MockGeocode.geocodeResults = (Map<String, Map>) tests.get("geocode");
        MockGeocode.reverseGeocodeResults = (Map<String, Map>) tests.get("reverseGeocode");
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    private void addEventData(Map<String, Object> event) {
        Map<String, Object> params = map("params", event);
        int created =
                db.executeTransactionally("CREATE (e:Event $params)", params, result -> result.getQueryStatistics()
                        .getNodesCreated());
        assertEquals("Expected a node to be created", 1, created);
        String name = event.get("name").toString();
        if (!event.containsKey("toofar")) {
            spaceNodes.put(name, event);
            if (!event.containsKey("toosoon") && !event.containsKey("toolate")) {
                spaceTimeNodes.put(name, event);
            }
        }
        eventNodes.put(name, event);
    }

    @Test
    public void testSimpleGeocode() {
        final Map<String, Object> config = Collections.emptyMap();
        geocodeOnceCommon(config);
    }

    @Test
    public void testGeocodeOpencageWrongUrlFormat() {
        // with provider different from osm/google we have to explicit an url correctly formatted (i.e. with 'PLACE'
        // string)
        final Map<String, Object> conf = map("provider", "opencage", "url", "wrongUrl", "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> geocodeOnceCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'PLACE' in url template"));
    }

    @Test
    public void testGeocodeOpencageMissingKey() {
        // with provider (via config map) different from osm/google we have to explicit the key
        final Map<String, Object> conf = map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> geocodeOnceCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
    }

    @Test
    public void testGeocodeOpencageMissingKeyViaApocConfig() {
        // with provider(via apocConfig()) different from osm/google we have to explicit the key
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "something");
        final Map<String, Object> conf = map("url", URL, "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> geocodeOnceCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
    }

    @Test
    public void testSimpleGeocodeViaApocConfig() {
        // Missing key but doesn't fail because provider is google via ApocConfig, not opencage like
        // testGeocodeOpencageMissingKey
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "google");
        final Map<String, Object> config = map("url", "mockUrl", "reverseUrl", "mockReverseUrl");
        geocodeOnceCommon(config);
    }

    @Test
    public void testSimpleGeocodeOpencageOverwiteApocConfigs() {
        // the key is defined in apocConfig()
        // the url and provider are in both apocConfig() and config map, but the second ones win
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "anotherOne");
        apocConfig().setProperty(Geocode.PREFIX + ".opencage.key", "myOwnMockKey");
        final Map<String, Object> config = map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL);
        geocodeOnceCommon(config);
    }

    @Test
    public void testSimpleGeocodeWithWrongProvider() {
        // just to make sure that the spatial.json is well implemented
        // we pass a well-formatted url, reverse url and key but an incorrect provider
        final Map<String, Object> config =
                map("provider", "incorrect", "url", URL, "reverseUrl", REVERSE_URL, "key", "myOwnMockKey");

        RuntimeException e = assertThrows(RuntimeException.class, () -> geocodeOnceCommon(config));
        assertTrue(e.getMessage().contains(WRONG_PROVIDER_ERR));
    }

    @Test
    public void testSimpleGeocodeOpencage() {
        final Map<String, Object> config =
                map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL, "key", "myOwnMockKey");
        geocodeOnceCommon(config);
    }

    @Test
    public void testSimpleGeocodeGoogle() {
        final Map<String, Object> config = map("provider", "google");
        geocodeOnceCommon(config);
    }

    private void geocodeOnceCommon(Map<String, Object> config) {
        String query = "MATCH (a:Event) \n" + "WHERE a.address IS NOT NULL AND a.name IS NOT NULL \n"
                + "CALL apoc.spatial.geocodeOnce(a.address, $config) "
                + "YIELD latitude, longitude, description\n"
                + "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND description IS NOT NULL \n"
                + "RETURN *";
        testCallCount(db, query, map("config", config), eventNodes.size());
    }

    @Test
    public void testGeocodePointAndDistance() {
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" + "MATCH (a:Event) \n"
                + "WHERE a.address IS NOT NULL \n"
                + "CALL apoc.spatial.geocodeOnce(a.address) YIELD location \n"
                + "WITH location, point.distance(point({latitude: location.latitude, longitude:location.longitude}), eiffel) AS distance\n"
                + "WHERE distance < 5000\n"
                + "RETURN location.description AS description, distance\n"
                + "ORDER BY distance\n"
                + "LIMIT 100\n";
        testCallCount(db, query, spaceNodes.size());
    }

    @Test
    public void testGraphRefactoring() {
        String refactorQuery = "MATCH (a:Event) \n" + "WHERE a.address IS NOT NULL AND a.latitude IS NULL \n"
                + "WITH a LIMIT 1000\n"
                + "CALL apoc.spatial.geocodeOnce(a.address) YIELD location \n"
                + "SET a.latitude = location.latitude\n"
                + "SET a.longitude = location.longitude";
        testCallEmpty(db, refactorQuery, emptyMap());
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" + "MATCH (a:Event) \n"
                + "WHERE a.latitude IS NOT NULL AND a.longitude IS NOT NULL \n"
                + "WITH a, point.distance(point(a), eiffel) AS distance\n"
                + "WHERE distance < 5000\n"
                + "RETURN a.name AS event, distance\n"
                + "ORDER BY distance\n"
                + "LIMIT 100\n";
        testCallCount(db, query, spaceNodes.size());
    }

    @Test
    public void testAllTheThings() {
        String query = "WITH apoc.date.parse('2016-06-01 00:00:00','h') as due_date,\n"
                + "     point({latitude: 48.8582532, longitude: 2.294287}) as eiffel\n"
                + "MATCH (e:Event)\n"
                + "WHERE e.address IS NOT NULL AND e.datetime IS NOT NULL \n"
                + "WITH apoc.date.parse(e.datetime,'h') as hours, e, due_date, eiffel\n"
                + "CALL apoc.spatial.geocodeOnce(e.address) YIELD location\n"
                + "WITH e, location,\n"
                + "     point.distance(point({longitude: location.longitude, latitude:location.latitude}), eiffel) as distance,\n"
                + "     (due_date - hours)/24.0 as days_before_due\n"
                + "WHERE distance < 5000 AND days_before_due < 14 AND hours < due_date\n"
                + "RETURN e.name as event, e.datetime as date,\n"
                + "       location.description as description, distance\n"
                + "ORDER BY distance\n"
                + "LIMIT 100\n";
        int expectedCount = spaceTimeNodes.size();
        testResult(db, query, res -> {
            int left = expectedCount;
            while (left > 0) {
                assertTrue(
                        "Expected " + expectedCount + " results, but got only " + (expectedCount - left),
                        res.hasNext());
                Map<String, Object> result = res.next();
                assertTrue("Event should have 'event' property", result.containsKey("event"));
                String name = (String) result.get("event");
                assertTrue("Event '" + name + "' was not expected", spaceTimeNodes.containsKey(name));
                left--;
            }
            assertFalse("Expected " + expectedCount + " results, but there are more ", res.hasNext());
        });
    }

    @Test
    public void testSimpleReverseGeocode() {
        final Map<String, Object> config = map();
        reverseGeocodeCommon(config);
    }

    @Test
    public void testReverseGeocodeOpencageWrongUrlFormat() {
        // with provider different from osm/google we have to explicit an url correctly formatted (i.e. with 'PLACE'
        // string)
        final Map<String, Object> conf = map("provider", "opencage", "url", "wrongUrl", "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> reverseGeocodeCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'PLACE' in url template"));
    }

    @Test
    public void testReverseGeocodeOpencageMissingKey() {
        // with provider (via config map) different from osm/google we have to explicit the key
        final Map<String, Object> conf = map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> reverseGeocodeCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
    }

    @Test
    public void testReverseGeocodeOpencageMissingKeyViaApocConfig() {
        // with provider(via apocConfig()) different from osm/google we have to explicit the key
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "something");
        final Map<String, Object> conf = map("url", URL, "reverseUrl", REVERSE_URL);

        RuntimeException e = assertThrows(RuntimeException.class, () -> reverseGeocodeCommon(conf));
        assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
    }

    @Test
    public void testSimpleReverseGeocodeViaApocConfig() {
        // Missing key but doesn't fail because provider is google via ApocConfig, not opencage like
        // testGeocodeOpencageMissingKey
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "google");
        final Map<String, Object> config = map("url", "mockUrl", "reverseUrl", "mockReverseUrl");
        reverseGeocodeCommon(config);
    }

    @Test
    public void testSimpleReverseGeocodeOpencageOverwiteApocConfigs() {
        // the key is defined in apocConfig()
        // the url and provider are in both apocConfig() and config map, but the second ones win
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "anotherOne");
        apocConfig().setProperty(Geocode.PREFIX + ".opencage.key", "myOwnMockKey");
        final Map<String, Object> config = map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL);
        reverseGeocodeCommon(config);
    }

    @Test
    public void testSimpleReverseGeocodeWithWrongProvider() {
        // just to make sure that the spatial.json is well implemented
        final Map<String, Object> config =
                map("provider", "incorrect", "url", URL, "reverseUrl", REVERSE_URL, "key", "myOwnMockKey");

        RuntimeException e = assertThrows(RuntimeException.class, () -> reverseGeocodeCommon(config));
        assertTrue(e.getMessage().contains(WRONG_PROVIDER_ERR));
    }

    @Test
    public void testSimpleReverseGeocodeOpencage() {
        final Map<String, Object> config =
                map("provider", "opencage", "url", URL, "reverseUrl", REVERSE_URL, "key", "myOwnMockKey");
        reverseGeocodeCommon(config);
    }

    @Test
    public void testSimpleReverseGeocodeGoogle() {
        final Map<String, Object> config = map("provider", "google");
        reverseGeocodeCommon(config);
    }

    private void reverseGeocodeCommon(Map<String, Object> config) {
        String query = "MATCH (a:Event) \n" + "WHERE a.lat IS NOT NULL AND a.lon IS NOT NULL AND a.name IS NOT NULL \n"
                + "CALL apoc.spatial.reverseGeocode(a.lat, a.lon, false, $config) \n"
                + "YIELD latitude, longitude, description\n"
                + "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND description IS NOT NULL \n"
                + "RETURN *";
        testCallCount(db, query, map("config", config), eventNodes.size());
    }

    @Test
    public void testNullAddressErrorGeocodeOnce() {
        testCallEmpty(db, "CALL apoc.spatial.geocodeOnce(null)", emptyMap());
    }

    @Test
    public void testNullAddressErrorGeocodeShouldFail() {
        testCallEmpty(db, "CALL apoc.spatial.geocode(null,1)", emptyMap());
    }
}
