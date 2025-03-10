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

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.Util.toDouble;
import static apoc.util.Util.toLong;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;

import apoc.util.JsonUtil;
import apoc.util.Util;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.*;

public class Geocode {
    public static final int MAX_RESULTS = 100;
    public static final String PREFIX = "apoc.spatial.geocode";
    public static final String GEOCODE_PROVIDER_KEY = "provider";

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public URLAccessChecker urlAccessChecker;

    interface GeocodeSupplier {
        Stream<GeoCodeResult> geocode(String params, long maxResults, URLAccessChecker urlAccessChecker);

        Stream<GeoCodeResult> reverseGeocode(Double latitude, Double longitude, URLAccessChecker urlAccessChecker);
    }

    private static class Throttler {
        private final TerminationGuard terminationGuard;
        private long throttleInMs;
        private static long lastCallTime = 0L;
        private static long DEFAULT_THROTTLE = 5 * 1000; // 5 seconds
        private static long MAX_THROTTLE = 60 * 60 * 1000; // 1 hour

        public Throttler(TerminationGuard terminationGuard, long throttle) {
            this.terminationGuard = terminationGuard;

            throttle = Math.min(throttle, MAX_THROTTLE);
            if (throttle < 0) throttle = DEFAULT_THROTTLE;

            this.throttleInMs = throttle;
        }

        private void waitForThrottle() {
            long msSinceLastCall = currentTimeMillis() - lastCallTime;
            while (msSinceLastCall < throttleInMs) {
                try {
                    terminationGuard.check();
                    long msToWait = throttleInMs - msSinceLastCall;
                    Thread.sleep(Math.min(msToWait, 1000));
                } catch (InterruptedException e) {
                    // ignore
                }
                msSinceLastCall = currentTimeMillis() - lastCallTime;
            }
            lastCallTime = currentTimeMillis();
        }
    }

    private static class SupplierWithKey implements GeocodeSupplier {
        private static final String[] FORMATTED_KEYS =
                new String[] {"formatted", "formatted_address", "address", "description", "display_name"};
        private static final String[] LAT_KEYS = new String[] {"lat", "latitude"};
        private static final String[] LNG_KEYS = new String[] {"lng", "longitude", "lon"};
        private Throttler throttler;
        private String configBase;
        private String urlTemplate;
        private String urlTemplateReverse;

        public SupplierWithKey(Configuration config, TerminationGuard terminationGuard, String provider) {
            this.configBase = provider;

            if (!config.containsKey(configKey("url"))) {
                throw new IllegalArgumentException("Missing 'url' for geocode provider: " + provider);
            }
            if (!config.containsKey(configKey("reverse.url"))) {
                throw new IllegalArgumentException("Missing 'reverse.url' for reverse-geocode provider: " + provider);
            }
            urlTemplate = config.getString(configKey("url"));
            if (!urlTemplate.contains("PLACE"))
                throw new IllegalArgumentException("Missing 'PLACE' in url template: " + urlTemplate);

            urlTemplateReverse = config.getString(configKey("reverse.url"));
            if (!urlTemplateReverse.contains("LAT") || !urlTemplateReverse.contains("LNG"))
                throw new IllegalArgumentException("Missing 'LAT' or 'LNG' in url template: " + urlTemplateReverse);

            if (urlTemplate.contains("KEY") && !config.containsKey(configKey("key"))) {
                throw new IllegalArgumentException("Missing 'key' for geocode provider: " + provider);
            }

            if (urlTemplateReverse.contains("KEY") && !config.containsKey(configKey("key"))) {
                throw new IllegalArgumentException("Missing 'key' for reverse-geocode provider: " + provider);
            }
            String key = config.getString(configKey("key"));
            urlTemplate = urlTemplate.replace("KEY", key);
            urlTemplateReverse = urlTemplateReverse.replace("KEY", key);

            this.throttler = new Throttler(
                    terminationGuard, config.getInt(configKey("throttle"), (int) Throttler.DEFAULT_THROTTLE));
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults, URLAccessChecker urlAccessChecker) {
            if (address.isEmpty()) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            String url = urlTemplate.replace("PLACE", Util.encodeUrlComponent(address));
            Object value = JsonUtil.loadJson(url, urlAccessChecker).findFirst().orElse(null);
            if (value instanceof List) {
                return findResults((List<Map<String, Object>>) value, maxResults);
            } else if (value instanceof Map) {
                Object results = ((Map) value).get("results");
                if (results instanceof List) {
                    return findResults((List<Map<String, Object>>) results, maxResults);
                }
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }

        @Override
        public Stream<GeoCodeResult> reverseGeocode(
                Double latitude, Double longitude, URLAccessChecker urlAccessChecker) {
            if (latitude == null || longitude == null) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            String url = urlTemplateReverse.replace("LAT", latitude.toString()).replace("LNG", longitude.toString());
            Object value = JsonUtil.loadJson(url, urlAccessChecker).findFirst().orElse(null);
            if (value instanceof List) {
                return findResults((List<Map<String, Object>>) value, 1);
            } else if (value instanceof Map) {
                Object results = ((Map) value).get("results");
                if (results instanceof List) {
                    return findResults((List<Map<String, Object>>) results, 1);
                }
            }
            throw new RuntimeException("Can't parse reverse-geocoding results " + value);
        }

        @SuppressWarnings("unchecked")
        private Stream<GeoCodeResult> findResults(List<Map<String, Object>> results, long maxResults) {
            return results.stream().limit(maxResults).map(data -> {
                String description = findFirstEntry(data, FORMATTED_KEYS);
                Map<String, Object> location = (Map<String, Object>) data.get("geometry");
                if (location.containsKey("location")) {
                    location = (Map<String, Object>) location.get("location");
                }
                String lat = findFirstEntry(location, LAT_KEYS);
                String lng = findFirstEntry(location, LNG_KEYS);
                return new GeoCodeResult(toDouble(lat), toDouble(lng), description, data);
            });
        }

        private String findFirstEntry(Map<String, Object> map, String[] keys) {
            for (String key : keys) {
                if (map.containsKey(key)) {
                    return valueOf(map.get(key));
                }
            }
            return "";
        }

        private String configKey(String name) {
            return configBase + "." + name;
        }
    }

    private static class OSMSupplier implements GeocodeSupplier {
        public static final String OSM_URL = "https://nominatim.openstreetmap.org";

        private static final String OSM_URL_REVERSE_GEOCODE = OSM_URL + "/reverse?format=jsonv2&";
        private static final String OSM_URL_GEOCODE = OSM_URL + "/search.php?format=json&q=";

        private Throttler throttler;

        public OSMSupplier(Configuration config, TerminationGuard terminationGuard) {
            this.throttler = new Throttler(
                    terminationGuard,
                    toLong(config.getString("osm.throttle", Long.toString(Throttler.DEFAULT_THROTTLE))));
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults, URLAccessChecker urlAccessChecker) {
            if (address.isEmpty()) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            Object value = JsonUtil.loadJson(OSM_URL_GEOCODE + Util.encodeUrlComponent(address), urlAccessChecker)
                    .findFirst()
                    .orElse(null);
            if (value instanceof List) {
                return ((List<Map<String, Object>>) value)
                        .stream()
                                .limit(maxResults)
                                .map(data -> new GeoCodeResult(
                                        toDouble(data.get("lat")),
                                        toDouble(data.get("lon")),
                                        valueOf(data.get("display_name")),
                                        data));
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }

        @Override
        public Stream<GeoCodeResult> reverseGeocode(
                Double latitude, Double longitude, URLAccessChecker urlAccessChecker) {
            if (latitude == null || longitude == null) {
                return Stream.empty();
            }
            throttler.waitForThrottle();

            Object value = JsonUtil.loadJson(
                            OSM_URL_REVERSE_GEOCODE + String.format("lat=%s&lon=%s", latitude, longitude),
                            urlAccessChecker)
                    .findFirst()
                    .orElse(null);
            if (value instanceof Map) {
                Map<String, Object> data = (Map<String, Object>) value;
                return Stream.of(new GeoCodeResult(
                        toDouble(data.get("lat")), toDouble(data.get("lon")), valueOf(data.get("display_name")), (Map<
                                        String, Object>)
                                data.get("address")));
            }
            throw new RuntimeException("Can't parse reverse-geocoding results " + value);
        }
    }

    private static class GoogleSupplier implements GeocodeSupplier {
        private final Throttler throttler;
        private Configuration config;

        private static final String BASE_GOOGLE_API_URL = "https://maps.googleapis.com/maps/api/geocode/json";

        private static final String REVERSE_GEOCODE_URL = BASE_GOOGLE_API_URL + "?%s&latlng=";
        private static final String GEOCODE_URL = BASE_GOOGLE_API_URL + "?%s&address=";

        public GoogleSupplier(Configuration config, TerminationGuard terminationGuard) {
            this.throttler = new Throttler(
                    terminationGuard,
                    toLong(config.getString("google.throttle", Long.toString(Throttler.DEFAULT_THROTTLE))));
            this.config = config;
        }

        private String credentials(Configuration config) {
            if (config.containsKey("google.client") && config.containsKey("google.signature")) {
                return "client=" + config.getString("google.client") + "&signature="
                        + config.getString("google.signature");
            } else if (config.containsKey("google.key")) {
                return "key=" + config.getString("google.key");
            } else {
                return "auth=free"; // throw new RuntimeException("apoc.spatial.geocode: No google client or key
                // specified in apoc.conf config file");
            }
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults, URLAccessChecker urlAccessChecker) {
            if (address.isEmpty()) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            Object value = JsonUtil.loadJson(
                            String.format(GEOCODE_URL, credentials(this.config)) + Util.encodeUrlComponent(address),
                            urlAccessChecker)
                    .findFirst()
                    .orElse(null);
            if (value instanceof Map) {
                Map map = (Map) value;
                if (map.get("status").equals("OVER_QUERY_LIMIT"))
                    throw new IllegalStateException("QUOTA_EXCEEDED from geocode API: " + map.get("status")
                            + " message: " + map.get("error_message"));
                Object results = map.get("results");
                if (results instanceof List) {
                    return ((List<Map<String, Object>>) results)
                            .stream().limit(maxResults).map(data -> {
                                Map location = (Map) ((Map) data.get("geometry")).get("location");
                                return new GeoCodeResult(
                                        toDouble(location.get("lat")),
                                        toDouble(location.get("lng")),
                                        valueOf(data.get("formatted_address")),
                                        data);
                            });
                }
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }

        @Override
        public Stream<GeoCodeResult> reverseGeocode(
                Double latitude, Double longitude, URLAccessChecker urlAccessChecker) {
            if (latitude == null || longitude == null) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            Object value = JsonUtil.loadJson(
                            String.format(REVERSE_GEOCODE_URL, credentials(this.config))
                                    + Util.encodeUrlComponent(latitude + "," + longitude),
                            urlAccessChecker)
                    .findFirst()
                    .orElse(null);
            if (value instanceof Map) {
                Map map = (Map) value;
                if (map.get("status").equals("OVER_QUERY_LIMIT"))
                    throw new IllegalStateException("QUOTA_EXCEEDED from geocode API: " + map.get("status")
                            + " message: " + map.get("error_message"));
                Object results = map.get("results");
                if (results instanceof List) {
                    return ((List<Map<String, Object>>) results)
                            .stream().limit(1).map(data -> {
                                Map location = (Map) ((Map) data.get("geometry")).get("location");
                                return new GeoCodeResult(
                                        toDouble(location.get("lat")),
                                        toDouble(location.get("lng")),
                                        valueOf(data.get("formatted_address")),
                                        data);
                            });
                }
            }
            throw new RuntimeException("Can't parse reverse-geocoding results " + value);
        }
    }

    private GeocodeSupplier getSupplier(Map<String, Object> configMap) {
        return getSupplier(configMap, terminationGuard);
    }

    public static GeocodeSupplier getSupplier(Map<String, Object> configMap, TerminationGuard terminationGuard) {
        final AbstractMap.SimpleEntry<GeocodeSupplier, String> results = getSupplierEntry(terminationGuard, configMap);
        return results.getKey();
    }

    public static AbstractMap.SimpleEntry<GeocodeSupplier, String> getSupplierEntry(
            TerminationGuard terminationGuard, Map<String, Object> configMap) {
        Configuration activeConfig = apocConfig().getConfig().subset(PREFIX);
        // with configMap we overwrite the ApocConfig, if none of these is found, we choose the default one, 'osm'
        final String provider = (String)
                configMap.getOrDefault(GEOCODE_PROVIDER_KEY, activeConfig.getString(GEOCODE_PROVIDER_KEY, "osm"));

        configMap.forEach((key, value) -> {
            // we transform e.g. key `reverseUrl` to `reverse.url`, consistently to ApocConfig
            final String dotCase = key.replaceAll("[A-Z][a-z]", ".$0").toLowerCase();
            activeConfig.setProperty(provider + "." + dotCase, value);
        });

        String supplier = provider.toLowerCase();
        final GeocodeSupplier geocodeSupplier = getGeocodeSupplier(terminationGuard, activeConfig, supplier);
        // we return both GeocodeSupplier for real implementations and String supplier for mock tests
        return new AbstractMap.SimpleEntry<>(geocodeSupplier, supplier);
    }

    public static GeocodeSupplier getGeocodeSupplier(
            TerminationGuard terminationGuard, Configuration activeConfig, String supplier) {
        return switch (supplier) {
            case "google" -> new GoogleSupplier(activeConfig, terminationGuard);
            case "osm" -> new OSMSupplier(activeConfig, terminationGuard);
            default -> new SupplierWithKey(activeConfig, terminationGuard, supplier);
        };
    }

    @Procedure("apoc.spatial.geocodeOnce")
    @Description(
            "Returns the geographic location (latitude, longitude, and description) of the given address using a geocoding service (default: OpenStreetMap).\n"
                    + "This procedure returns at most one result.")
    public Stream<GeoCodeResult> geocodeOnce(
            @Name(value = "location", description = "The location to search for.") String address,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            provider = 'osm' :: STRING,
                            url :: STRING,
                            reverseUrl: :: STRING,
                            key :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        return geocode(address, 1L, false, config);
    }

    @Procedure("apoc.spatial.geocode")
    @Description(
            "Returns the geographic location (latitude, longitude, and description) of the given address using a geocoding service (default: OpenStreetMap).")
    public Stream<GeoCodeResult> geocode(
            @Name(value = "location", description = "The location to search for.") String address,
            @Name(value = "maxResults", defaultValue = "100", description = "The maximum number of returned results.")
                    long maxResults,
            @Name(
                            value = "quotaException",
                            defaultValue = "false",
                            description =
                                    "Whether or not to throw an exception when the maximum request quota is reached.")
                    boolean quotaException,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            provider = 'osm' :: STRING,
                            url :: STRING,
                            reverseUrl: :: STRING,
                            key :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        if (address == null || address.isEmpty()) return Stream.empty();
        else {
            try {
                return getSupplier(config)
                        .geocode(
                                address,
                                maxResults == 0 ? MAX_RESULTS : Math.min(Math.max(maxResults, 1), MAX_RESULTS),
                                urlAccessChecker);
            } catch (IllegalStateException re) {
                if (!quotaException && re.getMessage().startsWith("QUOTA_EXCEEDED")) return Stream.empty();
                throw re;
            }
        }
    }

    @Procedure("apoc.spatial.reverseGeocode")
    @Description(
            "Returns a textual address from the given geographic location (latitude, longitude) using a geocoding service (default: OpenStreetMap).\n"
                    + "This procedure returns at most one result.")
    public Stream<GeoCodeResult> reverseGeocode(
            @Name(value = "latitude", description = "The latitude of the location.") double latitude,
            @Name(value = "longitude", description = "The longitude of the location.") double longitude,
            @Name(
                            value = "quotaException",
                            defaultValue = "false",
                            description =
                                    "Whether or not to throw an exception when the maximum request quota is reached.")
                    boolean quotaException,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            provider = 'osm' :: STRING,
                            url :: STRING,
                            reverseUrl: :: STRING,
                            key :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        try {
            return getSupplier(config).reverseGeocode(latitude, longitude, urlAccessChecker);
        } catch (IllegalStateException re) {
            if (!quotaException && re.getMessage().startsWith("QUOTA_EXCEEDED")) return Stream.empty();
            throw re;
        }
    }

    public static class GeoCodeResult {
        @Description("A detailed map of information on the found location.")
        public final Map<String, Object> location;

        @Description("A map of returned data from the given provider.")
        public final Map<String, Object> data;

        @Description("The latitude of the found location.")
        public final Double latitude;

        @Description("The longitude of the found location.")
        public final Double longitude;

        @Description("A description of the found location.")
        public final String description;

        public GeoCodeResult(Double latitude, Double longitude, String description, Map<String, Object> data) {
            this.data = data;
            this.latitude = latitude;
            this.longitude = longitude;
            this.description = description;
            this.location = map("latitude", latitude, "longitude", longitude, "description", description);
        }
    }
}
