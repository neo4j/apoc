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
package apoc.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FileTestUtil {

    public static void assertStreamEquals(File directoryExpected, String fileName, String actualText) {
        String expectedText = TestUtil.readFileToString(new File(directoryExpected, fileName));
        String[] actualArray = actualText.split("\n");
        String[] expectArray = expectedText.split("\n");
        assertEquals(expectArray.length, actualArray.length);
        for (int i = 0; i < actualArray.length; i++) {
            Object expected = stripIds(JsonUtil.parse(expectArray[i], null, Object.class));
            Object actual = stripIds(JsonUtil.parse(actualArray[i], null, Object.class));
            assertEquals(expected, actual);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object stripIds(Object value) {
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, Object> cleaned = new HashMap<>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                if ("id".equals(e.getKey())) {
                    continue; // skip ids
                }
                cleaned.put(e.getKey(), stripIds(e.getValue()));
            }
            return cleaned;
        }
        if (value instanceof List) {
            List<Object> list = (List<Object>) value;
            List<Object> cleaned = new ArrayList<>(list.size());
            for (Object o : list) {
                cleaned.add(stripIds(o));
            }
            return cleaned;
        }
        return value;
    }

    public static Path createTempFolder() {
        try {
            return Files.createTempDirectory(UUID.randomUUID().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
