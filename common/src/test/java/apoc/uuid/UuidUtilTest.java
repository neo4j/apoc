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
package apoc.uuid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class UuidUtilTest {

    @Test
    public void fromHexToBase64() {
        var input = "290d6cba-ce94-455e-b59f-029cf1e395c5";
        var output = UuidUtil.fromHexToBase64(input);
        assertEquals("KQ1sus6URV61nwKc8eOVxQ", output);
    }

    @Test
    public void fromBase64ToHex() {
        var input = "KQ1sus6URV61nwKc8eOVxQ";
        var output = UuidUtil.fromBase64ToHex(input);
        assertEquals("290d6cba-ce94-455e-b59f-029cf1e395c5", output);
    }

    @Test
    public void fromBase64WithAlignmentToHex() {
        var input = "KQ1sus6URV61nwKc8eOVxQ==";
        var output = UuidUtil.fromBase64ToHex(input);
        assertEquals("290d6cba-ce94-455e-b59f-029cf1e395c5", output);
    }

    @Test
    public void shouldFailIfHexFormatIsWrong() {
        var input = "290d6cba-455e-b59f-029cf1e395c5";
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> UuidUtil.fromHexToBase64(input));
        assertEquals("Invalid UUID string: 290d6cba-455e-b59f-029cf1e395c5", e.getMessage());
    }

    @Test
    public void shouldFailIfHexFormatIsEmpty() {
        var input = "";
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> UuidUtil.fromHexToBase64(input));
        assertEquals("Invalid UUID string: ", e.getMessage());
    }

    @Test
    public void shouldFailIfHexFormatIsNull() {
        assertThrows(NullPointerException.class, () -> UuidUtil.fromHexToBase64(null));
    }

    @Test
    public void shouldFailIfBase64LengthIsWrong() {
        var input1 = "KQ1sus6URV61nwKc8eO=="; // wrong length
        IllegalStateException e1 = assertThrows(IllegalStateException.class, () -> UuidUtil.fromBase64ToHex(input1));
        assertEquals("Invalid UUID length. Expected 24 characters", e1.getMessage());

        var input2 = "Q1sus6URV61nwKc8eOVxQ"; // wrong length
        IllegalStateException e2 = assertThrows(IllegalStateException.class, () -> UuidUtil.fromBase64ToHex(input2));
        assertEquals("Invalid UUID length. Expected 22 characters", e2.getMessage());
    }

    @Test
    public void shouldFailIfBase64IsEmpty() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> UuidUtil.fromBase64ToHex(""));
        assertEquals("Expected not empty UUID value", e.getMessage());
    }

    @Test
    public void shouldFailIfBase64IsNull() {
        assertThrows(NullPointerException.class, () -> UuidUtil.fromBase64ToHex(null));
    }

    @Test
    public void generateBase64ForSpecificUUIDs() {
        var uuid = UUID.fromString("00000000-0000-0000-0000-000000000000");
        var uuidBase64 = UuidUtil.generateBase64Uuid(uuid);
        assertEquals("AAAAAAAAAAAAAAAAAAAAAA", uuidBase64);
    }
}
