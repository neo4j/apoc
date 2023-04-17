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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SensitivePathGenerator {

    private SensitivePathGenerator() {}

    /**
     * It will return an instance of Pair<String, String> where first is the relative path
     * and other the absolute path of "etc/passwd"
     * @return
     */
    public static Pair<String, String> etcPasswd() {
        return base("/etc/passwd");
    }

    private static Pair<String, String> base(String path) {
        try {
            System.out.println("Paths.get(\"..\", System.getProperty(\"coreDir\")).toFile().getCanonicalPath() = " + Paths.get("..", System.getProperty("coreDir")).toFile().getCanonicalPath());
            Path path1 = Paths.get("").toAbsolutePath();
            System.out.println("path1 = " + path1);
            final String relativeFileName1 = IntStream.range(0, path1.getNameCount())
//            final String relativeFileName1 = IntStream.range(0, Paths.get("..", System.getProperty("coreDir")).toAbsolutePath().getNameCount())
                    .mapToObj(i -> "..")
                    .collect(Collectors.joining("/")) + path;
            final String absoluteFileName1 = Paths.get(relativeFileName1)
                    .toAbsolutePath().normalize().toString();


//            final Path dbPath = db.databaseLayout().databaseDirectory();
//            final String relativeFileName = IntStream.range(0, dbPath.getNameCount())
//                    .mapToObj(i -> "..")
//                    .collect(Collectors.joining("/")) + path;
//            final String absoluteFileName = Paths.get(relativeFileName)
//                    .toAbsolutePath().normalize().toString();
            return Pair.of(relativeFileName1, absoluteFileName1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}