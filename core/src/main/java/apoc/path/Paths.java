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
package apoc.path;

import apoc.util.collection.Iterables;
import java.util.Iterator;
import java.util.List;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * @author mh
 * @since 19.02.18
 */
public class Paths {

    @UserFunction("apoc.path.create")
    @Description("Returns a `PATH` from the given start `NODE` and `LIST<RELATIONSHIP>`.")
    public Path create(
            @Name("startNode") Node startNode, @Name(value = "rels", defaultValue = "[]") List<Relationship> rels) {
        if (startNode == null) return null;
        PathImpl.Builder builder = new PathImpl.Builder(startNode);
        if (rels != null) {
            for (Relationship rel : rels) {
                if (rel != null) {
                    builder = builder.push(rel);
                }
            }
        }
        return builder.build();
    }

    @UserFunction("apoc.path.slice")
    @Description("Returns a new `PATH` of the given length, taken from the given `PATH` at the given offset.")
    public Path slice(
            @Name("path") Path path,
            @Name(value = "offset", defaultValue = "0") long offset,
            @Name(value = "length", defaultValue = "-1") long length) {
        if (path == null) return null;
        if (offset < 0) offset = 0;
        if (length == -1) length = path.length() - offset;
        if (offset == 0 && length >= path.length()) return path;

        Iterator<Node> nodes = path.nodes().iterator();
        Iterator<Relationship> rels = path.relationships().iterator();
        for (long i = 0; i < offset && nodes.hasNext() && rels.hasNext(); i++) {
            nodes.next();
            rels.next();
        }
        if (!nodes.hasNext()) return null;

        PathImpl.Builder builder = new PathImpl.Builder(nodes.next());
        for (long i = 0; i < length && rels.hasNext(); i++) {
            builder = builder.push(rels.next());
        }
        return builder.build();
    }

    @UserFunction("apoc.path.combine")
    @Description("Combines the two given `PATH` values into one `PATH`.")
    public Path combine(@Name("path1") Path first, @Name("path2") Path second) {
        if (first == null) return second;
        if (second == null) return first;

        if (!first.endNode().equals(second.startNode()))
            throw new IllegalArgumentException(
                    "Paths don't connect on their end and start-nodes " + first + " with " + second);

        PathImpl.Builder builder = new PathImpl.Builder(first.startNode());
        for (Relationship rel : first.relationships()) builder = builder.push(rel);
        for (Relationship rel : second.relationships()) builder = builder.push(rel);
        return builder.build();
    }

    @UserFunction("apoc.path.elements")
    @Description("Converts the given `PATH` into a `LIST<NODE | RELATIONSHIP>`.")
    public List<Object> elements(@Name("path") Path path) {
        if (path == null) return null;
        return Iterables.asList((Iterable) path);
    }
}
