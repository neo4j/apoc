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
package apoc.lock;

import java.util.List;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

public class Lock {

    @Context
    public Transaction tx;

    @NotThreadSafe
    @Procedure(name = "apoc.lock.all", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given `NODE` and `RELATIONSHIP` values.")
    public void all(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.nodes", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given `NODE` values.")
    public void nodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.read.nodes", mode = Mode.READ)
    @Description("Acquires a read lock on the given `NODE` values.")
    public void readLockOnNodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireReadLock(node);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.rels", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given `RELATIONSHIP` values.")
    public void rels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.read.rels", mode = Mode.READ)
    @Description("Acquires a read lock on the given `RELATIONSHIP` values.")
    public void readLocksOnRels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireReadLock(rel);
        }
    }
}
