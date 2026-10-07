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

import static apoc.util.MvccUtil.failIfMvcc;

import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

public class Lock {

    private static final String MVCC_DETAIL =
            "Entity locks are not taken under MVCC, so the procedure cannot serialise concurrent writers.";

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @NotThreadSafe
    @Procedure(name = "apoc.lock.all", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given `NODE` and `RELATIONSHIP` values.")
    public void all(
            @Name(value = "nodes", description = "The list of nodes to acquire a write lock on.") List<Node> nodes,
            @Name(value = "rels", description = "The list of relationships to acquire a write lock on.")
                    List<Relationship> rels) {
        failIfMvcc(db, "apoc.lock.all", MVCC_DETAIL);
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
    public void nodes(
            @Name(value = "nodes", description = "The list of nodes to acquire a write lock on.") List<Node> nodes) {
        failIfMvcc(db, "apoc.lock.nodes", MVCC_DETAIL);
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.read.nodes", mode = Mode.READ)
    @Description("Acquires a read lock on the given `NODE` values.")
    public void readLockOnNodes(
            @Name(value = "nodes", description = "The list of nodes to acquire a read lock on.") List<Node> nodes) {
        failIfMvcc(db, "apoc.lock.read.nodes", MVCC_DETAIL);
        for (Node node : nodes) {
            tx.acquireReadLock(node);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.rels", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given `RELATIONSHIP` values.")
    public void rels(
            @Name(value = "rels", description = "The list of relationships to acquire a write lock on.")
                    List<Relationship> rels) {
        failIfMvcc(db, "apoc.lock.rels", MVCC_DETAIL);
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.lock.read.rels", mode = Mode.READ)
    @Description("Acquires a read lock on the given `RELATIONSHIP` values.")
    public void readLocksOnRels(
            @Name(value = "rels", description = "The list of relationships to acquire a read lock on.")
                    List<Relationship> rels) {
        failIfMvcc(db, "apoc.lock.read.rels", MVCC_DETAIL);
        for (Relationship rel : rels) {
            tx.acquireReadLock(rel);
        }
    }
}
