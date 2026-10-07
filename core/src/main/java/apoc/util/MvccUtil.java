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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Detects databases using the multiversion (MVCC) store format and provides the single, shared error used by the
 * procedures that are not supported on such databases.
 */
public class MvccUtil {

    public static final String MVCC_NOT_SUPPORTED_ERROR =
            "`%s` is not supported on a database using the multiversion (MVCC) store format.%s";

    private MvccUtil() {}

    /**
     * @return true if the given database uses a multiversion (MVCC) store format
     */
    public static boolean isMvcc(GraphDatabaseService db) {
        if (!(db instanceof GraphDatabaseAPI api)) {
            return false;
        }
        return api.getDependencyResolver()
                .resolveDependency(Database.class)
                .getStorageEngineFactory()
                .multiVersioned();
    }

    /**
     * Same as {@link #isMvcc(GraphDatabaseService)} for a database looked up by name, e.g. from the system database.
     * Unknown databases are reported as not MVCC.
     */
    public static boolean isMvcc(GraphDatabaseAPI currentDb, String databaseName) {
        if (databaseName == null) {
            return false;
        }
        try {
            return isMvcc(currentDb
                    .getDependencyResolver()
                    .resolveDependency(DatabaseManagementService.class)
                    .database(databaseName));
        } catch (DatabaseNotFoundException e) {
            return false;
        }
    }

    /**
     * @throws UnsupportedOperationException if the database uses the MVCC store format
     */
    public static void failIfMvcc(GraphDatabaseService db, String procedureName) {
        failIfMvcc(db, procedureName, null);
    }

    /**
     * @param detail optional extra explanation appended to the standard message
     * @throws UnsupportedOperationException if the database uses the MVCC store format
     */
    public static void failIfMvcc(GraphDatabaseService db, String procedureName, String detail) {
        if (isMvcc(db)) {
            throw mvccNotSupported(procedureName, detail);
        }
    }

    /**
     * Fails if the database named {@code databaseName} uses the MVCC store format.
     */
    public static void failIfMvcc(
            GraphDatabaseAPI currentDb, String databaseName, String procedureName, String detail) {
        if (isMvcc(currentDb, databaseName)) {
            throw mvccNotSupported(procedureName, detail);
        }
    }

    public static UnsupportedOperationException mvccNotSupported(String procedureName, String detail) {
        return new UnsupportedOperationException(
                String.format(MVCC_NOT_SUPPORTED_ERROR, procedureName, detail == null ? "" : " " + detail));
    }
}
