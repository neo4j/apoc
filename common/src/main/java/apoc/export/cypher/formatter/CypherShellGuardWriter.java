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
package apoc.export.cypher.formatter;

import java.io.PrintWriter;

/**
 * Guards a cypher-shell script against injected shell commands.
 * <p>
 * cypher-shell reads a script line by line and treats any line starting with ':' (e.g. :commit, :begin) as a command
 * rather than as Cypher. Identifiers can contain line breaks and quoted identifiers have no escape for them, so a
 * crafted label, relationship type or property key could otherwise put such a line into the output and break out of
 * the export's transactions. Everything written through this writer is checked; the exporter's own commands are
 * written with {@link #printDirective(String)}, which bypasses the check.
 */
public class CypherShellGuardWriter extends PrintWriter {

    private boolean atLineStart = true;

    public CypherShellGuardWriter(PrintWriter delegate) {
        super(delegate);
    }

    /**
     * Writes the exporter's own shell command text, e.g. <code>:commit</code>. When the writer is not guarded the text
     * is simply written.
     */
    public static void append(PrintWriter out, String text) {
        if (out instanceof CypherShellGuardWriter guarded) {
            guarded.printDirective(text);
        } else {
            out.append(text);
        }
    }

    private void printDirective(String text) {
        if (text.isEmpty()) return;
        super.write(text, 0, text.length());
        atLineStart = isLineTerminator(text.charAt(text.length() - 1));
    }

    @Override
    public void write(int c) {
        check(c);
        super.write(c);
    }

    @Override
    public void write(char[] buf, int off, int len) {
        for (int i = off; i < off + len; i++) check(buf[i]);
        super.write(buf, off, len);
    }

    @Override
    public void write(String s, int off, int len) {
        for (int i = off; i < off + len; i++) check(s.charAt(i));
        super.write(s, off, len);
    }

    private void check(int c) {
        if (isLineTerminator(c)) {
            atLineStart = true;
        } else if (atLineStart && !Character.isWhitespace(c)) {
            if (c == ':') {
                throw new IllegalArgumentException(
                        "Cannot export in cypher-shell format: a label, relationship type, property key or name "
                                + "contains a line break that would produce a line starting with ':' in the script, "
                                + "which cypher-shell would interpret as a command.");
            }
            atLineStart = false;
        }
    }

    private static boolean isLineTerminator(int c) {
        return c == '\n' || c == '\r' || c == '\u0085' || c == '\u2028' || c == '\u2029';
    }
}
