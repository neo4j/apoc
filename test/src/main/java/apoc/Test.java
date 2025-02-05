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
package apoc;

import apoc.export.util.CountingReader;
import apoc.util.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;

import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;

public class Test {

    public static void main(String[] args) {
        String pid = Long.toString( ProcessHandle.current().pid() );

        String filename = "simple.graphml";
        String file = "file://./test/" + filename;
        Object urlOrBinaryFile = file;

        URLAccessChecker urlAccessChecker = new URLAccessChecker() {
            @Override
            public URL checkURL(URL url) throws URLAccessValidationError {
                return url;
            }
        };
        ApocConfig apocConfig = new ApocConfig(null);
        apocConfig.setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        apocConfig.setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig.setProperty(ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, true);
        apocConfig.setProperty("server.directories.import", "/Users/hannes/Desktop/dev/apoc.nosync");
        try (CountingReader reader = FileUtils.readerFor(urlOrBinaryFile, "NONE", urlAccessChecker)) {

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            inputFactory.setProperty("javax.xml.stream.isCoalescing", true);
            inputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
            inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            XMLEventReader xmlReader = inputFactory.createXMLEventReader(reader);

            try {

                while (xmlReader.hasNext()) {
                    XMLEvent event;
                    try {
                        event = (XMLEvent) xmlReader.next();
                        if(event.isStartDocument()) {
                          System.out.println(event.asStartElement().getName().getLocalPart());
                        }
                    } catch (Exception e) {
                        // in case of unicode invalid chars we skip the event, or we exit in case of EOF
                        if (e.getMessage().contains("Unexpected EOF")) {
                            break;
                        } else if (e.getMessage().contains("DOCTYPE")) {
                            throw e;
                        }
                        continue;
                    }
                }
            } catch (Exception e) {
                throw e;
            } finally {
                xmlReader.close();
            }
            xmlReader = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println( "read the file, waiting ..." );

        try {
            ProcessBuilder builder = new ProcessBuilder();
            boolean checkForHandle = true;

            // Command to execute (Change based on OS)
            if ( System.getProperty("os.name").toLowerCase().contains("win") ) {
                // Windows
                // skip for now
                checkForHandle = false;
            } else {
                // macOS/Linux
                // lsof -p $pid | grep $filename
                builder.command("bash", "-c", "lsof -p %s | grep %s".formatted( pid, filename ));
            }
            long startTime = System.currentTimeMillis();
            while ( checkForHandle ) {
                Process process = builder.start(); // Start process
                // Read and print process output
                try ( BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream()))) {
                    var lines = reader.lines().toList();
                    if(lines.size() == 0) {
                        checkForHandle = false;
                    } else {
                        System.out.println(lines.getFirst());
                        //System.out.print( "." );
                    }
                }
                Thread.currentThread().sleep( 1000 );
            }
            long wait = (System.currentTimeMillis() - startTime) / 1000;
            System.out.println( "Handle closed after " + wait + " seconds");
        } catch ( IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
