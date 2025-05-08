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
package apoc.export.csv;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.CompressionAlgo.DEFLATE;
import static apoc.util.CompressionAlgo.GZIP;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assertError;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import apoc.HelperProcedures;
import apoc.csv.CsvTestUtil;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.util.CompressionAlgo;
import apoc.util.CompressionConfig;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

/**
 * @author mh
 * @since 22.05.16
 */
@EnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
public class ExportCsvTest {

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n"
            + "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\",\"\"User1\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"age\"\":42,\"\"male\"\":true,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n"
            + "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n"
            + "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"%n");
    private static final String EXPECTED_QUERY =
            String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n"
                    + "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User1\"\",\"\"User\"\"]\"%n"
                    + "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n"
                    + "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"%n");
    private static final String EXPECTED_QUERY_WITHOUT_QUOTES = String.format(
            "u.age,u.name,u.male,u.kids,labels(u)%n" + "42,foo,true,[\"a\",\"b\",\"c\"],[\"User1\",\"User\"]%n"
                    + "42,bar,,,[\"User\"]%n"
                    + "12,,,,[\"User\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_NONE = String.format(
            "a.name,a.city,a.street,labels(a)%n" + "Andrea,Milano,Via Garibaldi, 7,[\"Address1\",\"Address\"]%n"
                    + "Bar Sport,,,[\"Address\"]%n"
                    + ",,via Benni,[\"Address\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_ALWAYS =
            String.format("\"a.name\",\"a.city\",\"a.street\",\"labels(a)\"%n"
                    + "\"Andrea\",\"Milano\",\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n"
                    + "\"Bar Sport\",\"\",\"\",\"[\"\"Address\"\"]\"%n"
                    + "\"\",\"\",\"via Benni\",\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED_QUERY_QUOTES_NEEDED = String.format("a.name,a.city,a.street,labels(a)%n"
            + "Andrea,Milano,\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n"
            + "Bar Sport,,,\"[\"\"Address\"\"]\"%n"
            + ",,via Benni,\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED_ALL_ALWAYS =
            """
            "_id","_labels","age","city","kids","male","name","street","_start","_end","_type","id","value1","value2"
            "0",":User:User1","42","","[""a"",""b"",""c""]","true","foo","",,,,,,
            "1",":User","42","","","","bar","",,,,,,
            "2",":User","12","","","","","",,,,,,
            "3",":Address:Address1","","Milano","","","Andrea","Via Garibaldi, 7",,,,,,
            "4",":Address","","","","","Bar Sport","",,,,,,
            "5",":Address","","","","","","via Benni",,,,,,
            "6",":ESCAPING","1","","","","I am ""groot"", and more ","",,,,,,
            "7",":ESCAPING","2","","",""," ","",,,,,,
            "8",":ESCAPING","3","","","","","",,,,,,
            "9",":ESCAPING","4","","","","","",,,,,,
            ,,,,,,,,"0","1","KNOWS","","",""
            ,,,,,,,,"3","4","NEXT_DELIVERY","","",""
            ,,,,,,,,"8","9","REL","0"," ",""
            """;
    private static final String EXPECTED_ALL_ALWAYS_2 =
            """
            "_id","_labels","age","city","kids","male","name","street","_start","_end","_type","id","value1","value2"
            "0",":User:User1","42","","[""a"",""b"",""c""]","true","foo","",,,,,,
            "1",":User","42","","","","bar","",,,,,,
            "2",":User","12","","","","","",,,,,,
            "3",":Address:Address1","","Milano","","","Andrea","Via Garibaldi, 7",,,,,,
            "4",":Address","","","","","Bar Sport","",,,,,,
            "5",":Address","","","","","","via Benni",,,,,,
            "6",":ESCAPING","1","","","","I am ""groot"", and more ","",,,,,,
            "7",":ESCAPING","2","","",""," ","",,,,,,
            "8",":ESCAPING","3","","","","","",,,,,,
            "9",":ESCAPING","4","","","","","",,,,,,
            ,,,,,,,,"0","1","KNOWS","","",""
            ,,,,,,,,"8","9","REL","0"," ",""
            ,,,,,,,,"3","4","NEXT_DELIVERY","","",""
            """;
    private static final List<String> EXPECTED_ALL_ALWAYS_USER = List.of(
            """
            ":ID","name","age:long",":LABEL"
            "1","bar","42","User"
            "2","","12","User"
            """,
            """
            ":ID","age:long","name",":LABEL"
            "1","42","bar","User"
            "2","12","","User"
            """);
    private static final List<String> EXPECTED_ALL_ALWAYS_USER1 = List.of(
            """
            ":ID","name","age:long","male:boolean","kids",":LABEL"
            "0","foo","42","true","[""a"",""b"",""c""]","User1;User"
            """,
            """
            ":ID","age:long","kids","male:boolean","name",":LABEL"
            "0","42","[""a"",""b"",""c""]","true","foo","User1;User"
            """);
    private static final String EXPECTED_ALL_ALWAYS_ADDRESS =
            """
            ":ID","name","street",":LABEL"
            "4","Bar Sport","","Address"
            "5","","via Benni","Address"
            """;
    private static final String EXPECTED_ALL_ALWAYS_ADDRESS1 =
            """
            ":ID","name","city","street",":LABEL"
            "3","Andrea","Milano","Via Garibaldi, 7","Address1;Address"
            """;
    private static final List<String> EXPECTED_ALL_ALWAYS_ESCAPING = List.of(
            """
            ":ID","name","age:long",":LABEL"
            "6","I am ""groot"", and more ","1","ESCAPING"
            "7"," ","2","ESCAPING"
            "8","","3","ESCAPING"
            "9","","4","ESCAPING"
            """,
            """
            ":ID","age:long","name",":LABEL"
            "6","1","I am ""groot"", and more ","ESCAPING"
            "7","2"," ","ESCAPING"
            "8","3","","ESCAPING"
            "9","4","","ESCAPING"
            """);
    private static final String EXPECTED_ALL_ALWAYS_KNOWS =
            """
            ":START_ID",":END_ID",":TYPE"
            "0","1","KNOWS"
            """;
    private static final String EXPECTED_ALL_ALWAYS_NEXT_DELIVERY =
            """
            ":START_ID",":END_ID",":TYPE"
            "3","4","NEXT_DELIVERY"
            """;
    private static final List<String> EXPECTED_ALL_ALWAYS_REL = List.of(
            """
            ":START_ID",":END_ID",":TYPE","value2","value1","id:long"
            "8","9","REL",""," ","0"
            """,
            """
            ":START_ID",":END_ID",":TYPE","id:long","value1","value2"
            "8","9","REL","0"," ",""
            """);

    private static final String EXP_SAMPLE =
            """
            "_id","_labels","address","age","baz","city","foo","kids","last:Name","male","name","street","_start","_end","_type","id","one","three","value1","value2"
            "0",":User:User1","","42","","","","[""a"",""b"",""c""]","","true","foo","",,,,,,,,
            "1",":User","","42","","","","","","","bar","",,,,,,,,
            "2",":User","","12","","","","","","","","",,,,,,,,
            "3",":Address:Address1","","","","Milano","","","","","Andrea","Via Garibaldi, 7",,,,,,,,
            "4",":Address","","","","","","","","","Bar Sport","",,,,,,,,
            "5",":Address","","","","","","","","","","via Benni",,,,,,,,
            "6",":ESCAPING","","1","","","","","","","I am ""groot"", and more ","",,,,,,,,
            "7",":ESCAPING","","2","","","","","",""," ","",,,,,,,,
            "8",":ESCAPING","","3","","","","","","","","",,,,,,,,
            "9",":ESCAPING","","4","","","","","","","","",,,,,,,,
            "10",":Sample:User","","","","","","","Galilei","","","",,,,,,,,
            "11",":Sample:User","Universe","","","","","","","","","",,,,,,,,
            "12",":Sample:User","","","","","bar","","","","","",,,,,,,,
            "13",":Sample:User","","","baa","","true","","","","","",,,,,,,,
            ,,,,,,,,,,,,"0","1","KNOWS","","","","",""
            ,,,,,,,,,,,,"3","4","NEXT_DELIVERY","","","","",""
            ,,,,,,,,,,,,"8","9","REL","0","",""," ",""
            ,,,,,,,,,,,,"12","13","KNOWS","","two","four","",""
            """;
    private static final String EXPECTED_ALL_NONE =
            """
            _id,_labels,age,city,kids,male,name,street,_start,_end,_type,id,value1,value2
            0,:User:User1,42,,["a","b","c"],true,foo,,,,,,,
            1,:User,42,,,,bar,,,,,,,
            2,:User,12,,,,,,,,,,,
            3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,,,,
            4,:Address,,,,,Bar Sport,,,,,,,
            5,:Address,,,,,,via Benni,,,,,,
            6,:ESCAPING,1,,,,I am "groot", and more ,,,,,,,
            7,:ESCAPING,2,,,, ,,,,,,,
            8,:ESCAPING,3,,,,,,,,,,,
            9,:ESCAPING,4,,,,,,,,,,,
            ,,,,,,,,0,1,KNOWS,,,
            ,,,,,,,,8,9,REL,0, ,
            ,,,,,,,,3,4,NEXT_DELIVERY,,,
            """;

    private static final String EXPECTED_ALL_NONE_2 =
            """
            _id,_labels,age,city,kids,male,name,street,_start,_end,_type,id,value1,value2
            0,:User:User1,42,,["a","b","c"],true,foo,,,,,,,
            1,:User,42,,,,bar,,,,,,,
            2,:User,12,,,,,,,,,,,
            3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,,,,
            4,:Address,,,,,Bar Sport,,,,,,,
            5,:Address,,,,,,via Benni,,,,,,
            6,:ESCAPING,1,,,,I am "groot", and more ,,,,,,,
            7,:ESCAPING,2,,,, ,,,,,,,
            8,:ESCAPING,3,,,,,,,,,,,
            9,:ESCAPING,4,,,,,,,,,,,
            ,,,,,,,,0,1,KNOWS,,,
            ,,,,,,,,3,4,NEXT_DELIVERY,,,
            ,,,,,,,,8,9,REL,0, ,
            """;

    private static final List<String> EXPECTED_ALL_NONE_USER1 = List.of(
            """
            :ID,name,age:long,male:boolean,kids,:LABEL
            0,foo,42,true,["a","b","c"],User1;User
            """,
            """
            :ID,age:long,kids,male:boolean,name,:LABEL
            0,42,["a","b","c"],true,foo,User1;User
            """);
    private static final String EXPECTED_ALL_NONE_ADDRESS1 =
            """
            :ID,name,city,street,:LABEL
            3,Andrea,Milano,Via Garibaldi, 7,Address1;Address
            """;
    private static final List<String> EXPECTED_ALL_NONE_ESCAPING = List.of(
            """
            :ID,name,age:long,:LABEL
            6,I am "groot", and more ,1,ESCAPING
            7, ,2,ESCAPING
            8,,3,ESCAPING
            9,,4,ESCAPING
            """,
            """
            :ID,age:long,name,:LABEL
            6,1,I am "groot", and more ,ESCAPING
            7,2, ,ESCAPING
            8,3,,ESCAPING
            9,4,,ESCAPING
            """);

    private static final String EXPECTED_ALL_IF_NEEDED_RELS =
            """
            ,,,,,,,,0,1,KNOWS,,,
            ,,,,,,,,3,4,NEXT_DELIVERY,,,
            ,,,,,,,,8,9,REL,0, ,""
            """;
    private static final String EXPECTED_ALL_IF_NEEDED =
            """
            _id,_labels,age,city,kids,male,name,street,_start,_end,_type,id,value1,value2
            0,:User:User1,42,,"[""a"",""b"",""c""]",true,foo,,,,,,,
            1,:User,42,,,,bar,,,,,,,
            2,:User,12,,,,,,,,,,,
            3,:Address:Address1,,Milano,,,Andrea,"Via Garibaldi, 7",,,,,,
            4,:Address,,,,,Bar Sport,,,,,,,
            5,:Address,,,,,,via Benni,,,,,,
            6,:ESCAPING,1,,,,"I am ""groot"", and more ",,,,,,,
            7,:ESCAPING,2,,,, ,,,,,,,
            8,:ESCAPING,3,,,,,,,,,,,
            9,:ESCAPING,4,,,,,,,,,,,
            ,,,,,,,,0,1,KNOWS,,,
            ,,,,,,,,3,4,NEXT_DELIVERY,,,
            ,,,,,,,,8,9,REL,0, ,
            """;

    private static final List<String> EXPECTED_ALL_IF_NEEDED_USER = List.of(
            """
            :ID,name,age:long,:LABEL
            1,bar,42,User
            2,,12,User
            """,
            """
            :ID,age:long,name,:LABEL
            1,42,bar,User
            2,12,,User
            """);
    private static final List<String> EXPECTED_ALL_IF_NEEDED_USER1 = List.of(
            """
            :ID,name,age:long,male:boolean,kids,:LABEL
            0,foo,42,true,"[""a"",""b"",""c""]",User1;User
            """,
            """
            :ID,age:long,kids,male:boolean,name,:LABEL
            0,42,"[""a"",""b"",""c""]",true,foo,User1;User
            """);
    private static final String EXPECTED_ALL_IF_NEEDED_ADDRESS =
            """
            :ID,name,street,:LABEL
            4,Bar Sport,,Address
            5,,via Benni,Address
            """;
    private static final String EXPECTED_ALL_IF_NEEDED_ADDRESS1 =
            """
            :ID,name,city,street,:LABEL
            3,Andrea,Milano,"Via Garibaldi, 7",Address1;Address
            """;
    private static final List<String> EXPECTED_ALL_IF_NEEDED_ESCAPING = List.of(
            """
            :ID,name,age:long,:LABEL
            6,"I am ""groot"", and more ",1,ESCAPING
            7, ,2,ESCAPING
            8,,3,ESCAPING
            9,,4,ESCAPING
            """,
            """
            :ID,age:long,name,:LABEL
            6,1,"I am ""groot"", and more ",ESCAPING
            7,2, ,ESCAPING
            8,3,,ESCAPING
            9,4,,ESCAPING
            """);
    private static final String EXPECTED_ALL_IF_NEEDED_KNOWS =
            """
            :START_ID,:END_ID,:TYPE
            0,1,KNOWS
            """;
    private static final String EXPECTED_ALL_IF_NEEDED_NEXT_DELIVERY =
            """
            :START_ID,:END_ID,:TYPE
            3,4,NEXT_DELIVERY
            """;
    private static final List<String> EXPECTED_ALL_IF_NEEDED_REL = List.of(
            """
            :START_ID,:END_ID,:TYPE,value2,value1,id:long
            8,9,REL,, ,0
            """,
            """
            :START_ID,:END_ID,:TYPE,id:long,value1,value2
            8,9,REL,0, ,
            """);
    private static final String EXPECTED_QUERY_DIFFERENTIATE_NULLS_NONE =
            """
            age,name
            1,I am "groot", and more\s
            2,\s
            3,
            4,
            """;
    private static final String EXPECTED_QUERY_DIFFERENTIATE_NULLS_IF_NEEDED =
            """
            age,name
            1,"I am ""groot"", and more "
            2,\s
            3,""
            4,
            """;
    private static final String EXPECTED_QUERY_DONT_DIFFERENTIATE_NULLS_IF_NEEDED =
            """
            age,name
            1,"I am ""groot"", and more "
            2,\s
            3,
            4,
            """;
    private static final String EXPECTED_QUERY_DIFFERENTIATE_NULLS_ALWAYS =
            """
            "age","name"
            "1","I am ""groot"", and more "
            "2"," "
            "3",""
            "4",
            """;
    private static final String EXPECTED_QUERY_DONT_DIFFERENTIATE_NULLS_ALWAYS =
            """
            "age","name"
            "1","I am ""groot"", and more "
            "2"," "
            "3",""
            "4",""
            """;
    private static final String EXPECTED_DATA_DIFFERENTIATE_NULLS_NONE =
            """
            _id,_labels,age,name,_start,_end,_type
            6,:ESCAPING,1,I am "groot", and more ,,,
            7,:ESCAPING,2, ,,,
            8,:ESCAPING,3,,,,
            9,:ESCAPING,4,,,,
            """;
    private static final String EXPECTED_DATA_DIFFERENTIATE_NULLS_IF_NEEDED =
            """
            _id,_labels,age,name,_start,_end,_type
            6,:ESCAPING,1,"I am ""groot"", and more ",,,
            7,:ESCAPING,2, ,,,
            8,:ESCAPING,3,"",,,
            9,:ESCAPING,4,,,,
            """;
    private static final String EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_IF_NEEDED =
            """
            _id,_labels,age,name,_start,_end,_type
            6,:ESCAPING,1,"I am ""groot"", and more ",,,
            7,:ESCAPING,2, ,,,
            8,:ESCAPING,3,,,,
            9,:ESCAPING,4,,,,
            """;
    private static final String EXPECTED_DATA_DIFFERENTIATE_NULLS_ALWAYS =
            """
            "_id","_labels","age","name","_start","_end","_type"
            "6",":ESCAPING","1","I am ""groot"", and more ",,,
            "7",":ESCAPING","2"," ",,,
            "8",":ESCAPING","3","",,,
            "9",":ESCAPING","4",,,,
            """;
    private static final String EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_ALWAYS =
            """
            "_id","_labels","age","name","_start","_end","_type"
            "6",":ESCAPING","1","I am ""groot"", and more ",,,
            "7",":ESCAPING","2"," ",,,
            "8",":ESCAPING","3","",,,
            "9",":ESCAPING","4","",,,
            """;
    private static final String EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED =
            """
                    _id,_labels,age,city,kids,male,name,street,_start,_end,_type,id,value1,value2
                    0,:User:User1,42,,"[""a"",""b"",""c""]",true,foo,,,,,,,
                    1,:User,42,,,,bar,,,,,,,
                    2,:User,12,,,,,,,,,,,
                    3,:Address:Address1,,Milano,,,Andrea,"Via Garibaldi, 7",,,,,,
                    4,:Address,,,,,Bar Sport,,,,,,,
                    5,:Address,,,,,,via Benni,,,,,,
                    6,:ESCAPING,1,,,,"I am ""groot"", and more ",,,,,,,
                    7,:ESCAPING,2,,,, ,,,,,,,
                    8,:ESCAPING,3,,,,"",,,,,,,
                    9,:ESCAPING,4,,,,,,,,,,,
                    """
                    + EXPECTED_ALL_IF_NEEDED_RELS;

    private static final List<String> EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_USER = List.of(
            """
            :ID,name,age:long,:LABEL
            1,bar,42,User
            2,,12,User
            """,
            """
            :ID,age:long,name,:LABEL
            1,42,bar,User
            2,12,,User
            """);

    private static final String EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_ADDRESS =
            """
            :ID,name,street,:LABEL
            4,Bar Sport,,Address
            5,,via Benni,Address
            """;
    private static final List<String> EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_ESCAPING = List.of(
            """
            :ID,name,age:long,:LABEL
            6,"I am ""groot"", and more ",1,ESCAPING
            7, ,2,ESCAPING
            8,"",3,ESCAPING
            9,,4,ESCAPING
            """,
            """
            :ID,age:long,name,:LABEL
            6,1,"I am ""groot"", and more ",ESCAPING
            7,2, ,ESCAPING
            8,3,"",ESCAPING
            9,4,,ESCAPING
            """);
    private static final List<String> EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_REL = List.of(
            """
            :START_ID,:END_ID,:TYPE,value2,value1,id:long
            8,9,REL,"", ,0
            """,
            """
            :START_ID,:END_ID,:TYPE,id:long,value1,value2
            8,9,REL,0, ,""
            """);

    private static final String EXPECTED_ALL_DIFFERENTIATE_NULLS_ALWAYS =
            """
            "_id","_labels","age","city","kids","male","name","street","_start","_end","_type","id","value1","value2"
            "0",":User:User1","42",,"[""a"",""b"",""c""]","true","foo",,,,,,,
            "1",":User","42",,,,"bar",,,,,,,
            "2",":User","12",,,,,,,,,,,
            "3",":Address:Address1",,"Milano",,,"Andrea","Via Garibaldi, 7",,,,,,
            "4",":Address",,,,,"Bar Sport",,,,,,,
            "5",":Address",,,,,,"via Benni",,,,,,
            "6",":ESCAPING","1",,,,"I am ""groot"", and more ",,,,,,,
            "7",":ESCAPING","2",,,," ",,,,,,,
            "8",":ESCAPING","3",,,,"",,,,,,,
            "9",":ESCAPING","4",,,,,,,,,,,
            ,,,,,,,,"0","1","KNOWS",,,
            ,,,,,,,,"3","4","NEXT_DELIVERY",,,
            ,,,,,,,,"8","9","REL","0"," ",""
                    """;

    @Inject
    TestDirectory testDirectory;

    @Inject
    GraphDatabaseService db;

    private Path directory;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        this.directory = testDirectory.directory("import");
        builder.setConfigRaw(Map.of(
                "server.directories.import",
                directory.toAbsolutePath().toString(),
                "internal.dbms.cypher.enable_experimental_versions",
                "true"));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(
                db, ExportCSV.class, Graphs.class, Meta.class, ImportCsv.class, HelperProcedures.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        db.executeTransactionally(
                "CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally(
                "CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
        db.executeTransactionally(
                """
        CREATE
            (:ESCAPING {age: 1, name: 'I am "groot", and more '}),
            (:ESCAPING {age: 2, name: ' '}),
            (:ESCAPING {age: 3, name: ''})-[:REL {id: 0, value1: ' ', value2: ""}]->(:ESCAPING {age: 4})
        """);
    }

    private String readFile(String fileName) throws Exception {
        return readFile(fileName, UTF_8, CompressionAlgo.NONE);
    }

    private String readFile(String fileName, Charset charset, CompressionAlgo compression) throws Exception {
        final var path = directory.resolve(fileName);
        if (compression.isNone()) return Files.readString(path, charset);
        else return compression.decompress(Files.readAllBytes(path), charset);
    }

    @Test
    public void testExportInvalidQuoteValue() {
        assertThatThrownBy(() -> {
                    String fileName = "all.csv";
                    testCall(
                            db,
                            "CALL apoc.export.csv.all($file,{quotes: 'Invalid'})",
                            map("file", fileName),
                            (r) -> assertResults(fileName, r, "database"));
                })
                .hasMessage(
                        "Failed to invoke procedure `apoc.export.csv.all`: Caused by: java.lang.RuntimeException: The string value of the field quote is not valid");
    }

    @Test
    public void textExportWithTypes() {
        db.executeTransactionally(
                "CREATE (n:TestNode) SET n = {valFloat:toFloat(123), name:'NodeName', valInt:5, dateVal: date('2024-11-01')};");
        testCall(
                db,
                """
                            CALL apoc.graph.fromCypher("MATCH (n:TestNode) RETURN n", {}, 'TestNode.csv',{}) YIELD graph
                            CALL apoc.export.csv.graph(graph, null, {stream:true, useTypes:true}) YIELD properties, rows, data
                            return properties, rows, data;
                            """,
                (r) -> {
                    String data = (String) r.get("data");
                    // FLOAT value
                    assertTrue(data.contains("valFloat:float"));
                    assertTrue(data.contains("123.0"));
                    // INT value
                    assertTrue(data.contains("valInt:int"));
                    assertTrue(data.contains("5"));
                    // STRING value and unknown types to csv export
                    assertTrue(data.contains("name"));
                    assertTrue(data.contains("NodeName"));
                    assertTrue(data.contains("dateVal"));
                    assertTrue(data.contains("2024-11-01"));
                });

        final String deleteQuery = "MATCH (n:TestNode) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testExportAllCsvCompressed() throws Exception {
        final CompressionAlgo compressionAlgo = DEFLATE;
        String fileName = "all.csv.zz";
        testCall(
                db,
                "CALL apoc.export.csv.all($file, $config)",
                map("file", fileName, "config", map("compression", compressionAlgo.name())),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ALL_ALWAYS, readFile(fileName, UTF_8, compressionAlgo));
    }

    @Test
    public void testConsistentQuotingAlways() throws Exception {
        // All in one file
        String fileName1 = "allOneFileAlways.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: false})",
                map("file", fileName1),
                (r) -> assertResults(fileName1, r, "database"));
        assertEquals(EXPECTED_ALL_ALWAYS, readFile(fileName1));

        // In separate files
        String fileNameStart = "allBulkImportAlways";
        String fileName2 = fileNameStart + ".csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: true})",
                map("file", fileName2),
                (r) -> assertResults(fileName2, r, "database"));
        assertThat(readFile(fileNameStart + ".nodes.User.csv")).isIn(EXPECTED_ALL_ALWAYS_USER);
        assertThat(readFile(fileNameStart + ".nodes.User1.User.csv")).isIn(EXPECTED_ALL_ALWAYS_USER1);
        assertEquals(EXPECTED_ALL_ALWAYS_ADDRESS, readFile(fileNameStart + ".nodes.Address.csv"));
        assertEquals(EXPECTED_ALL_ALWAYS_ADDRESS1, readFile(fileNameStart + ".nodes.Address1.Address.csv"));
        assertThat(readFile(fileNameStart + ".nodes.ESCAPING.csv")).isIn(EXPECTED_ALL_ALWAYS_ESCAPING);
        assertEquals(EXPECTED_ALL_ALWAYS_KNOWS, readFile(fileNameStart + ".relationships.KNOWS.csv"));
        assertEquals(EXPECTED_ALL_ALWAYS_NEXT_DELIVERY, readFile(fileNameStart + ".relationships.NEXT_DELIVERY.csv"));
        assertThat(readFile(fileNameStart + ".relationships.REL.csv")).isIn(EXPECTED_ALL_ALWAYS_REL);

        // Streaming
        testCall(db, "CALL apoc.export.csv.all(null,{stream: true})", (r) -> {
            String data = (String) r.get("data");
            assertEquals(EXPECTED_ALL_ALWAYS, data);
        });
    }

    @Test
    public void testConsistentQuotingIfNeeded() throws Exception {
        // All in one file
        String fileName1 = "allOneFileIfNeeded.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: false, quotes: 'ifNeeded'})",
                map("file", fileName1),
                (r) -> assertResults(fileName1, r, "database"));
        assertEquals(EXPECTED_ALL_IF_NEEDED, readFile(fileName1));

        // In separate files
        String fileNameStart = "allBulkImportIfNeeded";
        String fileName2 = fileNameStart + ".csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: true, quotes: 'ifNeeded'})",
                map("file", fileName2),
                (r) -> assertResults(fileName2, r, "database"));
        assertThat(readFile(fileNameStart + ".nodes.User.csv")).isIn(EXPECTED_ALL_IF_NEEDED_USER);
        assertThat(readFile(fileNameStart + ".nodes.User1.User.csv")).isIn(EXPECTED_ALL_IF_NEEDED_USER1);
        assertEquals(EXPECTED_ALL_IF_NEEDED_ADDRESS, readFile(fileNameStart + ".nodes.Address.csv"));
        assertEquals(EXPECTED_ALL_IF_NEEDED_ADDRESS1, readFile(fileNameStart + ".nodes.Address1.Address.csv"));
        assertThat(readFile(fileNameStart + ".nodes.ESCAPING.csv")).isIn(EXPECTED_ALL_IF_NEEDED_ESCAPING);
        assertEquals(EXPECTED_ALL_IF_NEEDED_KNOWS, readFile(fileNameStart + ".relationships.KNOWS.csv"));
        assertEquals(
                EXPECTED_ALL_IF_NEEDED_NEXT_DELIVERY, readFile(fileNameStart + ".relationships.NEXT_DELIVERY.csv"));
        assertThat(readFile(fileNameStart + ".relationships.REL.csv")).isIn(EXPECTED_ALL_IF_NEEDED_REL);

        // Streaming
        testCall(db, "CALL apoc.export.csv.all(null,{stream: true, quotes: 'ifNeeded'})", (r) -> {
            String data = (String) r.get("data");
            assertEquals(EXPECTED_ALL_IF_NEEDED, data);
        });
    }

    @Test
    public void testConsistentQuotingIfNeededDifferentiateNulls() throws Exception {
        // All in one file
        String fileName1 = "allOneFileIfNeeded.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: false, quotes: 'ifNeeded', differentiateNulls: true})",
                map("file", fileName1),
                (r) -> assertResults(fileName1, r, "database"));
        assertEquals(EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED, readFile(fileName1));

        // In separate files
        String fileNameStart = "allBulkImportIfNeeded";
        String fileName2 = fileNameStart + ".csv";

        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: true, quotes: 'ifNeeded', differentiateNulls: true})",
                map("file", fileName2),
                (r) -> assertResults(fileName2, r, "database"));
        assertThat(readFile(fileNameStart + ".nodes.User.csv")).isIn(EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_USER);
        assertThat(readFile(fileNameStart + ".nodes.User1.User.csv")).isIn(EXPECTED_ALL_IF_NEEDED_USER1);
        assertEquals(
                EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_ADDRESS, readFile(fileNameStart + ".nodes.Address.csv"));
        assertEquals(EXPECTED_ALL_IF_NEEDED_ADDRESS1, readFile(fileNameStart + ".nodes.Address1.Address.csv"));
        assertThat(readFile(fileNameStart + ".nodes.ESCAPING.csv"))
                .isIn(EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_ESCAPING);
        assertEquals(EXPECTED_ALL_IF_NEEDED_KNOWS, readFile(fileNameStart + ".relationships.KNOWS.csv"));
        assertEquals(
                EXPECTED_ALL_IF_NEEDED_NEXT_DELIVERY, readFile(fileNameStart + ".relationships.NEXT_DELIVERY.csv"));
        assertThat(readFile(fileNameStart + ".relationships.REL.csv"))
                .isIn(EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED_REL);

        // Streaming
        testCall(
                db,
                "CALL apoc.export.csv.all(null,{stream: true, quotes: 'ifNeeded', differentiateNulls: true})",
                (r) -> {
                    String data = (String) r.get("data");
                    assertEquals(EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED, data);
                });
    }

    @Test
    public void testConsistentQuotingNone() throws Exception {
        // All in one file
        String fileName1 = "allOneFileNone.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: false, quotes: 'none'})",
                map("file", fileName1),
                (r) -> assertResults(fileName1, r, "database"));
        assertEquals(EXPECTED_ALL_NONE_2, readFile(fileName1));

        // In separate files
        String fileNameStart = "allBulkImportIfNone";
        String fileName2 = fileNameStart + ".csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: true, quotes: 'none'})",
                map("file", fileName2),
                (r) -> assertResults(fileName2, r, "database"));
        assertThat(readFile(fileNameStart + ".nodes.User.csv")).isIn(EXPECTED_ALL_IF_NEEDED_USER);
        assertThat(readFile(fileNameStart + ".nodes.User1.User.csv")).isIn(EXPECTED_ALL_NONE_USER1);
        assertEquals(EXPECTED_ALL_IF_NEEDED_ADDRESS, readFile(fileNameStart + ".nodes.Address.csv"));
        assertEquals(EXPECTED_ALL_NONE_ADDRESS1, readFile(fileNameStart + ".nodes.Address1.Address.csv"));
        assertThat(readFile(fileNameStart + ".nodes.ESCAPING.csv")).isIn(EXPECTED_ALL_NONE_ESCAPING);
        assertEquals(EXPECTED_ALL_IF_NEEDED_KNOWS, readFile(fileNameStart + ".relationships.KNOWS.csv"));
        assertEquals(
                EXPECTED_ALL_IF_NEEDED_NEXT_DELIVERY, readFile(fileNameStart + ".relationships.NEXT_DELIVERY.csv"));
        assertThat(readFile(fileNameStart + ".relationships.REL.csv")).isIn(EXPECTED_ALL_IF_NEEDED_REL);

        // Streaming
        testCall(db, "CALL apoc.export.csv.all(null,{stream: true, quotes: 'none'})", (r) -> {
            String data = (String) r.get("data");
            assertEquals(EXPECTED_ALL_NONE_2, data);
        });
    }

    @Test
    public void testCsvRoundTrip() {
        db.executeTransactionally(
                "CREATE (f:Roundtrip {name:'foo',age:42,male:true,kids:['a','b','c']}),(b:Roundtrip {name:'bar',age:42}),(c:Roundtrip {age:12})");

        String fileName = "separatedFiles.csv.gzip";
        final Map<String, Object> params = map(
                "file",
                fileName,
                "query",
                "MATCH (u:Roundtrip) return u.name as name",
                "config",
                map(CompressionConfig.COMPRESSION, GZIP.name()));
        testCall(
                db,
                "CALL apoc.export.csv.query($query, $file, $config)",
                params,
                (r) -> assertEquals(fileName, r.get("file")));

        final String deleteQuery = "MATCH (n:Roundtrip) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);

        testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Roundtrip']}], [], $config) ",
                params,
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n:Roundtrip) return n.name as name", r -> {
            final Set<String> actual = Iterators.asSet(r.columnAs("name"));
            assertEquals(Set.of("foo", "bar", ""), actual);
        });

        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testCsvBackslashes() {
        db.executeTransactionally("CREATE (n:Test {name: 'Test', value: '{\"new\":\"4\\'10\\\\\\\"\"}'})");

        String fileName = "test.csv.quotes.csv";
        final Map<String, Object> params =
                map("file", fileName, "query", "MATCH (n: Test) RETURN n", "config", map("quotes", "always"));

        testCall(db, "CALL apoc.export.csv.all($file, $config)", params, (r) -> assertEquals(fileName, r.get("file")));

        final String deleteQuery = "MATCH (n:Test) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);

        testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Test']}],[],{})",
                params,
                r -> assertEquals(14L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n:Test) RETURN n.name as name, n.value as value", r -> {
            var nodes = r.stream().filter(node -> node.get("name").equals("Test"));
            var actual = nodes.map(node -> (String) node.get("value")).collect(Collectors.toSet());
            assertEquals(Set.of("{\"new\":\"4'10\\\"\"}"), actual);
        });

        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testCsvQueryWithDifferentiatedNulls() throws Exception {
        Map<String, String> differentiateNulls = Map.of(
                "none", EXPECTED_QUERY_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_QUERY_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_QUERY_DIFFERENTIATE_NULLS_ALWAYS);
        Map<String, String> dontDifferentiateNulls = Map.of(
                "none", EXPECTED_QUERY_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_QUERY_DONT_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_QUERY_DONT_DIFFERENTIATE_NULLS_ALWAYS);

        Map<Boolean, Map<String, String>> differentiateNullCases =
                Map.of(true, differentiateNulls, false, dontDifferentiateNulls);

        String fileName = "test.csv.diffNulls.csv";
        for (Boolean shouldDifferentiateNulls : differentiateNullCases.keySet()) {
            Map<String, String> currentCases = differentiateNullCases.get(shouldDifferentiateNulls);
            for (String quotingType : currentCases.keySet()) {
                final Map<String, Object> params = map(
                        "file",
                        fileName,
                        "config",
                        map("quotes", quotingType, "differentiateNulls", shouldDifferentiateNulls));

                testCall(
                        db,
                        "CALL apoc.export.csv.query(\"MATCH (d:ESCAPING) WITH d RETURN d.age as age, d.name as name\", $file, $config)",
                        params,
                        (r) -> assertEquals(fileName, r.get("file")));

                assertEquals(currentCases.get(quotingType), readFile(fileName));
            }
        }
    }

    @Test
    public void testCsvDataWithDifferentiatedNulls() throws Exception {
        Map<String, String> differentiateNulls = Map.of(
                "none", EXPECTED_DATA_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_DATA_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_DATA_DIFFERENTIATE_NULLS_ALWAYS);
        Map<String, String> dontDifferentiateNulls = Map.of(
                "none", EXPECTED_DATA_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_ALWAYS);

        Map<Boolean, Map<String, String>> differentiateNullCases =
                Map.of(true, differentiateNulls, false, dontDifferentiateNulls);

        String fileName = "test.csv.diffNulls.csv";
        for (Boolean shouldDifferentiateNulls : differentiateNullCases.keySet()) {
            Map<String, String> currentCases = differentiateNullCases.get(shouldDifferentiateNulls);
            for (String quotingType : currentCases.keySet()) {
                final Map<String, Object> params = map(
                        "file",
                        fileName,
                        "config",
                        map("quotes", quotingType, "differentiateNulls", shouldDifferentiateNulls));

                testCall(
                        db,
                        """
                        MATCH (n:ESCAPING)
                        WITH COLLECT(n) as nodes
                        CALL apoc.export.csv.data(nodes, [], $file, $config)
                        YIELD file
                        RETURN file
                        """,
                        params,
                        (r) -> assertEquals(fileName, r.get("file")));

                assertEquals(currentCases.get(quotingType), readFile(fileName));
            }
        }
    }

    @Test
    public void testCsvGraphWithDifferentiatedNulls() throws Exception {
        Map<String, String> differentiateNulls = Map.of(
                "none", EXPECTED_DATA_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_DATA_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_DATA_DIFFERENTIATE_NULLS_ALWAYS);
        Map<String, String> dontDifferentiateNulls = Map.of(
                "none", EXPECTED_DATA_DIFFERENTIATE_NULLS_NONE,
                "ifNeeded", EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_DATA_DONT_DIFFERENTIATE_NULLS_ALWAYS);

        Map<Boolean, Map<String, String>> differentiateNullCases =
                Map.of(true, differentiateNulls, false, dontDifferentiateNulls);

        String fileName = "test.csv.diffNulls.csv";
        for (Boolean shouldDifferentiateNulls : differentiateNullCases.keySet()) {
            Map<String, String> currentCases = differentiateNullCases.get(shouldDifferentiateNulls);
            for (String quotingType : currentCases.keySet()) {
                final Map<String, Object> params = map(
                        "file",
                        fileName,
                        "config",
                        map("quotes", quotingType, "differentiateNulls", shouldDifferentiateNulls));

                testCall(
                        db,
                        """
                        CALL apoc.graph.fromCypher('MATCH (n:ESCAPING) RETURN n',{}, 'test',{description: "test graph"}) yield graph
                        CALL apoc.export.csv.graph(graph, $file, $config)
                        YIELD file
                        RETURN file
                        """,
                        params,
                        (r) -> assertEquals(fileName, r.get("file")));

                assertEquals(currentCases.get(quotingType), readFile(fileName));
            }
        }
    }

    @Test
    public void testCsvAllWithDifferentiatedNulls() throws Exception {
        Map<String, String> differentiateNulls = Map.of(
                "none", EXPECTED_ALL_NONE_2,
                "ifNeeded", EXPECTED_ALL_DIFFERENTIATE_NULLS_IF_NEEDED,
                "always", EXPECTED_ALL_DIFFERENTIATE_NULLS_ALWAYS);
        Map<String, String> dontDifferentiateNulls = Map.of(
                "none", EXPECTED_ALL_NONE_2,
                "ifNeeded", EXPECTED_ALL_IF_NEEDED,
                "always", EXPECTED_ALL_ALWAYS);

        Map<Boolean, Map<String, String>> differentiateNullCases =
                Map.of(true, differentiateNulls, false, dontDifferentiateNulls);

        String fileName = "test.csv.diffNulls.csv";
        for (Boolean shouldDifferentiateNulls : differentiateNullCases.keySet()) {
            Map<String, String> currentCases = differentiateNullCases.get(shouldDifferentiateNulls);
            for (String quotingType : currentCases.keySet()) {
                final Map<String, Object> params = map(
                        "file",
                        fileName,
                        "config",
                        map("quotes", quotingType, "differentiateNulls", shouldDifferentiateNulls));

                testCall(
                        db,
                        "CALL apoc.export.csv.all($file, $config)",
                        params,
                        (r) -> assertEquals(fileName, r.get("file")));

                assertEquals(currentCases.get(quotingType), readFile(fileName));
            }
        }
    }

    @Test
    public void testExportAllCsv() throws Exception {
        String fileName = "all.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithDotInName() throws Exception {
        String fileName = "all.with.dot.filename.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithoutExtension() throws Exception {
        String fileName = "all";
        testExportCsvAllCommon(fileName);
    }

    private void testExportCsvAllCommon(String fileName) throws Exception {
        testCall(
                db,
                "CALL apoc.export.csv.all($file,null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ALL_ALWAYS, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithSample() throws Exception {
        db.executeTransactionally(
                "CREATE (:User:Sample {`last:Name`:'Galilei'}), (:User:Sample {address:'Universe'}),\n"
                        + "(:User:Sample {foo:'bar'})-[:KNOWS {one: 'two', three: 'four'}]->(:User:Sample {baz:'baa', foo: true})");
        String fileName = "all.csv";
        final long totalNodes = 14L;
        final long totalRels = 4L;
        final long totalProps = 29L;
        testCall(
                db,
                "CALL apoc.export.csv.all($file, null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, true));
        assertEquals(EXP_SAMPLE, readFile(fileName));

        // quotes: 'none' to simplify header testing
        testCall(
                db,
                "CALL apoc.export.csv.all($file, {sampling: true, samplingConfig: {sample: 1}, quotes: 'none'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, false));

        final String[] s =
                Files.lines(directory.resolve(fileName)).findFirst().get().split(",");
        assertTrue(s.length < 19);
        assertTrue(Arrays.asList(s).containsAll(List.of("_id", "_labels", "_start", "_end", "_type")));

        db.executeTransactionally("MATCH (n:Sample) DETACH DELETE n");
    }

    @Test
    public void testExportAllCsvWithQuotes() throws Exception {
        String fileName = "all.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: true})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ALL_ALWAYS, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithoutQuotes() throws Exception {
        String fileName = "all.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: 'none'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ALL_NONE_2, readFile(fileName));
    }

    @Test
    public void testExportAllCsvNeededQuotes() throws Exception {
        String fileName = "all.csv";
        testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: 'ifNeeded'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ALL_IF_NEEDED, readFile(fileName));
    }

    @Test
    public void testExportGraphCsv() throws Exception {
        String fileName = "graph.csv";
        testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.csv.graph(graph, $file,{quotes: 'none'}) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_ALL_NONE, readFile(fileName));
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() throws Exception {
        String fileName = "graph.csv";
        testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph " + "CALL apoc.export.csv.graph(graph, $file,null) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_ALL_ALWAYS_2, readFile(fileName));
    }

    @Test
    public void testExportQueryCsv() throws Exception {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        testCall(db, "CALL apoc.export.csv.query($query,$file,null)", map("file", fileName, "query", query), (r) -> {
            assertTrue("Should get statement", r.get("source").toString().contains("statement: cols(5)"));
            assertEquals(fileName, r.get("file"));
            assertEquals("csv", r.get("format"));
        });
        assertEquals(EXPECTED_QUERY, readFile(fileName));
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() throws Exception {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        testCall(
                db,
                "CALL apoc.export.csv.query($query,$file,{quotes: false})",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportCsvAdminOperationErrorMessage() {
        String filename = "test.csv";
        List<String> invalidQueries =
                List.of("SHOW CONSTRAINTS YIELD id, name, type RETURN *", "SHOW INDEXES YIELD id, name, type RETURN *");

        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(
                    QueryExecutionException.class,
                    () -> testCall(
                            db,
                            """
                        CALL apoc.export.csv.query(
                        $query,
                        $file,
                        {quotes: false}
                        )""",
                            map("query", query, "file", filename),
                            (r) -> {}));

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.csv.query");
        }
    }

    @Test
    public void testExportQueryNodesCsv() throws Exception {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) return u";
        testCall(db, "CALL apoc.export.csv.query($query,$file,null)", map("file", fileName, "query", query), (r) -> {
            assertTrue("Should get statement", r.get("source").toString().contains("statement: cols(1)"));
            assertEquals(fileName, r.get("file"));
            assertEquals("csv", r.get("format"));
        });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    @Test
    public void testExportQueryNodesCsvParams() throws Exception {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        testCall(
                db,
                "CALL apoc.export.csv.query($query,$file,{params:{age:10}})",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertResults(fileName, r, source, 10L, 3L, 22L, true);
    }

    private void assertResults(
            String fileName,
            Map<String, Object> r,
            final String source,
            Long expectedNodes,
            Long expectedRelationships,
            Long expectedProperties,
            boolean assertPropEquality) {
        assertEquals(expectedNodes, r.get("nodes"));
        assertEquals(expectedRelationships, r.get("relationships"));
        if (assertPropEquality) {
            assertEquals(expectedProperties, r.get("properties"));
        } else {
            assertTrue((Long) r.get("properties") < expectedProperties);
        }
        final String expectedSource = source + ": nodes(" + expectedNodes + "), rels(" + expectedRelationships + ")";
        assertEquals(expectedSource, r.get("source"));
        assertCsvCommon(fileName, r);
    }

    private void assertCsvCommon(String fileName, Map<String, Object> r) {
        assertEquals(fileName, r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportAllCsvStreaming() {
        String statement = "CALL apoc.export.csv.all(null,{stream:true,batchSize:2})";
        assertExportStreaming(statement, NONE);
    }

    @Test
    public void testExportAllCsvStreamingCompressed() {
        final CompressionAlgo algo = GZIP;
        String statement =
                "CALL apoc.export.csv.all(null, {compression: '" + algo.name() + "',stream:true,batchSize:2})";
        assertExportStreaming(statement, algo);
    }

    private void assertExportStreaming(String statement, CompressionAlgo algo) {
        StringBuilder sb = new StringBuilder();
        testResult(db, statement, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(2L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(4L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(3L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(6L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(4L, r.get("batches"));
            assertEquals(8L, r.get("nodes"));
            assertEquals(8L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(16L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(5L, r.get("batches"));
            assertEquals(10L, r.get("nodes"));
            assertEquals(10L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(19L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(6L, r.get("batches"));
            assertEquals(10L, r.get("nodes"));
            assertEquals(12L, r.get("rows"));
            assertEquals(2L, r.get("relationships"));
            assertEquals(19L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(7L, r.get("batches"));
            assertEquals(10L, r.get("nodes"));
            assertEquals(13L, r.get("rows"));
            assertEquals(3L, r.get("relationships"));
            assertEquals(22L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            res.close();
        });
        assertEquals(EXPECTED_ALL_ALWAYS, sb.toString());
    }

    @Test
    public void testCypherCsvStreaming() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));
        assertEquals(EXPECTED_QUERY, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithoutQuotes() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: false, stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));

        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, sb.toString());
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchUsers(StringBuilder sb) {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(15L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

    @Test
    public void testCypherCsvStreamingWithAlwaysQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'always', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_ALWAYS, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithNeededQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'ifNeeded', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NEEDED, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithNoneQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'none', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NONE, sb.toString());
    }

    @Test
    public void testExportQueryCsvIssue1188() {
        String copyright = "\n"
                + "(c) 2018 Hovsepian, Albanese, et al. \"\"ASCB(r),\"\" \"\"The American Society for Cell Biology(r),\"\" and \"\"Molecular Biology of the Cell(r)\"\" are registered trademarks of The American Society for Cell Biology.\n"
                + "2018\n"
                + "\n"
                + "This article is distributed by The American Society for Cell Biology under license from the author(s). Two months after publication it is available to the public under an Attribution-Noncommercial-Share Alike 3.0 Unported Creative Commons License.\n"
                + "\n";
        String pk = "5921569";
        db.executeTransactionally(
                "CREATE (n:Document{pk:$pk, copyright: $copyright})", map("copyright", copyright, "pk", pk));
        String query = "MATCH (n:Document{pk:'5921569'}) return n.pk as pk, n.copyright as copyright";
        testCall(
                db,
                "CALL apoc.export.csv.query($query, null, $config)",
                map("query", query, "config", map("stream", true)),
                (r) -> {
                    List<String[]> csv = CsvTestUtil.toCollection(r.get("data").toString());
                    assertEquals(2, csv.size());
                    assertArrayEquals(new String[] {"pk", "copyright"}, csv.get(0));
                    assertArrayEquals(new String[] {"5921569", copyright}, csv.get(1));
                });
        db.executeTransactionally("MATCH (d:Document) DETACH DELETE d");
    }

    @Test
    public void testExportWgsPoint() {
        db.executeTransactionally(
                "CREATE (p:Position {place: point({latitude: 12.78, longitude: 56.7, height: 1.1})})");

        testCall(
                db,
                "CALL apoc.export.csv.query($query, null, {quotes: 'none', stream: true}) YIELD data RETURN data",
                map("query", "MATCH (p:Position) RETURN p.place as place"),
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> place = Util.fromJson(data.split(System.lineSeparator())[1], Map.class);
                    assertEquals(12.78D, (double) place.get("latitude"), 0);
                    assertEquals(56.7D, (double) place.get("longitude"), 0);
                    assertEquals(1.1D, (double) place.get("height"), 0);
                });
        db.executeTransactionally("MATCH (n:Position) DETACH DELETE n");
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchAddress(StringBuilder sb) {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(8L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

    @Test
    public void testDifferentCypherVersionsApocCsvQuery() {
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.export.csv.query('%s RETURN apoc.cypherVersion() AS version', null, { stream:true }) YIELD data RETURN data",
                    cypherVersion.outerVersion, cypherVersion.innerVersion);
            testCall(db, query, r -> assertTrue(r.get("data").toString().contains(cypherVersion.result)));
        }
    }
}
