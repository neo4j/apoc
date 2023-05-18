CREATE CONSTRAINT SingleUnique FOR (x:Label) REQUIRE (x.prop) IS UNIQUE;
CREATE CONSTRAINT SingleUniqueRel FOR ()-[x:TYPE]->() REQUIRE (x.prop) IS UNIQUE;
CREATE CONSTRAINT CompositeUnique FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS UNIQUE;
CREATE CONSTRAINT CompositeUniqueRel FOR ()-[x:TYPE]-() REQUIRE (x.prop1, x.prop2) IS UNIQUE;
CREATE CONSTRAINT SingleExists FOR (x:Label2) REQUIRE (x.prop) IS NOT NULL;
CREATE CONSTRAINT SingleExistsRel FOR ()<-[r:TYPE2]-() REQUIRE (r.prop) IS NOT NULL;
CREATE CONSTRAINT SingleNodeKey FOR (x:Label3) REQUIRE (x.prop) IS NODE KEY;
CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (t:Person) REQUIRE (t.name, t.surname) IS NODE KEY;
CREATE CONSTRAINT KnowsConsNotNull FOR ()-[r:KNOWS]-() REQUIRE (r.foo) IS NOT NULL;
CREATE CONSTRAINT KnowsConsUnique FOR ()-[r:KNOWS]-() REQUIRE (r.two) IS UNIQUE;

CREATE (a:Person {name: 'John', surname: 'Snow'})
CREATE (b:Person {name: 'Matt', surname: 'Jackson'})
CREATE (c:Person {name: 'Jenny', surname: 'White'})
CREATE (d:Person {name: 'Susan', surname: 'Brown'})
CREATE (e:Person {name: 'Tom', surname: 'Taylor'})
CREATE (a)-[:KNOWS {foo: 1}]->(b);