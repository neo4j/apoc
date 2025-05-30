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
package apoc.help;

import java.util.Set;

public class HelpUtil {
    // Please update here on the addition of any new procedures or functions!
    public static final Set<String> coreProceduresV5 = Set.of(
            "apoc.periodic.truncate",
            "apoc.periodic.list",
            "apoc.periodic.commit",
            "apoc.periodic.cancel",
            "apoc.periodic.submit",
            "apoc.periodic.repeat",
            "apoc.periodic.countdown",
            "apoc.periodic.iterate",
            "apoc.trigger.install",
            "apoc.trigger.drop",
            "apoc.trigger.dropAll",
            "apoc.trigger.stop",
            "apoc.trigger.start",
            "apoc.trigger.show",
            "apoc.trigger.add",
            "apoc.trigger.remove",
            "apoc.trigger.removeAll",
            "apoc.trigger.list",
            "apoc.trigger.pause",
            "apoc.trigger.resume",
            "apoc.util.sleep",
            "apoc.util.validate",
            "apoc.merge.node.eager",
            "apoc.merge.node",
            "apoc.merge.nodeWithStats.eager",
            "apoc.merge.nodeWithStats",
            "apoc.merge.relationship",
            "apoc.merge.relationshipWithStats",
            "apoc.merge.relationship.eager",
            "apoc.merge.relationshipWithStats.eager",
            "apoc.nodes.cycles",
            "apoc.nodes.link",
            "apoc.nodes.get",
            "apoc.nodes.delete",
            "apoc.nodes.rels",
            "apoc.nodes.collapse",
            "apoc.nodes.group",
            "apoc.example.movies",
            "apoc.path.expand",
            "apoc.path.expandConfig",
            "apoc.path.subgraphNodes",
            "apoc.path.subgraphAll",
            "apoc.path.spanningTree",
            "apoc.graph.fromData",
            "apoc.graph.from",
            "apoc.graph.fromPath",
            "apoc.graph.fromPaths",
            "apoc.graph.fromDB",
            "apoc.graph.fromCypher",
            "apoc.graph.fromDocument",
            "apoc.graph.validateDocument",
            "apoc.lock.all",
            "apoc.lock.nodes",
            "apoc.lock.read.nodes",
            "apoc.lock.rels",
            "apoc.lock.read.rels",
            "apoc.algo.aStar",
            "apoc.algo.aStarConfig",
            "apoc.algo.dijkstra",
            "apoc.algo.allSimplePaths",
            "apoc.algo.cover",
            "apoc.meta.stats",
            "apoc.meta.data.of",
            "apoc.meta.data",
            "apoc.meta.schema",
            "apoc.meta.nodeTypeProperties",
            "apoc.meta.relTypeProperties",
            "apoc.meta.graph",
            "apoc.meta.graph.of",
            "apoc.meta.graphSample",
            "apoc.meta.subGraph",
            "apoc.cypher.runTimeboxed",
            "apoc.cypher.run",
            "apoc.cypher.runMany",
            "apoc.cypher.runManyReadOnly",
            "apoc.cypher.doIt",
            "apoc.cypher.runWrite",
            "apoc.cypher.runSchema",
            "apoc.when",
            "apoc.do.when",
            "apoc.case",
            "apoc.do.case",
            "apoc.atomic.add",
            "apoc.atomic.subtract",
            "apoc.atomic.concat",
            "apoc.atomic.insert",
            "apoc.atomic.remove",
            "apoc.atomic.update",
            "apoc.math.regr",
            "apoc.search.nodeAllReduced",
            "apoc.search.nodeReduced",
            "apoc.search.multiSearchReduced",
            "apoc.search.nodeAll",
            "apoc.search.node",
            "apoc.schema.assert",
            "apoc.schema.nodes",
            "apoc.schema.relationships",
            "apoc.coll.zipToRows",
            "apoc.coll.elements",
            "apoc.coll.partition",
            "apoc.coll.split",
            "apoc.coll.pairWithOffset",
            "apoc.load.jsonArray",
            "apoc.load.json",
            "apoc.load.jsonParams",
            "apoc.load.xml",
            "apoc.import.xml",
            "apoc.load.arrow.stream",
            "apoc.load.arrow",
            "apoc.schema.properties.distinct",
            "apoc.schema.properties.distinctCount",
            "apoc.log.stream",
            "apoc.text.phoneticDelta",
            "apoc.export.arrow.stream.all",
            "apoc.export.arrow.stream.graph",
            "apoc.export.arrow.stream.query",
            "apoc.export.arrow.all",
            "apoc.export.arrow.graph",
            "apoc.export.arrow.query",
            "apoc.export.cypher.all",
            "apoc.export.cypher.data",
            "apoc.export.cypher.graph",
            "apoc.export.cypher.query",
            "apoc.export.cypher.schema",
            "apoc.import.json",
            "apoc.export.json.all",
            "apoc.export.json.data",
            "apoc.export.json.graph",
            "apoc.export.json.query",
            "apoc.import.csv",
            "apoc.export.csv.all",
            "apoc.export.csv.data",
            "apoc.export.csv.graph",
            "apoc.export.csv.query",
            "apoc.import.graphml",
            "apoc.export.graphml.all",
            "apoc.export.graphml.data",
            "apoc.export.graphml.graph",
            "apoc.export.graphml.query",
            "apoc.spatial.sortByDistance",
            "apoc.spatial.geocodeOnce",
            "apoc.spatial.geocode",
            "apoc.spatial.reverseGeocode",
            "apoc.create.node",
            "apoc.create.addLabels",
            "apoc.create.setProperty",
            "apoc.create.setRelProperty",
            "apoc.create.setProperties",
            "apoc.create.removeProperties",
            "apoc.create.setRelProperties",
            "apoc.create.removeRelProperties",
            "apoc.create.setLabels",
            "apoc.create.removeLabels",
            "apoc.create.nodes",
            "apoc.create.relationship",
            "apoc.create.vNode",
            "apoc.create.vNodes",
            "apoc.create.vRelationship",
            "apoc.create.virtualPath",
            "apoc.create.clonePathToVirtual",
            "apoc.create.clonePathsToVirtual",
            "apoc.create.uuids",
            "apoc.warmup.run",
            "apoc.stats.degrees",
            "apoc.help",
            "apoc.refactor.rename.label",
            "apoc.refactor.rename.type",
            "apoc.refactor.rename.nodeProperty",
            "apoc.refactor.rename.typeProperty",
            "apoc.refactor.extractNode",
            "apoc.refactor.collapseNode",
            "apoc.refactor.cloneNodes",
            "apoc.refactor.cloneSubgraphFromPaths",
            "apoc.refactor.cloneSubgraph",
            "apoc.refactor.mergeNodes",
            "apoc.refactor.mergeRelationships",
            "apoc.refactor.setType",
            "apoc.refactor.to",
            "apoc.refactor.invert",
            "apoc.refactor.from",
            "apoc.refactor.normalizeAsBoolean",
            "apoc.refactor.categorize",
            "apoc.refactor.deleteAndReconnect",
            "apoc.convert.setJsonProperty",
            "apoc.convert.toTree",
            "apoc.paths.toJsonTree",
            "apoc.neighbors.tohop",
            "apoc.neighbors.tohop.count",
            "apoc.neighbors.byhop",
            "apoc.neighbors.byhop.count",
            "apoc.neighbors.athop",
            "apoc.neighbors.athop.count");
    ;

    public static final Set<String> coreFunctionsV5 = Set.of(
            "apoc.temporal.format",
            "apoc.temporal.formatDuration",
            "apoc.temporal.toZonedTemporal",
            "apoc.util.sha1",
            "apoc.util.sha256",
            "apoc.util.sha384",
            "apoc.util.sha512",
            "apoc.util.md5",
            "apoc.util.validatePredicate",
            "apoc.util.decompress",
            "apoc.util.compress",
            "apoc.node.relationship.exists",
            "apoc.nodes.connected",
            "apoc.node.labels",
            "apoc.node.id",
            "apoc.rel.id",
            "apoc.rel.startNode",
            "apoc.rel.endNode",
            "apoc.rel.type",
            "apoc.any.properties",
            "apoc.any.property",
            "apoc.node.degree",
            "apoc.node.degree.in",
            "apoc.node.degree.out",
            "apoc.node.relationship.types",
            "apoc.nodes.relationship.types",
            "apoc.node.relationships.exist",
            "apoc.nodes.relationships.exist",
            "apoc.nodes.isDense",
            "apoc.any.isDeleted",
            "apoc.path.create",
            "apoc.path.slice",
            "apoc.path.combine",
            "apoc.path.elements",
            "apoc.date.toYears",
            "apoc.date.fields",
            "apoc.date.field",
            "apoc.date.currentTimestamp",
            "apoc.date.format",
            "apoc.date.toISO8601",
            "apoc.date.fromISO8601",
            "apoc.date.parse",
            "apoc.date.systemTimezone",
            "apoc.date.convert",
            "apoc.date.convertFormat",
            "apoc.date.add",
            "apoc.label.exists",
            "apoc.meta.cypher.isType",
            "apoc.meta.cypher.type",
            "apoc.meta.cypher.types",
            "apoc.meta.nodes.count",
            "apoc.diff.nodes",
            "apoc.cypher.runFirstColumnMany",
            "apoc.cypher.runFirstColumnSingle",
            "apoc.hashing.fingerprint",
            "apoc.hashing.fingerprinting",
            "apoc.hashing.fingerprintGraph",
            "apoc.math.maxLong",
            "apoc.math.minLong",
            "apoc.math.maxDouble",
            "apoc.math.minDouble",
            "apoc.math.maxInt",
            "apoc.math.minInt",
            "apoc.math.maxByte",
            "apoc.math.minByte",
            "apoc.math.sigmoid",
            "apoc.math.sigmoidPrime",
            "apoc.math.tanh",
            "apoc.math.coth",
            "apoc.math.cosh",
            "apoc.math.sinh",
            "apoc.math.sech",
            "apoc.math.csch",
            "apoc.number.format",
            "apoc.number.parseInt",
            "apoc.number.parseFloat",
            "apoc.number.exact.add",
            "apoc.number.exact.sub",
            "apoc.number.exact.mul",
            "apoc.number.exact.div",
            "apoc.number.exact.toInteger",
            "apoc.number.exact.toFloat",
            "apoc.number.exact.toExact",
            "apoc.number.romanToArabic",
            "apoc.number.arabicToRoman",
            "apoc.schema.node.indexExists",
            "apoc.schema.relationship.indexExists",
            "apoc.schema.node.constraintExists",
            "apoc.schema.relationship.constraintExists",
            "apoc.coll.stdev",
            "apoc.coll.runningTotal",
            "apoc.coll.zip",
            "apoc.coll.pairs",
            "apoc.coll.pairsMin",
            "apoc.coll.sum",
            "apoc.coll.avg",
            "apoc.coll.min",
            "apoc.coll.max",
            "apoc.coll.partition",
            "apoc.coll.contains",
            "apoc.coll.set",
            "apoc.coll.insert",
            "apoc.coll.insertAll",
            "apoc.coll.remove",
            "apoc.coll.indexOf",
            "apoc.coll.containsAll",
            "apoc.coll.containsSorted",
            "apoc.coll.containsAllSorted",
            "apoc.coll.isEqualCollection",
            "apoc.coll.toSet",
            "apoc.coll.sumLongs",
            "apoc.coll.sort",
            "apoc.coll.sortNodes",
            "apoc.coll.sortMaps",
            "apoc.coll.union",
            "apoc.coll.removeAll",
            "apoc.coll.subtract",
            "apoc.coll.intersection",
            "apoc.coll.disjunction",
            "apoc.coll.unionAll",
            "apoc.coll.shuffle",
            "apoc.coll.randomItem",
            "apoc.coll.randomItems",
            "apoc.coll.containsDuplicates",
            "apoc.coll.duplicates",
            "apoc.coll.duplicatesWithCount",
            "apoc.coll.frequencies",
            "apoc.coll.frequenciesAsMap",
            "apoc.coll.occurrences",
            "apoc.coll.flatten",
            "apoc.coll.sortMulti",
            "apoc.coll.combinations",
            "apoc.coll.different",
            "apoc.coll.dropDuplicateNeighbors",
            "apoc.coll.fill",
            "apoc.coll.sortText",
            "apoc.coll.pairWithOffset",
            "apoc.xml.parse",
            "apoc.map.groupBy",
            "apoc.map.groupByMulti",
            "apoc.map.fromNodes",
            "apoc.map.fromPairs",
            "apoc.map.fromLists",
            "apoc.map.values",
            "apoc.map.fromValues",
            "apoc.map.merge",
            "apoc.map.mergeList",
            "apoc.map.get",
            "apoc.map.mget",
            "apoc.map.submap",
            "apoc.map.setKey",
            "apoc.map.setEntry",
            "apoc.map.setPairs",
            "apoc.map.setLists",
            "apoc.map.setValues",
            "apoc.map.removeKey",
            "apoc.map.removeKeys",
            "apoc.map.clean",
            "apoc.map.updateTree",
            "apoc.map.flatten",
            "apoc.map.unflatten",
            "apoc.map.sortedProperties",
            "apoc.version",
            "apoc.scoring.existence",
            "apoc.scoring.pareto",
            "apoc.text.phonetic",
            "apoc.text.doubleMetaphone",
            "apoc.text.indexOf",
            "apoc.text.indexesOf",
            "apoc.text.replace",
            "apoc.text.byteCount",
            "apoc.text.bytes",
            "apoc.text.regreplace",
            "apoc.text.split",
            "apoc.text.regexGroups",
            "apoc.text.regexGroupsByName",
            "apoc.text.join",
            "apoc.text.clean",
            "apoc.text.compareCleaned",
            "apoc.text.distance",
            "apoc.text.levenshteinDistance",
            "apoc.text.levenshteinSimilarity",
            "apoc.text.hammingDistance",
            "apoc.text.jaroWinklerDistance",
            "apoc.text.sorensenDiceSimilarity",
            "apoc.text.fuzzyMatch",
            "apoc.text.urlencode",
            "apoc.text.urldecode",
            "apoc.text.lpad",
            "apoc.text.rpad",
            "apoc.text.format",
            "apoc.text.slug",
            "apoc.text.random",
            "apoc.text.capitalize",
            "apoc.text.capitalizeAll",
            "apoc.text.decapitalize",
            "apoc.text.decapitalizeAll",
            "apoc.text.swapCase",
            "apoc.text.camelCase",
            "apoc.text.upperCamelCase",
            "apoc.text.snakeCase",
            "apoc.text.toUpperCase",
            "apoc.text.base64Encode",
            "apoc.text.base64Decode",
            "apoc.text.base64UrlEncode",
            "apoc.text.base64UrlDecode",
            "apoc.text.charAt",
            "apoc.text.code",
            "apoc.text.hexValue",
            "apoc.text.hexCharAt",
            "apoc.text.toCypher",
            "apoc.text.repeat",
            "apoc.bitwise.op",
            "apoc.data.url",
            "apoc.create.vNode",
            "apoc.create.virtual.fromNode",
            "apoc.create.vRelationship",
            "apoc.create.uuid",
            "apoc.create.uuidBase64",
            "apoc.create.uuidBase64ToHex",
            "apoc.create.uuidHexToBase64",
            "apoc.json.path",
            "apoc.convert.toJson",
            "apoc.convert.getJsonProperty",
            "apoc.convert.getJsonPropertyMap",
            "apoc.convert.fromJsonMap",
            "apoc.convert.fromJsonList",
            "apoc.convert.toSortedJsonMap",
            "apoc.convert.toMap",
            "apoc.convert.toList",
            "apoc.convert.toNode",
            "apoc.convert.toRelationship",
            "apoc.convert.toSet",
            "apoc.convert.toNodeList",
            "apoc.convert.toRelationshipList",
            "apoc.agg.percentiles",
            "apoc.agg.product",
            "apoc.agg.graph",
            "apoc.agg.maxItems",
            "apoc.agg.minItems",
            "apoc.agg.statistics",
            "apoc.agg.median",
            "apoc.agg.nth",
            "apoc.agg.first",
            "apoc.agg.last",
            "apoc.agg.slice");
    ;

    public static final Set<String> coreProceduresV25 = Set.of(
            "apoc.periodic.truncate",
            "apoc.periodic.list",
            "apoc.periodic.commit",
            "apoc.periodic.cancel",
            "apoc.periodic.submit",
            "apoc.periodic.repeat",
            "apoc.periodic.countdown",
            "apoc.periodic.iterate",
            "apoc.trigger.install",
            "apoc.trigger.drop",
            "apoc.trigger.dropAll",
            "apoc.trigger.stop",
            "apoc.trigger.start",
            "apoc.trigger.show",
            "apoc.trigger.list",
            "apoc.util.sleep",
            "apoc.util.validate",
            "apoc.merge.node.eager",
            "apoc.merge.node",
            "apoc.merge.nodeWithStats.eager",
            "apoc.merge.nodeWithStats",
            "apoc.merge.relationship",
            "apoc.merge.relationshipWithStats",
            "apoc.merge.relationship.eager",
            "apoc.merge.relationshipWithStats.eager",
            "apoc.nodes.cycles",
            "apoc.nodes.link",
            "apoc.nodes.get",
            "apoc.nodes.delete",
            "apoc.nodes.rels",
            "apoc.nodes.collapse",
            "apoc.nodes.group",
            "apoc.example.movies",
            "apoc.path.expand",
            "apoc.path.expandConfig",
            "apoc.path.subgraphNodes",
            "apoc.path.subgraphAll",
            "apoc.path.spanningTree",
            "apoc.graph.fromData",
            "apoc.graph.from",
            "apoc.graph.fromPath",
            "apoc.graph.fromPaths",
            "apoc.graph.fromDB",
            "apoc.graph.fromCypher",
            "apoc.graph.fromDocument",
            "apoc.graph.validateDocument",
            "apoc.lock.all",
            "apoc.lock.nodes",
            "apoc.lock.read.nodes",
            "apoc.lock.rels",
            "apoc.lock.read.rels",
            "apoc.algo.aStar",
            "apoc.algo.aStarConfig",
            "apoc.algo.dijkstra",
            "apoc.algo.allSimplePaths",
            "apoc.algo.cover",
            "apoc.meta.stats",
            "apoc.meta.data.of",
            "apoc.meta.data",
            "apoc.meta.schema",
            "apoc.meta.nodeTypeProperties",
            "apoc.meta.relTypeProperties",
            "apoc.meta.graph",
            "apoc.meta.graph.of",
            "apoc.meta.graphSample",
            "apoc.meta.subGraph",
            "apoc.cypher.runTimeboxed",
            "apoc.cypher.run",
            "apoc.cypher.runMany",
            "apoc.cypher.runManyReadOnly",
            "apoc.cypher.doIt",
            "apoc.cypher.runWrite",
            "apoc.cypher.runSchema",
            "apoc.when",
            "apoc.do.when",
            "apoc.case",
            "apoc.do.case",
            "apoc.atomic.add",
            "apoc.atomic.subtract",
            "apoc.atomic.concat",
            "apoc.atomic.insert",
            "apoc.atomic.remove",
            "apoc.atomic.update",
            "apoc.math.regr",
            "apoc.search.nodeAllReduced",
            "apoc.search.nodeReduced",
            "apoc.search.multiSearchReduced",
            "apoc.search.nodeAll",
            "apoc.search.node",
            "apoc.schema.assert",
            "apoc.schema.nodes",
            "apoc.schema.relationships",
            "apoc.coll.zipToRows",
            "apoc.coll.elements",
            "apoc.coll.partition",
            "apoc.coll.split",
            "apoc.coll.pairWithOffset",
            "apoc.load.jsonArray",
            "apoc.load.json",
            "apoc.load.xml",
            "apoc.import.xml",
            "apoc.schema.properties.distinct",
            "apoc.schema.properties.distinctCount",
            "apoc.text.phoneticDelta",
            "apoc.export.cypher.all",
            "apoc.export.cypher.data",
            "apoc.export.cypher.graph",
            "apoc.export.cypher.query",
            "apoc.export.cypher.schema",
            "apoc.import.json",
            "apoc.export.json.all",
            "apoc.export.json.data",
            "apoc.export.json.graph",
            "apoc.export.json.query",
            "apoc.import.csv",
            "apoc.export.csv.all",
            "apoc.export.csv.data",
            "apoc.export.csv.graph",
            "apoc.export.csv.query",
            "apoc.import.graphml",
            "apoc.export.graphml.all",
            "apoc.export.graphml.data",
            "apoc.export.graphml.graph",
            "apoc.export.graphml.query",
            "apoc.spatial.sortByDistance",
            "apoc.spatial.geocodeOnce",
            "apoc.spatial.geocode",
            "apoc.spatial.reverseGeocode",
            "apoc.create.node",
            "apoc.create.addLabels",
            "apoc.create.setProperty",
            "apoc.create.setRelProperty",
            "apoc.create.setProperties",
            "apoc.create.removeProperties",
            "apoc.create.setRelProperties",
            "apoc.create.removeRelProperties",
            "apoc.create.setLabels",
            "apoc.create.removeLabels",
            "apoc.create.nodes",
            "apoc.create.relationship",
            "apoc.create.vNode",
            "apoc.create.vNodes",
            "apoc.create.vRelationship",
            "apoc.create.virtualPath",
            "apoc.create.clonePathToVirtual",
            "apoc.create.clonePathsToVirtual",
            "apoc.stats.degrees",
            "apoc.help",
            "apoc.refactor.rename.label",
            "apoc.refactor.rename.type",
            "apoc.refactor.rename.nodeProperty",
            "apoc.refactor.rename.typeProperty",
            "apoc.refactor.extractNode",
            "apoc.refactor.collapseNode",
            "apoc.refactor.cloneNodes",
            "apoc.refactor.cloneSubgraphFromPaths",
            "apoc.refactor.cloneSubgraph",
            "apoc.refactor.mergeNodes",
            "apoc.refactor.mergeRelationships",
            "apoc.refactor.setType",
            "apoc.refactor.to",
            "apoc.refactor.invert",
            "apoc.refactor.from",
            "apoc.refactor.normalizeAsBoolean",
            "apoc.refactor.categorize",
            "apoc.refactor.deleteAndReconnect",
            "apoc.convert.setJsonProperty",
            "apoc.paths.toJsonTree",
            "apoc.neighbors.tohop",
            "apoc.neighbors.tohop.count",
            "apoc.neighbors.byhop",
            "apoc.neighbors.byhop.count",
            "apoc.neighbors.athop",
            "apoc.neighbors.athop.count");
    ;

    public static final Set<String> coreFunctionsV25 = Set.of(
            "apoc.temporal.format",
            "apoc.temporal.formatDuration",
            "apoc.temporal.toZonedTemporal",
            "apoc.util.sha1",
            "apoc.util.sha256",
            "apoc.util.sha384",
            "apoc.util.sha512",
            "apoc.util.md5",
            "apoc.util.validatePredicate",
            "apoc.util.decompress",
            "apoc.util.compress",
            "apoc.node.relationship.exists",
            "apoc.nodes.connected",
            "apoc.node.labels",
            "apoc.node.id",
            "apoc.rel.id",
            "apoc.rel.startNode",
            "apoc.rel.endNode",
            "apoc.rel.type",
            "apoc.any.properties",
            "apoc.any.property",
            "apoc.node.degree",
            "apoc.node.degree.in",
            "apoc.node.degree.out",
            "apoc.node.relationship.types",
            "apoc.nodes.relationship.types",
            "apoc.node.relationships.exist",
            "apoc.nodes.relationships.exist",
            "apoc.nodes.isDense",
            "apoc.any.isDeleted",
            "apoc.path.create",
            "apoc.path.slice",
            "apoc.path.combine",
            "apoc.path.elements",
            "apoc.date.toYears",
            "apoc.date.fields",
            "apoc.date.field",
            "apoc.date.currentTimestamp",
            "apoc.date.format",
            "apoc.date.toISO8601",
            "apoc.date.fromISO8601",
            "apoc.date.parse",
            "apoc.date.systemTimezone",
            "apoc.date.convert",
            "apoc.date.convertFormat",
            "apoc.date.add",
            "apoc.label.exists",
            "apoc.meta.cypher.isType",
            "apoc.meta.cypher.type",
            "apoc.meta.cypher.types",
            "apoc.meta.nodes.count",
            "apoc.diff.nodes",
            "apoc.cypher.runFirstColumnMany",
            "apoc.cypher.runFirstColumnSingle",
            "apoc.hashing.fingerprint",
            "apoc.hashing.fingerprinting",
            "apoc.hashing.fingerprintGraph",
            "apoc.math.maxLong",
            "apoc.math.minLong",
            "apoc.math.maxDouble",
            "apoc.math.minDouble",
            "apoc.math.maxInt",
            "apoc.math.minInt",
            "apoc.math.maxByte",
            "apoc.math.minByte",
            "apoc.math.sigmoid",
            "apoc.math.sigmoidPrime",
            "apoc.math.tanh",
            "apoc.math.coth",
            "apoc.math.cosh",
            "apoc.math.sinh",
            "apoc.math.sech",
            "apoc.math.csch",
            "apoc.number.format",
            "apoc.number.parseInt",
            "apoc.number.parseFloat",
            "apoc.number.exact.add",
            "apoc.number.exact.sub",
            "apoc.number.exact.mul",
            "apoc.number.exact.div",
            "apoc.number.exact.toInteger",
            "apoc.number.exact.toFloat",
            "apoc.number.exact.toExact",
            "apoc.number.romanToArabic",
            "apoc.number.arabicToRoman",
            "apoc.schema.node.indexExists",
            "apoc.schema.relationship.indexExists",
            "apoc.schema.node.constraintExists",
            "apoc.schema.relationship.constraintExists",
            "apoc.coll.stdev",
            "apoc.coll.runningTotal",
            "apoc.coll.zip",
            "apoc.coll.pairs",
            "apoc.coll.pairsMin",
            "apoc.coll.sum",
            "apoc.coll.avg",
            "apoc.coll.min",
            "apoc.coll.max",
            "apoc.coll.partition",
            "apoc.coll.contains",
            "apoc.coll.set",
            "apoc.coll.insert",
            "apoc.coll.insertAll",
            "apoc.coll.remove",
            "apoc.coll.indexOf",
            "apoc.coll.containsAll",
            "apoc.coll.containsSorted",
            "apoc.coll.containsAllSorted",
            "apoc.coll.isEqualCollection",
            "apoc.coll.toSet",
            "apoc.coll.sumLongs",
            "apoc.coll.sort",
            "apoc.coll.sortNodes",
            "apoc.coll.sortMaps",
            "apoc.coll.union",
            "apoc.coll.removeAll",
            "apoc.coll.subtract",
            "apoc.coll.intersection",
            "apoc.coll.disjunction",
            "apoc.coll.unionAll",
            "apoc.coll.shuffle",
            "apoc.coll.randomItem",
            "apoc.coll.randomItems",
            "apoc.coll.containsDuplicates",
            "apoc.coll.duplicates",
            "apoc.coll.duplicatesWithCount",
            "apoc.coll.frequencies",
            "apoc.coll.frequenciesAsMap",
            "apoc.coll.occurrences",
            "apoc.coll.flatten",
            "apoc.coll.sortMulti",
            "apoc.coll.combinations",
            "apoc.coll.different",
            "apoc.coll.dropDuplicateNeighbors",
            "apoc.coll.fill",
            "apoc.coll.sortText",
            "apoc.coll.pairWithOffset",
            "apoc.xml.parse",
            "apoc.map.groupBy",
            "apoc.map.groupByMulti",
            "apoc.map.fromNodes",
            "apoc.map.fromPairs",
            "apoc.map.fromLists",
            "apoc.map.values",
            "apoc.map.fromValues",
            "apoc.map.merge",
            "apoc.map.mergeList",
            "apoc.map.get",
            "apoc.map.mget",
            "apoc.map.submap",
            "apoc.map.setKey",
            "apoc.map.setPairs",
            "apoc.map.setLists",
            "apoc.map.setValues",
            "apoc.map.removeKey",
            "apoc.map.removeKeys",
            "apoc.map.clean",
            "apoc.map.updateTree",
            "apoc.map.flatten",
            "apoc.map.unflatten",
            "apoc.map.sortedProperties",
            "apoc.version",
            "apoc.scoring.existence",
            "apoc.scoring.pareto",
            "apoc.text.phonetic",
            "apoc.text.doubleMetaphone",
            "apoc.text.indexOf",
            "apoc.text.indexesOf",
            "apoc.text.replace",
            "apoc.text.byteCount",
            "apoc.text.bytes",
            "apoc.text.split",
            "apoc.text.regexGroups",
            "apoc.text.regexGroupsByName",
            "apoc.text.join",
            "apoc.text.clean",
            "apoc.text.compareCleaned",
            "apoc.text.distance",
            "apoc.text.levenshteinSimilarity",
            "apoc.text.hammingDistance",
            "apoc.text.jaroWinklerDistance",
            "apoc.text.sorensenDiceSimilarity",
            "apoc.text.fuzzyMatch",
            "apoc.text.urlencode",
            "apoc.text.urldecode",
            "apoc.text.lpad",
            "apoc.text.rpad",
            "apoc.text.format",
            "apoc.text.slug",
            "apoc.text.random",
            "apoc.text.capitalize",
            "apoc.text.capitalizeAll",
            "apoc.text.decapitalize",
            "apoc.text.decapitalizeAll",
            "apoc.text.swapCase",
            "apoc.text.camelCase",
            "apoc.text.upperCamelCase",
            "apoc.text.snakeCase",
            "apoc.text.toUpperCase",
            "apoc.text.base64Encode",
            "apoc.text.base64Decode",
            "apoc.text.base64UrlEncode",
            "apoc.text.base64UrlDecode",
            "apoc.text.charAt",
            "apoc.text.code",
            "apoc.text.hexValue",
            "apoc.text.hexCharAt",
            "apoc.text.toCypher",
            "apoc.text.repeat",
            "apoc.bitwise.op",
            "apoc.data.url",
            "apoc.create.vNode",
            "apoc.create.virtual.fromNode",
            "apoc.create.vRelationship",
            "apoc.create.uuidBase64",
            "apoc.create.uuidBase64ToHex",
            "apoc.create.uuidHexToBase64",
            "apoc.json.path",
            "apoc.convert.toJson",
            "apoc.convert.getJsonProperty",
            "apoc.convert.getJsonPropertyMap",
            "apoc.convert.fromJsonMap",
            "apoc.convert.fromJsonList",
            "apoc.convert.toSortedJsonMap",
            "apoc.convert.toMap",
            "apoc.convert.toList",
            "apoc.convert.toNode",
            "apoc.convert.toRelationship",
            "apoc.convert.toSet",
            "apoc.convert.toNodeList",
            "apoc.convert.toRelationshipList",
            "apoc.agg.percentiles",
            "apoc.agg.product",
            "apoc.agg.graph",
            "apoc.agg.maxItems",
            "apoc.agg.minItems",
            "apoc.agg.statistics",
            "apoc.agg.median",
            "apoc.agg.nth",
            "apoc.agg.first",
            "apoc.agg.last",
            "apoc.agg.slice");
}
