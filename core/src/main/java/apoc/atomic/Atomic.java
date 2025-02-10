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
package apoc.atomic;

import apoc.atomic.util.AtomicUtils;
import apoc.util.ArrayBackedList;
import apoc.util.MapUtil;
import apoc.util.Util;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.*;

/**
 * @author AgileLARUS
 * @since 20-06-17
 */
public class Atomic {

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    /**
     * increment a property's value
     */
    @Procedure(name = "apoc.atomic.add", mode = Mode.WRITE)
    @Description("Sets the given property to the sum of itself and the given `INTEGER` or `FLOAT` value.\n"
            + "The procedure then sets the property to the returned sum.")
    public Stream<AtomicResults> add(
            @Name(
                            value = "container",
                            description =
                                    "The node or relationship that contains the property to which the value will be added.")
                    Object container,
            @Name(value = "propertyName", description = "The name of the property whose value will be added to.")
                    String property,
            @Name(value = "number", description = "The number to add.") Number number,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(container);
        final Number[] newValue = new Number[1];
        final Number[] oldValue = new Number[1];
        Entity entity = Util.rebind(tx, (Entity) container);

        final ExecutionContext executionContext = new ExecutionContext(tx, entity);
        retry(
                executionContext,
                (context) -> {
                    oldValue[0] = (Number) entity.getProperty(property);
                    newValue[0] = AtomicUtils.sum((Number) entity.getProperty(property), number);
                    entity.setProperty(property, newValue[0]);
                    return context.entity.getProperty(property);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], newValue[0]));
    }

    /**
     * decrement a property's value
     */
    @Procedure(name = "apoc.atomic.subtract", mode = Mode.WRITE)
    @Description("Sets the property of a value to itself minus the given `INTEGER` or `FLOAT` value.\n"
            + "The procedure then sets the property to the returned sum.")
    public Stream<AtomicResults> subtract(
            @Name(
                            value = "container",
                            description =
                                    "The node or relationship that contains the property from which the value will be subtracted.")
                    Object container,
            @Name(
                            value = "propertyName",
                            description = "The name of the property from which the value will be subtracted.")
                    String property,
            @Name(value = "number", description = "The number to subtract.") Number number,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(container);
        Entity entity = Util.rebind(tx, (Entity) container);
        final Number[] newValue = new Number[1];
        final Number[] oldValue = new Number[1];

        final ExecutionContext executionContext = new ExecutionContext(tx, entity);
        retry(
                executionContext,
                (context) -> {
                    oldValue[0] = (Number) entity.getProperty(property);
                    newValue[0] = AtomicUtils.sub((Number) entity.getProperty(property), number);
                    entity.setProperty(property, newValue[0]);
                    return context.entity.getProperty(property);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], newValue[0]));
    }

    /**
     * concat a property's value
     */
    @Procedure(name = "apoc.atomic.concat", mode = Mode.WRITE)
    @Description("Sets the given property to the concatenation of itself and the `STRING` value.\n"
            + "The procedure then sets the property to the returned `STRING`.")
    public Stream<AtomicResults> concat(
            @Name(
                            value = "container",
                            description =
                                    "The node or relationship that contains the property to which the value will be concatenated.")
                    Object container,
            @Name(value = "propertyName", description = "The name of the property to be concatenated.") String property,
            @Name(value = "string", description = "The string value to concatenate with the property.") String string,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(container);
        Entity entity = Util.rebind(tx, (Entity) container);
        final String[] newValue = new String[1];
        final String[] oldValue = new String[1];

        final ExecutionContext executionContext = new ExecutionContext(tx, entity);
        retry(
                executionContext,
                (context) -> {
                    oldValue[0] = entity.getProperty(property).toString();
                    newValue[0] = oldValue[0].concat(string);
                    entity.setProperty(property, newValue[0]);

                    return context.entity.getProperty(property);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], newValue[0]));
    }

    /**
     * insert a value into an array property value
     */
    @Procedure(name = "apoc.atomic.insert", mode = Mode.WRITE)
    @Description("Inserts a value at position into the `LIST<ANY>` value of a property.\n"
            + "The procedure then sets the result back on the property.")
    public Stream<AtomicResults> insert(
            @Name(value = "container", description = "The node or relationship that has a property containing a list.")
                    Object container,
            @Name(
                            value = "propertyName",
                            description = "The name of the property into which the value will be inserted.")
                    String property,
            @Name(value = "position", description = "The position in the list to insert the item into.") Long position,
            @Name(value = "value", description = "The value to insert.") Object value,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(container);
        Entity entity = Util.rebind(tx, (Entity) container);
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final ExecutionContext executionContext = new ExecutionContext(tx, entity);

        retry(
                executionContext,
                (context) -> {
                    oldValue[0] = entity.getProperty(property);
                    List<Object> values = insertValueIntoArray(entity.getProperty(property), position, value);
                    Class clazz;
                    try {
                        clazz = Class.forName(values.toArray()[0].getClass().getName());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    newValue[0] = Array.newInstance(clazz, values.size());
                    try {
                        System.arraycopy(values.toArray(), 0, newValue[0], 0, values.size());
                    } catch (Exception e) {
                        String message = "Property's array value has type: "
                                + values.toArray()[0].getClass().getName() + ", and your value to insert has type: "
                                + value.getClass().getName();
                        throw new ArrayStoreException(message);
                    }
                    entity.setProperty(property, newValue[0]);
                    return context.entity.getProperty(property);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], newValue[0]));
    }

    /**
     * remove a value into an array property value
     */
    @Procedure(name = "apoc.atomic.remove", mode = Mode.WRITE)
    @Description("Removes the element at position from the `LIST<ANY>` value of a property.\n"
            + "The procedure then sets the property to the resulting `LIST<ANY>` value.")
    public Stream<AtomicResults> remove(
            @Name(value = "container", description = "The node or relationship that has a property containing a list.")
                    Object container,
            @Name(
                            value = "propertyName",
                            description = "The name of the property from which the value will be removed.")
                    String property,
            @Name(value = "position", description = "The position in the list to remove the item from.") Long position,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(container);
        Entity entity = Util.rebind(tx, (Entity) container);
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final ExecutionContext executionContext = new ExecutionContext(tx, entity);

        retry(
                executionContext,
                (context) -> {
                    Object[] arrayBackedList = new ArrayBackedList(entity.getProperty(property)).toArray();

                    oldValue[0] = arrayBackedList;
                    if (position > arrayBackedList.length || position < 0) {
                        throw new RuntimeException("Position " + position + " is out of range for array of length "
                                + arrayBackedList.length);
                    }
                    Object[] newArray = ArrayUtils.addAll(
                            Arrays.copyOfRange(arrayBackedList, 0, position.intValue()),
                            Arrays.copyOfRange(arrayBackedList, position.intValue() + 1, arrayBackedList.length));
                    Class clazz;
                    try {
                        clazz = Class.forName(arrayBackedList[0].getClass().getName());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    /*it's not possible to return directly the newArray, we have to create a new array with the specific class*/
                    newValue[0] = Array.newInstance(clazz, newArray.length);
                    System.arraycopy(newArray, 0, newValue[0], 0, newArray.length);
                    entity.setProperty(property, newValue[0]);

                    return context.entity.getProperty(property);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], newValue[0]));
    }

    /**
     * update the property's value
     */
    @Procedure(name = "apoc.atomic.update", mode = Mode.WRITE)
    @Description("Updates the value of a property with a Cypher operation.")
    public Stream<AtomicResults> update(
            @Name(value = "container", description = "The node or relationship with the property to be updated.")
                    Object nodeOrRelationship,
            @Name(value = "propertyName", description = "The name of the property to be updated.") String property,
            @Name(value = "operation", description = "The operation to perform to update the property.")
                    String operation,
            @Name(value = "retryAttempts", defaultValue = "5", description = "The max retry attempts.")
                    Long retryAttempts) {
        checkIsEntity(nodeOrRelationship);
        Entity entity = Util.rebind(tx, (Entity) nodeOrRelationship);
        final Object[] oldValue = new Object[1];
        final ExecutionContext executionContext = new ExecutionContext(tx, entity);

        retry(
                executionContext,
                (context) -> {
                    oldValue[0] = entity.getProperty(property);
                    String statement = "WITH $container AS n WITH n SET n." + Util.sanitize(property, true) + "="
                            + operation + ";";
                    Map<String, Object> properties = MapUtil.map("container", entity);
                    return context.tx.execute(Util.prefixQuery(procedureCallContext, statement), properties);
                },
                retryAttempts);

        return Stream.of(new AtomicResults(entity, property, oldValue[0], entity.getProperty(property)));
    }

    private static class ExecutionContext {
        private final Transaction tx;

        private final Entity entity;

        public ExecutionContext(Transaction tx, Entity entity) {
            this.tx = tx;
            this.entity = entity;
        }
    }

    private List<Object> insertValueIntoArray(Object oldValue, Long position, Object value) {
        List<Object> values = new ArrayList<>();
        if (oldValue.getClass().isArray()) values.addAll(new ArrayBackedList(oldValue));
        else values.add(oldValue);
        if (position > values.size()) values.add(value);
        else values.add(position.intValue(), value);
        return values;
    }

    private void retry(ExecutionContext executionContext, Function<ExecutionContext, Object> work, Long retryAttempts) {
        try {
            tx.acquireWriteLock(executionContext.entity);
            work.apply(executionContext);
        } catch (Neo4jException | NotFoundException | AssertionError e) {
            if (retryAttempts > 0) {
                retry(executionContext, work, retryAttempts - 1);
            } else {
                throw e;
            }
        }
    }

    private void checkIsEntity(Object container) {
        if (!(container instanceof Entity)) throw new RuntimeException("You Must pass Node or Relationship");
    }

    public class AtomicResults {
        @Description("The updated node or relationship.")
        public Object container;

        @Description("The name of the updated property.")
        public String property;

        @Description("The original value on the property.")
        public Object oldValue;

        @Description("The new value on the property.")
        public Object newValue;

        public AtomicResults(Object container, String property, Object oldValue, Object newValue) {
            this.container = container;
            this.property = property;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }
}
