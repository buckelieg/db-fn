/*
* Copyright 2016 Anatoly Kutyakov
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
package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;

@ParametersAreNonnullByDefault
public interface DB {

    /**
     * Calls stored procedure. Supplied params are considered as IN parameters
     *
     * @param query  procedure call string
     * @param params procedure IN parameters
     * @return procedure call builder
     * @see ProcedureCall
     */
    @Nonnull
    ProcedureCall call(String query, Object... params);

    /**
     * Calls stored procedure.
     *
     * @param query  procedure call string
     * @param params procedure parameters as declared (IN/OUT/INOUT)
     * @return procedure call builder
     * @see ProcedureCall
     */
    @Nonnull
    ProcedureCall call(String query, P<?>... params);

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param query  SELECT query to execute. Can be WITH query
     * @param params query parameters on the declared order of '?'
     * @return select query builder
     * @see Select
     */
    @Nonnull
    Select select(String query, Object... params);

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return select query builder
     * @see Select
     */
    @Nonnull
    Select select(String query, Map<String, ?> namedParams);

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @param <T>         type bounds
     * @return select query builder
     * @see Select
     */
    @Nonnull
    <T extends Map.Entry<String, ?>> Select select(String query, T... namedParams);

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query  INSERT/UPDATE/DELETE query to execute.
     * @param params query parameters on the declared order of '?'
     * @return affected rows
     */
    int update(String query, Object... params);

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query       INSERT/UPDATE/DELETE query to execute.
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @param <T>         type bounds
     * @return affected rows
     */
    <T extends Map.Entry<String, ?>> int update(String query, T... namedParams);

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query named parameters. Parameter name in the form of :name
     * @return affected rows
     */
    int update(String query, Map<String, ?>... batch);
}
