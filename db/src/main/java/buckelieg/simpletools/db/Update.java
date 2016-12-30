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

/**
 * An abstraction for DML queries (INSERT/UPDATE/DELETE)
 */
public interface Update extends Query<Integer> {

    /**
     * Marks that this update should be executed within single transaction.
     * Useful for batch operations.
     * <p>
     * By default (i.e. when this methods is not called) every single update is executed in its own transaction.
     *
     * @return update query
     */
    Update transacted();

    /**
     * Executes one of INSERT, UPDATE or DELETE statements.
     *
     * @return affected rows
     */
    Integer execute();

}
