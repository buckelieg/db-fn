/*
* Copyright 2016-2017 Anatoly Kutyakov
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
package buckelieg.fn.db;

import javax.annotation.Nonnull;

/**
 * Query abstraction.
 *
 * @param <R> query execution results type
 * @see AutoCloseable
 * @see Select
 * @see Update
 * @see StoredProcedure
 */
public interface Query<R> extends AutoCloseable {

    /**
     * Executes this query with expected results of certain type.
     *
     * @return query execution result
     */
    @Nonnull
    R execute();

    /**
     * Closes this query
     *
     * @see AutoCloseable#close()
     */
    @Override
    default void close() {
    }
}
