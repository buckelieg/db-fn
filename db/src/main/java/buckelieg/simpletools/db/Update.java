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
package buckelieg.simpletools.db;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

public interface Update extends Query<Long> {

    /**
     * Tells this update will be a large update
     *
     * @return an update statement abstraction
     * @see PreparedStatement#executeLargeUpdate
     */
    Update large();

    /**
     * Tells DB to use batch (if possible)
     * @return an update statement abstraction
     * @see DatabaseMetaData#supportsBatchUpdates()
     */
    Update useBatch();
}
