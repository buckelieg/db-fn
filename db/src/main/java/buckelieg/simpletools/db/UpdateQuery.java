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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

final class UpdateQuery extends AbstractQuery implements Update {

    UpdateQuery(Statement statement) {
        super(statement);
    }

    @Override
    public Integer execute() {
        int rows = 0;
        try {
            rows = ((PreparedStatement) statement).executeUpdate();
        } catch (SQLException e) {
            logSQLException(String.format("Could not execute statement '%s'", statement), e);
        } finally {
            close();
        }
        return rows;
    }
}
