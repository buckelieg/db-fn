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
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

@ParametersAreNonnullByDefault
class ProcedureCallQuery extends SelectQuery implements ProcedureCall {

    private Try<CallableStatement, ?, SQLException> storedProcedureResultsHandler;

    ProcedureCallQuery(Statement statement) {
        super(statement);
    }

    @Nonnull
    @Override
    public <T> Select withResultsHandler(Try<CallableStatement, T, SQLException> mapper) {
        this.storedProcedureResultsHandler = Objects.requireNonNull(mapper, "Procedure results extractor must be provided");
        return this;
    }

    @Override
    protected void doExecute() throws SQLException {
        if (((CallableStatement) statement).execute()) {
            this.rs = statement.getResultSet();
        }
    }

    protected boolean doMove() throws SQLException {
        boolean moved = super.doMove();
        if (!moved) {
            try {
                if (statement.getMoreResults()) {
                    rs = statement.getResultSet();
                    return super.doMove();
                }
            } catch (SQLException e) {
                logSQLException("Could not move result set on", e);
            }
            if (storedProcedureResultsHandler != null) {
                try {
                    storedProcedureResultsHandler.doTry((CallableStatement) statement);
                } catch (SQLException e) {
                    throw new RuntimeException("Thrown in procedure results handler", e);
                } finally {
                    close();
                }
            }
        }
        return moved;
    }

}
