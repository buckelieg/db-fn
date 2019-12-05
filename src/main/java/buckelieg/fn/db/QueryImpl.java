/*
 * Copyright 2016- Anatoly Kutyakov
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.checkAnonymous;
import static buckelieg.fn.db.Utils.setStatementParameters;

@SuppressWarnings("unchecked")
final class QueryImpl extends AbstractQuery<Statement> {

    private boolean isPrepared;

    QueryImpl(Connection connection, String query, Object... params) {
        super(connection, query, params);
    }

    @Override
    Statement prepareStatement(Connection connection, String query, Object... params) throws SQLException {
        return (this.isPrepared = params != null && params.length != 0) ? setStatementParameters(connection.prepareStatement(query), params) : connection.createStatement();
    }

    @Override
    public Void execute() {
        withStatement(s -> isPrepared ? ((PreparedStatement) s).execute() : s.execute(checkAnonymous(query)));
        close(); // force closing this statement since we will not process any of its possible results
        return null;
    }

    @Nonnull
    @Override
    public Query poolable(boolean poolable) {
        return setPoolable(poolable);
    }

    @Nonnull
    @Override
    public Query timeout(int timeout) {
        return setTimeout(timeout);
    }

    @Nonnull
    @Override
    public Query escaped(boolean escapeProcessing) {
        return setEscapeProcessing(escapeProcessing);
    }

    @Nonnull
    @Override
    public Query skipWarnings(boolean skipWarnings) {
        return setSkipWarnings(skipWarnings);
    }

    @Nonnull
    @Override
    public Query print(Consumer<String> printer) {
        return log(printer);
    }
}
