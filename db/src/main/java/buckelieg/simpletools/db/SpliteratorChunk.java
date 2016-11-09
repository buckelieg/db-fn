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

import com.sun.rowset.CachedRowSetImpl;

import javax.sql.rowset.CachedRowSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

final class SpliteratorChunk implements Spliterator<ResultSet> {

    private CachedRowSet rows;
    private ImmutableResultSet wrapper;

    public SpliteratorChunk(CachedRowSet rows) {
        this.rows = Objects.requireNonNull(rows);
        this.wrapper = new ImmutableResultSet(rows);
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResultSet> action) {
        try {
            if (rows.next()) {
                action.accept(wrapper.setDelegate(rows));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public Spliterator<ResultSet> trySplit() {
        try {
            if (rows.getRow() <= rows.size() / 2 && rows.size() / 2 < 50) {
                CachedRowSet split = new CachedRowSetImpl();
                split.populate(rows, rows.size() / 2);
                return new SpliteratorChunk(split);
            }
        } catch (SQLException e) {

        }
        return null;
    }

    @Override
    public long estimateSize() {
        return rows.size();
    }

    @Override
    public int characteristics() {
        return Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.SIZED | Spliterator.SUBSIZED;
    }
}
