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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.SQLType;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Stored Procedure parameter
 *
 * @param <T> parameter value type
 * @see StoredProcedure
 */
@ParametersAreNonnullByDefault
public final class P<T> {
    /**
     * @see java.sql.ParameterMetaData
     */
    private final int mode;
    private final String name;
    private final T value;
    private final SQLType type;

    private P(int mode, SQLType type, String name, @Nullable T value) {
        this.mode = mode;
        this.type = requireNonNull(type, "Parameter type must be provided");
        this.name = requireNonNull(name, "Parameter name must be provided");
        this.value = value;
    }

    public static <T> P<T> in(SQLType type, String name, T value) {
        return new P<>(ParameterMetaData.parameterModeIn, type, name, value);
    }

    public static <T> P<T> in(T value) {
        return in(JDBCType.JAVA_OBJECT, "", value);
    }

    public static <T> P<T> in(SQLType type, T value) {
        return in(type, "", value);
    }

    public static <T> P<T> in(String name, T value) {
        return in(JDBCType.JAVA_OBJECT, name, value);
    }

    public static <T> P<T> out(SQLType type, String name) {
        return new P<>(ParameterMetaData.parameterModeOut, type, name, null);
    }

    public static <T> P<T> out(SQLType type) {
        return out(type, "");
    }

    public static <T> P<T> out(String name) {
        return out(JDBCType.JAVA_OBJECT, name);
    }

    public static <T> P<T> inOut(SQLType type, String name, T value) {
        return new P<>(ParameterMetaData.parameterModeInOut, type, name, value);
    }

    public static <T> P<T> inOut(SQLType type, T value) {
        return inOut(type, "", value);
    }

    public static <T> P<T> inOut(String name, T value) {
        return inOut(JDBCType.JAVA_OBJECT, name, value);
    }

    public static <T> P<T> inOut(T value) {
        return inOut(JDBCType.JAVA_OBJECT, "", value);
    }

    public boolean isIn() {
        return mode == ParameterMetaData.parameterModeIn || mode == ParameterMetaData.parameterModeInOut;
    }

    public boolean isOut() {
        return mode == ParameterMetaData.parameterModeOut || mode == ParameterMetaData.parameterModeInOut;
    }

    public boolean isInOut() {
        return mode == ParameterMetaData.parameterModeInOut;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nullable
    public T getValue() {
        return value;
    }

    @Nullable
    public SQLType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P<?> p = (P<?>) o;
        return mode == p.mode && name.equals(p.name) && value.equals(p.value) && type.equals(p.type);
    }

    @Override
    public int hashCode() {
        int result = mode;
        result = 31 * result + name.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s:%s=%s(%s)", isInOut() ? "INOUT" : isOut() ? "OUT" : "IN", name, value, type.getName());
    }
}
