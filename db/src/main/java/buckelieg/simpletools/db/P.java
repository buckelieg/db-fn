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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNullableByDefault;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.SQLType;
import java.util.Objects;

@ParametersAreNullableByDefault
public final class P<T> {
    /**
     * @see java.sql.ParameterMetaData
     */
    private final int mode;
    private final String name;
    private final T value;
    private final SQLType type;

    private P(int mode, SQLType type, @Nonnull String name, T value) {
        this.mode = mode;
        this.type = type;
        this.name = Objects.requireNonNull(name, "Name must not be null");
        this.value = value;
    }

    public static <T> P<T> in(SQLType type, @Nonnull String name, T value) {
        return new P<>(ParameterMetaData.parameterModeIn, type, name, value);
    }

    public static <T> P<T> out(SQLType type, @Nonnull String name) {
        return new P<>(ParameterMetaData.parameterModeOut, type, name, null);
    }

    public static <T> P<T> inOut(SQLType type, @Nonnull String name, T value) {
        return new P<>(ParameterMetaData.parameterModeInOut, type, name, value);
    }

    public static <T> P<T> in(T value) {
        return in(JDBCType.JAVA_OBJECT, "", value);
    }

    public static <T> P<T> in(SQLType type, T value) {
        return in(type, "", value);
    }

    public static <T> P<T> out(SQLType type) {
        return out(type, "");
    }

    public static <T> P<T> inOut(SQLType type, T value) {
        return inOut(type, "", value);
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
        return mode == p.mode &&
                Objects.equals(name, p.name) &&
                Objects.equals(value, p.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, name, value);
    }

    @Override
    public String toString() {
        return String.format("%s:%s=%s", isInOut() ? "INOUT" : isOut() ? "OUT" : "IN", name, value);
    }
}
