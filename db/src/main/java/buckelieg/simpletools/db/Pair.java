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

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;
import java.util.Objects;

@ParametersAreNonnullByDefault
final class Pair<K, V> implements Map.Entry<K, V> {
    private final K key;
    private final V val;

    private Pair(K key, @Nullable V val) {
        this.key = Objects.requireNonNull(key, "Key must be provided");
        this.val = val;
    }

    static <K, V> Pair<K, V> of(K key, V val) {
        return new Pair<>(key, val);
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return val;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException(String.format("Pair '%s' is read only", toString()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(key, pair.key) &&
                Objects.equals(val, pair.val);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, val);
    }

    @Override
    public String toString() {
        return String.format("%s=%s", key, val);
    }
}
