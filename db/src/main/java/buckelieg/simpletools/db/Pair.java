package buckelieg.simpletools.db;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;

@ParametersAreNonnullByDefault
final class Pair<K, V> implements Map.Entry<K, V> {
    private final K key;
    private final V val;

    public Pair(K key, V val) {
        this.key = key;
        this.val = val;
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
        throw new UnsupportedOperationException(String.format("Pair '%s=%s' is read only", key, value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        return key.equals(pair.key) && val.equals(pair.val);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + val.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s=%s", key, val);
    }
}
