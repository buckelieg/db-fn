package buckelieg.fn.db;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

class BoxedPrimitiveIterable implements Iterable<Number> {

    private Object array;

    BoxedPrimitiveIterable(Object array) {
        this.array = Objects.requireNonNull(array, "Array must be provided");
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("Provided object must be an array");
        }
    }

    @Override
    public Iterator<Number> iterator() {
        return new Iterator<Number>() {

            private AtomicInteger currentIndex = new AtomicInteger();

            @Override
            public boolean hasNext() {
                return array != null && currentIndex.get() < Array.getLength(array);
            }

            @Override
            public Number next() {
                if (array != null) return (Number) Array.get(array, currentIndex.getAndIncrement());
                throw new NoSuchElementException();
            }
        };
    }
}
