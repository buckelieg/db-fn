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
package buckelieg.fn.db;

import java.util.Objects;

@FunctionalInterface
public interface TryFunction<I, O, E extends Throwable> {

    /**
     * Represents some function which might throw an Exception
     *
     * @param input function input.
     * @return mapped value
     * @throws E in case of something went wrong
     */
    O apply(I input) throws E;

    /**
     * Returns a function that always returns its input argument.
     *
     * @param <T> the type of the input and output objects to the function
     * @return a function that always returns its input argument
     */
    static <T> TryFunction<T, T, ?> identity() {
        return t -> t;
    }

    /**
     * Returns a composed function that first applies the {@code before}
     * function to its input, and then applies this function to the result.
     *
     * @param <V>    the type of input to the {@code before} function, and to the
     *               composed function
     * @param before the function to apply before this function is applied
     * @return a composed function that first applies the {@code before}
     * function and then applies this function
     * @throws E                    an exception
     * @throws NullPointerException if before is null
     * @see #andThen(TryFunction)
     */
    default <V> TryFunction<V, O, E> compose(TryFunction<? super V, ? extends I, ? extends E> before) throws E {
        return (V v) -> apply(Objects.requireNonNull(before).apply(v));
    }

    /**
     * Returns a composed function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     *
     * @param <V>   the type of output of the {@code after} function, and of the
     *              composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws E                    an exception
     * @throws NullPointerException if after is null
     * @see #compose(TryFunction)
     */
    default <V> TryFunction<I, V, E> andThen(TryFunction<? super O, ? extends V, ? extends E> after) throws E {
        return (I t) -> Objects.requireNonNull(after).apply(apply(t));
    }
}
