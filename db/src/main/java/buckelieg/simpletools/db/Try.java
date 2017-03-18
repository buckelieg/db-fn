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

@FunctionalInterface
public interface Try<O, E extends Exception> {

    /**
     * Represents some function which might throw an Exception
     *
     * @return optional value
     * @throws E in case of something went wrong
     */
    O doTry() throws E;

    @FunctionalInterface
    interface Consume<I, E extends Exception> {

        /**
         * Represents some function which might throw an Exception without a return value
         *
         * @throws E an exception
         */
        void doTry() throws E;

        interface _1<I, E extends Exception> {
            /**
             * Represents some function which might throw an Exception without a return value
             *
             * @param input to process
             * @throws E an exception
             */
            void doTry(I input) throws E;
        }

    }

    @FunctionalInterface
    interface _1<I, O, E extends Exception> {

        /**
         * Represents some function which might throw an Exception
         *
         * @param input optional function input.
         * @return value
         * @throws E in case of something went wrong
         */
        O doTry(I input) throws E;

    }
}
