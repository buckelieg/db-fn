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

class SQLRuntimeException extends RuntimeException {

    private String message;

    public SQLRuntimeException() {
        super();
    }

    public SQLRuntimeException(String message) {
        super(message);
    }

    public SQLRuntimeException(String message, boolean fillStackTrace) {
        this.message = message;
        if (fillStackTrace) {
            fillInStackTrace();
        }
    }

    public SQLRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLRuntimeException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return message;
    }
}