/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.common.exception;

public class AoPServiceRuntimeException extends RuntimeException {

    public AoPServiceRuntimeException(String message) {
        super(message);
    }

    public AoPServiceRuntimeException(Exception e) {
        super(e);
    }

    public static class ProducerCreationRuntimeException extends AoPServiceRuntimeException {
        public ProducerCreationRuntimeException(Exception e) {
            super(e);
        }
    }

    public static class ExchangeParameterException extends AoPServiceRuntimeException{
        public ExchangeParameterException(String message) {
            super(message);
        }
    }

    public static class NotSupportedOperationException extends AoPServiceRuntimeException{
        public NotSupportedOperationException(String message) {
            super(message);
        }
    }

    public static class MessageRouteException extends AoPServiceRuntimeException{
        public MessageRouteException(String message) {
            super(message);
        }
    }

    public static class MessageConvertException extends AoPServiceRuntimeException{
        public MessageConvertException(String message) {
            super(message);
        }
    }

}
