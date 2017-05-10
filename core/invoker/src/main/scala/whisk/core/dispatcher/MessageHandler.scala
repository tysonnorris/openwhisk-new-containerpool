/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.dispatcher

import akka.actor.ActorRef

import scala.concurrent.Future
import whisk.common.TransactionId
import whisk.core.connector.{ActivationMessage => Message}

/**
 * Abstract base class for a handler for a connector (e.g., Kafka) message.
 */
abstract class MessageHandler(val name: String) {

    /**
     * Runs handler for a Kafka message. This method is run inside a future.
     * If the method fails with an exception, the exception completes
     * the wrapping future within which the method is run.
     *
     * @param msg the Message object to process
     * @param transid the transaction id for the Kafka message
     * @return Future that executes the handler
     */
    def onMessage(msg: Message,listener:Option[ActorRef] = None)(implicit transid: TransactionId): Future[Any]
}
