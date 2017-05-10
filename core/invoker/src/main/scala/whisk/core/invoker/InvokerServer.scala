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

package whisk.core.invoker

import akka.actor.{Actor, Props}
import spray.httpx.SprayJsonSupport._
import spray.routing.Route
import whisk.common.{Logging, TransactionId}
import whisk.core.connector.ActivationMessage
import whisk.core.entity.WhiskActivation
import whisk.http.BasicRasService

import scala.concurrent.{ExecutionContext, Promise}
/**
 * Implements web server to handle certain REST API calls.
 * Currently provides a health ping route, only.
 */
class InvokerServer(invokerReactive: InvokerReactive, implicit val logging: Logging)
    extends BasicRasService
    with Actor {
    private implicit val executionContext: ExecutionContext = context.dispatcher

    override def actorRefFactory = context
    override def routes(implicit transid: TransactionId): Route = {
        // handleRejections wraps the inner Route with a logical error-handler for unmatched paths
        handleRejections(customRejectionHandler) {
            super.routes ~ invokeRoute
        }
    }
    private val invokeRoute = {
        (path("invoke") & post) {
            entity(as[ActivationMessage]) { msg =>
                val activationPromise = Promise[WhiskActivation]
                val listener = context.actorOf(Props(new Actor{
                    override def receive: Receive = {
                        case activation:WhiskActivation => {
                            activationPromise.trySuccess(activation)
                        }
                    }
                }), "listener-"+msg.activationId)
                implicit val tid = msg.transid
                invokerReactive.onMessage(msg, Some(listener))
                complete {
                    activationPromise.future
                }
            }
        }
    }
}
