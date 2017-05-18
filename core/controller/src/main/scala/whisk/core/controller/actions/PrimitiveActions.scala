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

package whisk.core.controller.actions

import akka.actor.ActorSystem
import spray.json._
import whisk.common.{Logging, LoggingMarkers, StartMarker, TransactionId}
import whisk.core.connector.ActivationMessage
import whisk.core.controller.{WhiskActionsApi, WhiskServices}
import whisk.core.database.NoDocumentException
import whisk.core.entity._
import whisk.core.entity.types.{ActivationStore, EntityStore}
import whisk.utils.ExecutionContextFactory.FutureExtensions

import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

protected[actions] trait PrimitiveActions {
    /** The core collections require backend services to be injected in this trait. */
    services: WhiskServices =>

    /** An actor system for timed based futures. */
    protected implicit val actorSystem: ActorSystem

    /** An execution context for futures. */
    protected implicit val executionContext: ExecutionContext

    protected implicit val logging: Logging

    /** Database service to CRUD actions. */
    protected val entityStore: EntityStore

    /** Database service to get activations. */
    protected val activationStore: ActivationStore

    /** Max duration for active ack. */
    protected val activeAckTimeout = WhiskActionsApi.maxWaitForBlockingActivation

    /**
     * Gets document from datastore to confirm a valid action activation then posts request to loadbalancer.
     * If the loadbalancer accepts the requests with an activation id, then wait for the result of the activation
     * if this is a blocking invoke, else return the activation id.
     *
     * NOTE: This is a point-in-time type of statement:
     * For activations of actions, cause is populated only for actions that were invoked as a result of a sequence activation.
     * For actions that are enclosed in a sequence and are activated as a result of the sequence activation, the cause
     * contains the activation id of the immediately enclosing sequence.
     * e.g.,: s -> a, x, c    and   x -> c  (x and s are sequences, a, b, c atomic actions)
     * cause for a, x, c is the activation id of s
     * cause for c is the activation id of x
     * cause for s is not defined
     *
     * @param subject the subject invoking the action
     * @param docid the action document id
     * @param payload the dynamic arguments for the activation
     * @param timeout the timeout used for polling for result if the invoke is blocking
     * @param blocking true iff this is a blocking invoke
     * @param cause the activation id that is responsible for this invoke/activation
     * @param transid a transaction id for logging
     * @return a promise that completes with (ActivationId, Some(WhiskActivation)) if blocking else (ActivationId, None)
     */
    protected[actions] def invokeSingleAction(
        user: Identity,
        action: WhiskAction,
        payload: Option[JsObject],
        timeout: FiniteDuration,
        blocking: Boolean,
        cause: Option[ActivationId] = None)(
            implicit transid: TransactionId): Future[(ActivationId, Option[WhiskActivation])] = {
        require(action.exec.kind != Exec.SEQUENCE, "this method requires a primitive action")

        // merge package parameters with action (action parameters supersede), then merge in payload
        val args = action.parameters merge payload
        val message = ActivationMessage(
            transid,
            FullyQualifiedEntityName(action.namespace, action.name, Some(action.version)),
            action.rev,
            user,
            activationIdFactory.make(), // activation id created here
            activationNamespace = user.namespace.toPath,
            args,
            cause = cause)

        val start = transid.started(this, LoggingMarkers.CONTROLLER_LOADBALANCER, s"[POST] action activation id: ${message.activationId}")
        val postedFuture = loadBalancer.publish(action, message, activeAckTimeout)
        postedFuture flatMap { activationResponse =>
            transid.finished(this, start)
            //activation was submitted to invoker, but no response yet
            if (blocking) {
                val start2 = transid.started(this, LoggingMarkers.CONTROLLER_LOGIC("waitingResponse"), s"waiting for: ${message.activationId}")
                activationResponse map {
                    //response has been received
                    whiskActivation => {
                        transid.finished(this, start2, s"received response for :${message.activationId}")
                        (whiskActivation.activationId, Some(whiskActivation))
                    }
                }
//                waitForActivationResponse(user, message.activationId, timeout, activationResponse) map {
//                    whiskActivation => (whiskActivation.activationId, Some(whiskActivation))
//                }
            } else {
                // Duration of the non-blocking activation in Controller.
                // We use the start time of the tid instead of a startMarker to avoid passing the start marker around.
                transid.finished(this, StartMarker(transid.meta.start, LoggingMarkers.CONTROLLER_ACTIVATION))
                Future.successful { (message.activationId, None) }
            }
        }
    }

    /**
     * This is a fast path used for blocking calls in which we do not need the full WhiskActivation record from the DB.
     * Polls for the activation response from an underlying data structure populated from Kafka active acknowledgements.
     * If this mechanism fails to produce an answer quickly, the future will switch to polling the database for the response
     * record.
     */
    private def waitForActivationResponse(user: Identity, activationId: ActivationId, totalWaitTime: FiniteDuration, activationResponse: Future[WhiskActivation])(implicit transid: TransactionId) = {
        // this is the promise which active ack or db polling will try to complete in one of four ways:
        // 1. active ack response
        // 2. failing active ack (due to active ack timeout), fall over to db polling
        // 3. timeout on db polling => converts activation to non-blocking (returns activation id only)
        // 4. internal error
        val promise = Promise[WhiskActivation]
        val docid = DocId(WhiskEntity.qualifiedName(user.namespace.toPath, activationId))

        logging.info(this, s"[POST] action activation will block on result up to $totalWaitTime")

        // the active ack will timeout after specified duration, causing the db polling to kick in
        activationResponse map {
            activation => promise.trySuccess(activation)
        } onFailure {
            case t: TimeoutException =>
                logging.info(this, s"[POST] switching to poll db, active ack expired")
                pollDbForResult(docid, activationId, promise)
            case t: Throwable =>
                logging.info(this, s"[POST] switching to poll db, active ack exception: ${t.getMessage}")
                pollDbForResult(docid, activationId, promise)
        }

        val response = promise.future withTimeout (totalWaitTime, new BlockingInvokeTimeout(activationId))

        response onComplete {
            case Success(_) =>
                // Duration of the blocking activation in Controller.
                // We use the start time of the tid instead of a startMarker to avoid passing the start marker around.
                transid.finished(this, StartMarker(transid.meta.start, LoggingMarkers.CONTROLLER_ACTIVATION_BLOCKING))
            case Failure(t) =>
                // short circuits db polling
                promise.tryFailure(t)
        }

        response // will either complete with activation or fail with timeout
    }

    /**
     * Polls for activation record. It is assumed that an activation record is created atomically and never updated.
     * Fetch the activation record by its id. If it exists, complete the promise. Otherwise recursively poll until
     * either there is an error in the get, or the promise has completed because it timed out. The promise MUST
     * complete in the caller to terminate the polling.
     */
    private def pollDbForResult(
        docid: DocId,
        activationId: ActivationId,
        promise: Promise[WhiskActivation])(
            implicit transid: TransactionId): Unit = {
        // check if promise already completed due to timeout expiration (abort polling if so)
        if (!promise.isCompleted) {
            WhiskActivation.get(activationStore, docid) map {
                activation => promise.trySuccess(activation.withoutLogs) // Logs always not provided on blocking call
            } onFailure {
                case e: NoDocumentException =>
                    Thread.sleep(500)
                    logging.debug(this, s"[POST] action activation not yet timed out, will poll for result")
                    pollDbForResult(docid, activationId, promise)
                case t: Throwable =>
                    logging.error(this, s"[POST] action activation failed while waiting on result: ${t.getMessage}")
                    promise.tryFailure(t)
            }
        } else {
            logging.info(this, s"[POST] action activation timed out, terminated polling for result")
        }
    }
}
