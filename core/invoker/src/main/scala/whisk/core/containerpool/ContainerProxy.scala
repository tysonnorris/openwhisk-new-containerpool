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

package whisk.core.containerpool

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Status.{Failure => FailureMessage}
import akka.actor.{ActorRef, FSM, Props, Stash}
import akka.pattern.pipe
import spray.json.DefaultJsonProtocol._
import spray.json._
import whisk.common.TransactionId
import whisk.core.connector.ActivationMessage
import whisk.core.container.Interval
import whisk.core.entity._
import whisk.core.entity.size._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

// States
sealed trait ContainerState
case object Uninitialized extends ContainerState
case object Starting extends ContainerState
case object Started extends ContainerState
case object Running extends ContainerState
case object Ready extends ContainerState
case object Pausing extends ContainerState
case object Paused extends ContainerState
case object Removing extends ContainerState

// Data
sealed abstract class ContainerData(val lastUsed: Instant)
case class NoData() extends ContainerData(Instant.EPOCH)
case class PreWarmedData(container: Container, kind: String) extends ContainerData(Instant.EPOCH)
case class WarmedData(container: Container, namespace: EntityName, action: ExecutableWhiskAction, override val lastUsed: Instant) extends ContainerData(lastUsed)

// Events received by the actor
case class Start(exec: CodeExec[_])
case class Run(action: ExecutableWhiskAction, msg: ActivationMessage, listener:Option[Promise[WhiskActivation]])
case object Remove

// Events sent by the actor
case class NeedWork(data: ContainerData, proxy:ContainerProxy)
case object ContainerPaused
case object ContainerRemoved

/**
 * A proxy that wraps a Container. It is used to keep track of the lifecycle
 * of a container and to guarantee a contract between the client of the container
 * and the container itself.
 *
 * The contract is as follows:
 * 1. Only one job is to be sent to the ContainerProxy at one time. ContainerProxy
 *    will delay all further jobs until the first job is finished for defensiveness
 *    reasons.
 * 2. The next job can be sent to the ContainerProxy after it indicated capacity by
 *    sending NeedWork to its parent.
 * 3. A Remove message can be sent at any point in time. Like multiple jobs though,
 *    it will be delayed until the currently running job has finished.
 *
 * @constructor
 * @param factory a function generating a Container
 * @param sendActiveAck a function sending the activation via active ack
 * @param storeActivation a function storing the activation in a persistent store
 */
class ContainerProxy(
    factory: (TransactionId, String, CodeExec[_], ByteSize) => Future[Container],
    sendActiveAck: (TransactionId, WhiskActivation) => Future[Any],
    storeActivation: (TransactionId, WhiskActivation) => Future[Any],
    pool:ActorRef) extends FSM[ContainerState, ContainerData] with Stash {
    implicit val ec = context.system.dispatcher

    val unusedTimeout = 30.seconds
    val pauseGrace = 1.second

    startWith(Uninitialized, NoData())

    when(Uninitialized) {
        // pre warm a container
        case Event(job: Start, _) =>
            factory(
                TransactionId.invokerWarmup,
                ContainerProxy.containerName("prewarm", job.exec.kind),
                job.exec,
                256.MB)
                .map(container => PreWarmedData(container, job.exec.kind))
                .pipeTo(self)

            goto(Starting)

        // cold start
        case Event(job: Run, _) =>
            implicit val transid = job.msg.transid
            factory(
                job.msg.transid,
                ContainerProxy.containerName(job.msg.user.namespace.name, job.action.name.name),
                job.action.exec,
                job.action.limits.memory.megabytes.MB)
                .andThen {
                    case Success(container) => self ! PreWarmedData(container, job.action.exec.kind)
                    case Failure(t) =>
                        val response = t match {
                            case WhiskContainerStartupError(msg) => ActivationResponse.whiskError(msg)
                            case BlackboxStartupError(msg)       => ActivationResponse.applicationError(msg)
                            case _                               => ActivationResponse.whiskError(t.getMessage)
                        }
                        val activation = ContainerProxy.constructWhiskActivation(job, Interval.zero, response)
                        sendActiveAck(transid, activation)
                        storeActivation(transid, activation)
                }
                .flatMap {
                    container =>
                        run(container, job)
                            .map(_ => WarmedData(container, job.msg.user.namespace, job.action, Instant.now))
                }.pipeTo(self)

            goto(Running)
    }

    when(Starting) {
        // container was successfully obtained
        case Event(data: PreWarmedData, _) =>
            pool ! NeedWork(data, this)
            goto(Started) using data

        // container creation failed
        case Event(_: FailureMessage, _) =>
            pool ! ContainerRemoved
            stop()

        case _ => delay
    }

    when(Started) {
        case Event(job: Run, data: PreWarmedData) =>
            implicit val transid = job.msg.transid
            run(data.container, job)
                .map(_ => WarmedData(data.container, job.msg.user.namespace, job.action, Instant.now))
                .pipeTo(self)

            goto(Running)

        case Event(Remove, data: PreWarmedData) => destroyContainer(data.container)
    }

    when(Running) {
        // Intermediate state, we were able to start a container
        // and we keep it in case we need to destroy it.
        case Event(data: PreWarmedData, _) => stay using data

        // Run was successful
        case Event(data: WarmedData, _) =>
            pool ! NeedWork(data, this)
            goto(Ready) using data

        // Failed after /init (the first run failed)
        case Event(_: FailureMessage, data: PreWarmedData) => destroyContainer(data.container)

        // Failed for a subsequent /run
        case Event(_: FailureMessage, data: WarmedData)    => destroyContainer(data.container)

        // Failed at getting a container for a cold-start run
        case Event(_: FailureMessage, _) =>
            pool ! ContainerRemoved
            stop()

        case _ => delay
    }

    when(Ready, stateTimeout = pauseGrace) {
        case Event(job: Run, data: WarmedData) =>
            implicit val transid = job.msg.transid
            run(data.container, job)
                .map(_ => WarmedData(data.container, job.msg.user.namespace, job.action, Instant.now))
                .pipeTo(self)

            goto(Running)

        // pause grace timed out
        case Event(StateTimeout, data: WarmedData) =>
//            data.container.halt()(TransactionId.invokerNanny).map(_ => ContainerPaused).pipeTo(self)
//            goto(Pausing)
            //logging.info(this, "skipping pause...")
            goto(Ready) using data

        case Event(Remove, data: WarmedData) => destroyContainer(data.container)
    }

    when(Pausing) {
        case Event(ContainerPaused, data: WarmedData) =>
            pool ! NeedWork(data, this)
            goto(Paused)

        case Event(_: FailureMessage, data: WarmedData) => destroyContainer(data.container)
        case _ => delay
    }

    when(Paused, stateTimeout = unusedTimeout) {
        case Event(job: Run, data: WarmedData) =>
            implicit val transid = job.msg.transid
            data.container.resume()
                .flatMap(_ => run(data.container, job))
                .map(_ => WarmedData(data.container, job.msg.user.namespace, job.action, Instant.now))
                .pipeTo(self)

            goto(Running)

//        // timeout or removing
//        case Event(StateTimeout | Remove, data: WarmedData) => {
//            destroyContainer(data.container)
//        }
    }

    when(Removing) {
        case Event(job: Run, _) =>
            // Send the job back to the pool to be rescheduled
            pool ! job
            stay
        case Event(ContainerRemoved, _) => stop()
    }

    // Unstash all messages stashed while in intermediate state
    onTransition {
        case _ -> Started => unstashAll()
        case _ -> Ready   => unstashAll()
        case _ -> Paused  => unstashAll()
    }

    initialize()

    /** Delays all incoming messages until unstashAll() is called */
    def delay = {
        stash()
        stay
    }

    /**
     * Destroys the container after unpausing it if needed. Can be used
     * as a state progression as it goes to Removing.
     *
     * @param container the container to destroy
     */
    def destroyContainer(container: Container) = {
        pool ! ContainerRemoved

        val unpause = stateName match {
            case Paused => container.resume()(TransactionId.invokerNanny)
            case _      => Future.successful(())
        }

        unpause
            .flatMap(_ => container.destroy()(TransactionId.invokerNanny))
            .map(_ => ContainerRemoved).pipeTo(self)

        goto(Removing)
    }

    /**
     * Runs the job, initialize first if necessary.
     *
     * @param container the container to run the job on
     * @param job the job to run
     * @return a future completing after logs have been collected and
     *         added to the WhiskActivation
     */
    def run(container: Container, job: Run)(implicit tid: TransactionId): Future[WhiskActivation] = {
        val actionTimeout = job.action.limits.timeout.duration

        // Only initialize iff we haven't yet warmed the container
        val initialize = stateData match {
            case data: WarmedData => Future.successful(Interval.zero)
            case _                => container.initialize(job.action.containerInitializer, actionTimeout)
        }

        val activation: Future[WhiskActivation] = initialize.flatMap { initInterval =>
            val parameters = job.msg.content getOrElse JsObject()

            val environment = JsObject(
                "api_key" -> job.msg.user.authkey.compact.toJson,
                "namespace" -> job.msg.user.namespace.toJson,
                "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
                "activation_id" -> job.msg.activationId.toString.toJson,
                // compute deadline on invoker side avoids discrepancies inside container
                // but potentially under-estimates actual deadline
                "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

            container.run(parameters, environment, actionTimeout)(job.msg.transid).map {
                case (runInterval, response) =>
                    val initRunInterval = Interval(runInterval.start.minusMillis(initInterval.duration.toMillis), runInterval.end)
                    ContainerProxy.constructWhiskActivation(job, initRunInterval, response)
            }
        }.recover {
            case InitializationError(response, interval) =>
                ContainerProxy.constructWhiskActivation(job, interval, response)
        }

        // Sending active ack and storing the activation are concurrent side-effects
        // and do not block further execution of the future. They are completely
        // asynchronous.
        activation.andThen {
            case Success(activation) => {
                //send to the listener OR activeAck
                job.listener match {
                    case Some(listener) => listener.trySuccess(activation)
                    case _ => sendActiveAck(tid, activation)
                }
            }
            //skip logs for now
//        }.flatMap { activation =>
//            val exec = job.action.exec.asInstanceOf[CodeExec[_]]
//            container.logs(job.action.limits.logs.asMegaBytes, exec.sentinelledLogs).map { logs =>
//                activation.withLogs(ActivationLogs(logs))
//            }
        }.andThen {
            case Success(activation) => storeActivation(tid, activation)
        }.flatMap { activation =>
            // Fail the future iff the activation was unsuccessful to facilitate
            // better cleanup logic.
            if (activation.response.isSuccess) Future.successful(activation)
            else Future.failed(new Exception())
        }
    }
}

object ContainerProxy {
    def props(factory: (TransactionId, String, CodeExec[_], ByteSize) => Future[Container],
              ack: (TransactionId, WhiskActivation) => Future[Any],
              store: (TransactionId, WhiskActivation) => Future[Any],
              pool:ActorRef) = Props(new ContainerProxy(factory, ack, store, pool))

    // Needs to be threadsafe as it's used by multiple proxys concurrently.
    private val count = new AtomicInteger(0)
    private def containerNumber() = count.incrementAndGet()

    /**
     * Generates a unique containername
     *
     * @param prefix the container name's prefix
     * @param suffic the container name's suffix
     * @return a unique container name
     */
    def containerName(prefix: String, suffix: String) =
        s"wsk_${containerNumber()}_${prefix}_${suffix}".replaceAll("[^a-zA-Z0-9_]", "")

    /**
     * Creates a WhiskActivation ready to be sent via active ack.
     *
     * @param job the job that was executed
     * @param interval the time it took to execute the job
     * @param response the response to return to the user
     * @return a WhiskActivation to be sent to the user
     */
    def constructWhiskActivation(job: Run, interval: Interval, response: ActivationResponse) = {
        val causedBy = if (job.msg.causedBySequence) Parameters("causedBy", "sequence".toJson) else Parameters()
        WhiskActivation(
            activationId = job.msg.activationId,
            namespace = job.msg.activationNamespace,
            subject = job.msg.user.subject,
            cause = job.msg.cause,
            name = job.action.name,
            version = job.action.version,
            start = interval.start,
            end = interval.end,
            duration = Some(interval.duration.toMillis),
            response = response,
            annotations = {
                Parameters("limits", job.action.limits.toJson) ++
                    Parameters("path", job.action.fullyQualifiedName(false).toString.toJson) ++ causedBy
            })
    }
}
