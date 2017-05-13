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

package whisk.core.containerpool.docker

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRefFactory
import spray.client.pipelining._
import spray.http.HttpRequest
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.container.{HttpUtils, Interval, RunResult}
import whisk.core.containerpool.{BlackboxStartupError, Container, InitializationError, WhiskContainerStartupError}
import whisk.core.entity.ActivationResponse.ContainerResponse
import whisk.core.entity.size._
import whisk.core.entity.{ActivationResponse, ByteSize}
import whisk.core.invoker.ActionLogDriver
import whisk.http.Messages

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DockerContainer {
    /**
     * Creates a container running on a docker daemon.
     *
     * @param transid transaction creating the container
     * @param image image to create the container from
     * @param userProvidedImage whether the image is provided by the user
     *     or is an OpenWhisk provided image
     * @param memory memorylimit of the container
     * @param cpuShares sharefactor for the container
     * @param environment environment variables to set on the container
     * @param network network to launch the container in
     * @param name optional name for the container
     * @return a container
     */
    def create(transid: TransactionId,
               image: String,
               userProvidedImage: Boolean = false,
               memory: ByteSize = 256.MB,
               cpuShares: Int = 0,
               environment: Map[String, String] = Map(),
               network: String = "bridge",
               name: Option[String] = None)(
                   implicit docker: DockerApiWithFileAccess, runc: RuncApi, ec: ExecutionContext, log: Logging, actorFactory: ActorRefFactory) = {
        implicit val tid = transid

        val environmentArgs = (environment + ("SERVICE_IGNORE" -> true.toString)).map {
            case (key, value) => Seq("-e", s"$key=$value")
        }.flatten

        val args = Seq(
            "--cap-drop", "NET_RAW",
            "--cap-drop", "NET_ADMIN",
            "--ulimit", "nofile=1024:1024",
            "--pids-limit", "1024", // OW PR 2119
            "--cpu-shares", cpuShares.toString,
            "--memory", s"${memory.toMB}m",
            "--memory-swap", s"${memory.toMB}m",
            "--network", network) ++
            environmentArgs ++
            name.map(n => Seq("--name", n)).getOrElse(Seq.empty)

        val pulled = if (userProvidedImage) {
            docker.pull(image).recoverWith {
                case _ => Future.failed(BlackboxStartupError(s"Failed to pull container image '$image'."))
            }
        } else Future.successful(())

        val container = for {
            _ <- pulled
            id <- docker.run(image, args)
            ip <- docker.inspectIPAddress(id, network).andThen {
                // remove the container immediatly if inspect failed as
                // we cannot recover that case automatically
                case Failure(_) => docker.rm(id)
            }
        } yield new DockerContainer(id, ip)

        container.recoverWith {
            case t => if (userProvidedImage) {
                Future.failed(BlackboxStartupError(t.getMessage))
            } else {
                Future.failed(WhiskContainerStartupError(t.getMessage))
            }
        }
    }
}

/**
 * Represents a container as run by docker.
 *
 * This class contains OpenWhisk specific behavior and as such does not necessarily
 * use docker commands to achieve the effects needed.
 *
 * @constructor
 * @param id the id of the container
 * @param ip the ip of the container
 */
class DockerContainer(id: ContainerId, ip: ContainerIp)(
    implicit docker: DockerApiWithFileAccess, runc: RuncApi, ec: ExecutionContext, log: Logging, actorFactory: ActorRefFactory) extends Container with ActionLogDriver {

    /** The last read-position in the log file */
    private var logFileOffset = 0L

    protected val logsRetryCount = 15
    protected val logsRetryWait = 100.millis

    /** HTTP connection to the container, will be lazily established by callContainer */
    private var httpConnection: Option[HttpUtils] = None

    def halt()(implicit transid: TransactionId): Future[Unit] = runc.pause(id)
    def resume()(implicit transid: TransactionId): Future[Unit] = runc.resume(id)
    def destroy()(implicit transid: TransactionId): Future[Unit] = docker.rm(id)

    def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Interval] = {
        val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_INIT, s"sending initialization to $id $ip")

        val body = JsObject("value" -> initializer)
        callContainer("/init", body, timeout, retry = true).andThen { // never fails
            case Success(r: RunResult) =>
                transid.finished(this, start.copy(start = r.interval.start), s"initialization result: ${r.toBriefString}", endTime = r.interval.end)
            case Failure(t) =>
                transid.failed(this, start, s"initializiation failed with $t")
        }.flatMap { result =>
            if (result.ok) {
                Future.successful(result.interval)
            } else if (result.interval.duration >= timeout) {
                Future.failed(InitializationError(ActivationResponse.applicationError(Messages.timedoutActivation(timeout, true)), result.interval))
            } else {
                Future.failed(InitializationError(ActivationResponse.processInitResponseContent(result.response, log), result.interval))
            }
        }
    }

    def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {
        val actionName = environment.fields.get("action_name").map(_.convertTo[String]).getOrElse("")
        val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_RUN, s"sending arguments to $actionName at $id $ip")

        val parameterWrapper = JsObject("value" -> parameters)
        val body = JsObject(parameterWrapper.fields ++ environment.fields)
            callContainer("/run", body, timeout, retry = false).andThen { // never fails
            case Success(r: RunResult) =>
                transid.finished(this, start.copy(start = r.interval.start), s"running result: ${r.toBriefString}", endTime = r.interval.end)
            case Failure(t) =>
                transid.failed(this, start, s"initializiation failed with $t")
        }.map { result =>
            val response = if (result.interval.duration >= timeout) {
                ActivationResponse.applicationError(Messages.timedoutActivation(timeout, false))
            } else {
                ActivationResponse.processRunResponseContent(result.response, log)
            }

            (result.interval, response)
        }
    }

    /**
     * Obtains the container's stdout and stderr output and converts it to our own JSON format.
     * At the moment, this is done by reading the internal Docker log file for the container.
     * Said file is written by Docker's JSON log driver and has a "well-known" location and name.
     *
     * For warm containers, the container log file already holds output from
     * previous activations that have to be skipped. For this reason, a starting position
     * is kept and updated upon each invocation.
     *
     * If asked, check for sentinel markers - but exclude the identified markers from
     * the result returned from this method.
     *
     * Only parses and returns as much logs as fit in the passed log limit.
     * Even if the log limit is exceeded, advance the starting position for the next invocation
     * behind the bytes most recently read - but don't actively read any more until sentinel
     * markers have been found.
     *
     * @param limit the limit to apply to the log size
     * @param waitForSentinel determines if the processor should wait for a sentinel to appear
     *
     * @return a vector of Strings with log lines in our own JSON format
     */
    def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Future[Vector[String]] = {

        def readLogs(retries: Int): Future[Vector[String]] = {
            docker.rawContainerLogs(id, logFileOffset).flatMap { rawLogBytes =>
                val rawLog = new String(rawLogBytes.array, rawLogBytes.arrayOffset, rawLogBytes.position, StandardCharsets.UTF_8)
                val (isComplete, isTruncated, formattedLogs) = processJsonDriverLogContents(rawLog, waitForSentinel, limit)

                if (retries > 0 && !isComplete && !isTruncated) {
                    log.info(this, s"log cursor advanced but missing sentinel, trying $retries more times")
                    Thread.sleep(logsRetryWait.toMillis)
                    readLogs(retries - 1)
                } else {
                    logFileOffset += rawLogBytes.position - rawLogBytes.arrayOffset
                    Future.successful(formattedLogs)
                }
            }.andThen {
                case Failure(e) =>
                    log.error(this, s"Failed to obtain logs of ${id.asString}: ${e.getClass} - ${e.getMessage}")
            }
        }

        readLogs(logsRetryCount)
    }

    /**
     * Makes an HTTP request to the container.
     *
     * Note that `http.post` will not throw an exception, hence the generated Future cannot fail.
     *
     * @param path relative path to use in the http request
     * @param body body to send
     * @param timeout timeout of the request
     * @param retry whether or not to retry the request
     */
    val pipeline: HttpRequest => Future[String] = (
      sendReceive
        ~> unmarshal[String]
      )
    val requestCounter = new AtomicInteger(0)
    protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false)(implicit context:ActorRefFactory): Future[RunResult] = {
        val started = Instant.now()
//        val http = httpConnection.getOrElse {
//            val conn = new HttpUtils(s"${ip.asString}:8080", timeout, 1.MB)
//            httpConnection = Some(conn)
//            conn
//        }
//        Future {
//            http.post(path, body, retry)
//        }.map { response =>
//            val finished = Instant.now()
//            RunResult(Interval(started, finished), response)
//        }
//        see http://kamon.io/teamblog/2014/11/02/understanding-spray-client-timeout-settings/
        log.info(this, s"###### starting request to container ${requestCounter.incrementAndGet()}")
        val request = Post(s"http://${ip.asString}:8080${path}", body)
        pipeline(request).map (responseBody => {
            val finished = Instant.now()
            log.info(this, s"###### ending request to container ${requestCounter.decrementAndGet()}")
            RunResult(Interval(started, finished), Right(ContainerResponse(true, responseBody)))
        })

    }
}
