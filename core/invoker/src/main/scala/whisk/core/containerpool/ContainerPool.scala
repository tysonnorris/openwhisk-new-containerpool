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

import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.Props
import whisk.common.AkkaLogging
import whisk.core.dispatcher.ActivationFeed.FreeWilly
import whisk.core.entity.CodeExec
import whisk.core.entity.EntityName
import whisk.core.entity.ExecutableWhiskAction

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)

/**
 * A pool managing containers to run actions on.
 *
 * @param childFactory method to create new containers
 * @param poolSize maximum size of containers allowed in the pool
 * @param feed actor to request more work from
 */
class ContainerPool(
    childFactory: ActorRefFactory => ActorRef,
    poolSize: Int,
    feed: ActorRef,
    prewarmConfig: Option[PrewarmingConfig] = None) extends Actor {
    val logging = new AkkaLogging(context.system.log)

    val pool = new mutable.HashMap[ActorRef, WorkerData]
    val rtPool = new mutable.HashMap[ContainerProxy, WorkerData]
    ContainerPool.rtPool = rtPool
    val prewarmedPool = new mutable.HashMap[ActorRef, WorkerData]
    val minimumRemovalAge = 1.seconds //min time since last use for removal eligibility

    prewarmConfig.foreach { config =>
        logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
        (1 to config.count).foreach { _ =>
            prewarmContainer()
        }
    }

    def receive: Receive = {
        // A job to run on a container
        case r: Run =>
            // Schedule a job to a warm container
            ContainerPool.schedule(r.action, r.msg.user.namespace, pool.toMap).orElse {
                // Create a cold container iff there's space in the pool
                if (pool.size < poolSize) {
                    takePrewarmContainer(r.action.exec.kind).orElse {
                        Some(createContainer())
                    }
                } else None
            }.orElse {
                // Remove a container whose last use is before lastUseBefore
                // and create a new one for the given job
                val lastUseBefore = Instant.now.minusSeconds(minimumRemovalAge.toSeconds)
                ContainerPool.remove(r.msg.user.namespace, pool.toMap, lastUseBefore).map { toDelete =>
                    removeContainer(toDelete)
                    createContainer()
                }
            } match {
                case Some(actor) =>
                    pool.get(actor) match {
                        case Some(w) =>
                            pool.update(actor, WorkerData(w.data, Busy))
                            actor ! r
                        case None =>
                            logging.error(this, "actor data not found")
                            self ! r
                    }
                case None =>
                    // "reenqueue" the request to find a container at a later point in time
                    self ! r
            }

        // Container is free to take more work
        case NeedWork(data: WarmedData, proxy:ContainerProxy) =>
            pool.update(sender(), WorkerData(data, Free))
            logging.info(this, "adding to rtPool:"+data.container)
            rtPool.update(proxy, WorkerData(data, Free))
            feed ! FreeWilly

        // Container is prewarmed and ready to take work
        case NeedWork(data: PreWarmedData, proxy:ContainerProxy) =>
            prewarmedPool.update(sender(), WorkerData(data, Free))

        // Container got removed
        case ContainerRemoved =>
            pool.remove(sender())

    }

    /** Creates a new container and updates state accordingly. */
    def createContainer() = {
        val ref = childFactory(context)
        pool.update(ref, WorkerData(NoData(), Free))
        ref
    }

    /** Creates a new prewarmed container */
    def prewarmContainer() = prewarmConfig.foreach(config => childFactory(context) ! Start(config.exec))

    /**
     * Takes a prewarm container out of the prewarmed pool
     * iff a container with a matching kind is found.
     *
     * @param kind the kind you want to invoke
     * @return the container iff found
     */
    def takePrewarmContainer(kind: String) = prewarmConfig.flatMap { config =>
        prewarmedPool.find {
            case (_, WorkerData(PreWarmedData(_, `kind`), _)) => true
            case _ => false
        }.map {
            case (ref, data) =>
                // Move the container to the usual pool
                pool.update(ref, data)
                prewarmedPool.remove(ref)
                // Create a new prewarm container
                prewarmContainer()

                ref
        }
    }

    /** Removes a container and updates state accordingly. */
    def removeContainer(toDelete: ActorRef) = {
        toDelete ! Remove
        pool.remove(toDelete)
    }
}

object ContainerPool {

    var rtPool:mutable.HashMap[ContainerProxy, WorkerData] = null

    /**
     * Finds the best container for a given job to run on.
     *
     * Selects an arbitrary warm container from the passed pool of idle containers
     * that matches the action and the invocation namespace. The implementation uses
     * matching such that structural equality of action and the invocation namespace
     * is required.
     * Returns None iff no matching container is in the idle pool.
     * Does not consider pre-warmed containers.
     *
     * @param action the action to run
     * @param namespace the namespace, that wants to run the action
     * @param idles a map of idle containers, awaiting work
     * @return a container if one found
     */
    def schedule[A](action: ExecutableWhiskAction, namespace: EntityName, idles: Map[A, WorkerData]): Option[A] = {
        idles.find {
            case (_, WorkerData(WarmedData(_, `namespace`, `action`, _), _)) => true
            case _ => false
        }.map(_._1)
    }

//    def scheduleRt[A](action: ExecutableWhiskAction, namespace: EntityName): Option[A] = {
//        (rtPool.toMap).find {
//            case (_, WorkerData(WarmedData(_, `namespace`, `action`, _), _)) => true
//            case _ => false
//        }.map(_._1)
//    }
    /**
     * Finds the best container to remove to make space for the job passed to run.
     *
     * Determines which namespace consumes most resources in the current pool and
     * takes away one of their containers iff the namespace placing the new job is
     * not already the most consuming one.
     *
     * @param namespace the namespace that wants to get a container
     * @param pool a map of all containers in the pool
     * @return a container to be removed iff found
     */
    def remove[A](namespace: EntityName, pool: Map[A, WorkerData], lastUseBefore:Instant): Option[A] = {
        val grouped = pool.collect {
            case (ref, WorkerData(w: WarmedData, _)) if w.lastUsed.toEpochMilli < lastUseBefore.toEpochMilli  => ref -> w
        }.groupBy {
            case (ref, data) => data.namespace
        }

        if (!grouped.isEmpty) {
            val (maxConsumer, containersToDelete) = grouped.maxBy(_._2.size)
            val (ref, _) = containersToDelete.minBy(_._2.lastUsed)
            Some(ref)
        } else None
    }

    def props(factory: ActorRefFactory => ActorRef,
              size: Int,
              feed: ActorRef,
              prewarmConfig: Option[PrewarmingConfig] = None) = Props(new ContainerPool(factory, size, feed, prewarmConfig))
}

case class PrewarmingConfig(count: Int, exec: CodeExec[Any])
