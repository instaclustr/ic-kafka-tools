/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.api

import java.nio._
import kafka.api.ApiUtils._
import kafka.common._
import kafka.network.RequestChannel.Response
import kafka.network.{RequestChannel, BoundedByteBufferSend}
import collection.mutable.{LinkedHashMap, LinkedHashSet}


object TransactionRequest {
  val CurrentVersion = 0.shortValue

  def readFrom(buffer: ByteBuffer): TransactionRequest = {
    val versionId: Short = buffer.getShort
    val correlationId: Int = buffer.getInt
    val clientId: String = readShortString(buffer)
    val ackTimeoutMs: Int = buffer.getInt
    val requestInfo = TransactionRequestInfo.readFrom(buffer)

    TransactionRequest(versionId, correlationId, clientId, ackTimeoutMs, requestInfo)
  }

  def transactionRequestWithNewControl(oldTxRequest: TransactionRequest, newTxControl: Short): TransactionRequest = {
    oldTxRequest.copy(requestInfo = oldTxRequest.requestInfo.copy(txControl = newTxControl))
  }
}




object TxRequestTypes {
  // The following flags are set in the message header of both data records and transaction control records as described below
  val Ongoing: Short = 0   // Sent from Producer to data brokers (i.e., set in the header of the actual data records)
  val Begin: Short = 1     // Sent from Producer to Transaction Manager, appended to transactionTopic.
  val PreCommit: Short = 2 // Sent from Producer to Transaction Manager, appended to transactionTopic.
  val Commit: Short = 3    // Sent from Transaction Manager to Brokers, appended to topic partitions. (Also read by consumer.)
  val Committed: Short = 4 // Appended to transaction control topic after Transaction Manager receives a successful response from "Commit" requests.
  val PreAbort: Short = 5  // Similar to "PreCommit".
  val Abort: Short = 6     // Similar to "Commit".
  val Aborted: Short = 7   // Similar to "Committed".
}


case class TransactionRequest(versionId: Short = TransactionRequest.CurrentVersion,
                              correlationId: Int,
                              clientId: String,
                              ackTimeoutMs: Int,
                              requestInfo: TransactionRequestInfo)
    extends RequestOrResponse(Some(RequestKeys.TransactionKey)) {

  def this(correlationId: Int,
           clientId: String,
           ackTimeoutMs: Int,
           requestInfo: TransactionRequestInfo) =
    this(TransactionRequest.CurrentVersion, correlationId, clientId, ackTimeoutMs, requestInfo)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
    requestInfo.writeTo(buffer)
  }

  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) + /* client id */
    4 + /* ackTimeoutMs */
    requestInfo.sizeInBytes
  }

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {

    val transactionResponseStatus = requestInfo.txPartitions.map {
      topicAndPartition => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    val errorResponse = TransactionResponse(correlationId, requestInfo.txId, transactionResponseStatus.toMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  def responseFor(status: Map[TopicAndPartition, Short]) = {
    TransactionResponse(correlationId, requestInfo.txId, status);
  }

  override def describe(details: Boolean): String = {
    val transactionRequest = new StringBuilder
    transactionRequest.append("Name: " + this.getClass.getSimpleName)
    transactionRequest.append("; Version: " + versionId)
    transactionRequest.append("; CorrelationId: " + correlationId)
    transactionRequest.append("; ClientId: " + clientId)
    transactionRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms")
    if (details)
      transactionRequest.append("; requestInfos: " + requestInfo)
    transactionRequest.toString()
  }
}

object TransactionRequestInfo {

  def readFrom(buffer: ByteBuffer): TransactionRequestInfo = {
    val txGroupId: String = readShortString(buffer)
    val txId: Int = buffer.getInt
    val txControl: Short = buffer.getShort
    val txTimeoutMs: Int = buffer.getInt

    val topicCount = buffer.getInt
    val txPartitions = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt
        TopicAndPartition(topic, partition)
      })
    }).toList

    TransactionRequestInfo(txGroupId, txId, txControl, txTimeoutMs, txPartitions)
  }
}

case class TransactionRequestInfo(txGroupId: String, txId: Int, txControl: Short, txTimeoutMs: Int,
                                  txPartitions: Seq[TopicAndPartition]) {

  private lazy val partitionsGroupedByTopic = txPartitions.groupBy(_.topic)

  def sizeInBytes: Int = {
    shortStringLength(txGroupId) + /* groupId */
    4 + /* txId */
    2 + /* txControl */
    4 + /* txTimeoutMs */
    4 + /* number of topics */
    partitionsGroupedByTopic.foldLeft(0)((foldedTopics, topicAndPartitions) => {
      foldedTopics +
      shortStringLength(topicAndPartitions._1) + /* topic */
      4 + /* number of partitions */
      4 * topicAndPartitions._2.size /* partitions */
    })
  }

  def writeTo(buffer: ByteBuffer) {
    writeShortString(buffer, txGroupId)
    buffer.putInt(txId)
    buffer.putShort(txControl)
    buffer.putInt(txTimeoutMs)
    buffer.putInt(partitionsGroupedByTopic.size)
    partitionsGroupedByTopic.foreach {
      case (topic, topicAndPartitions) =>
        writeShortString(buffer, topic) //write the topic
        buffer.putInt(topicAndPartitions.size) //the number of partitions
        topicAndPartitions.foreach {topicAndPartition: TopicAndPartition =>
          buffer.putInt(topicAndPartition.partition)
        }
    }
  }

  override def toString(): String = {
    val requestInfo = new StringBuilder
    requestInfo.append("gId: " + txGroupId)
    requestInfo.append("; txId: " + txId)
    requestInfo.append("; txControl: " + txControl)
    requestInfo.append("; txTimeoutMs: " + txTimeoutMs)
    requestInfo.append("; TopicAndPartition: (" + txPartitions.mkString(",") + ")")
    requestInfo.toString()
  }

}
