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

import java.nio.ByteBuffer
import kafka.network.{BoundedByteBufferSend, RequestChannel}
import kafka.network.RequestChannel.Response
import kafka.common.ErrorMapping

object TxCoordinatorMetadataRequest {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer) = {
    // envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = ApiUtils.readShortString(buffer)

    // request
    val txGroupId = ApiUtils.readShortString(buffer)
    TxCoordinatorMetadataRequest(txGroupId, versionId, correlationId, clientId)
  }

}

case class TxCoordinatorMetadataRequest(txGroupId: String,
                                      versionId: Short = TxCoordinatorMetadataRequest.CurrentVersion,
                                      correlationId: Int = 0,
                                      clientId: String = TxCoordinatorMetadataRequest.DefaultClientId)
  extends RequestOrResponse(Some(RequestKeys.TxCoordinatorMetadataKey)) {

  def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    ApiUtils.shortStringLength(clientId) +
    ApiUtils.shortStringLength(txGroupId)

  def writeTo(buffer: ByteBuffer) {
    // envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    ApiUtils.writeShortString(buffer, clientId)

    // transaction coordinator metadata request
    ApiUtils.writeShortString(buffer, txGroupId)
  }

  override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    // return TransactionCoordinatorNotAvailable for all uncaught errors
    val errorResponse = TxCoordinatorMetadataResponse(None, ErrorMapping.TxCoordinatorNotAvailableCode)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  def describe(details: Boolean) = {
    val transactionMetadataRequest = new StringBuilder
    transactionMetadataRequest.append("Name: " + this.getClass.getSimpleName)
    transactionMetadataRequest.append("; Version: " + versionId)
    transactionMetadataRequest.append("; CorrelationId: " + correlationId)
    transactionMetadataRequest.append("; ClientId: " + clientId)
    transactionMetadataRequest.append("; Group: " + txGroupId)
    transactionMetadataRequest.toString()
  }
}
