/*
 * Copyright 2019 ABSA Group Limited
 *
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

package za.co.absa.spline.persistence

import com.arangodb.ArangoDBException
import za.co.absa.spline.persistence.ArangoCode._

import scala.PartialFunction.condOpt

object RetryableException {

  private val retryableCodes = Set(
    ArangoConflict,
    ArangoUniqueConstraintViolated,
    Deadlock,
    ArangoSyncTimeout,
    LockTimeout,
    ArangoWriteThrottleTimeout,
    ClusterTimeout)
    .map(_.code)

  def unapply(exception: Exception): Option[ArangoDBException] = condOpt(exception) {
    case e: ArangoDBException if retryableCodes(e.getErrorNum) => e
  }

}
