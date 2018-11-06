/*
 * Copyright 2017 Barclays Africa Group Limited
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

package za.co.absa.spline.persistence.mongo

import com.mongodb.DuplicateKeyException
import org.slf4s.Logging
import za.co.absa.spline.model.streaming.ProgressEvent
import za.co.absa.spline.persistence.api.ProgressEventWriter
import za.co.absa.spline.persistence.mongo.DBSchemaVersionHelper.serializeWithVersion
import za.co.absa.spline.persistence.mongo.MongoWriterFields._

import scala.concurrent.{ExecutionContext, Future, blocking}

/**
  * The class represents Mongo persistence writer for the [[za.co.absa.spline.model.streaming.ProgressEvent ProgressEvent]] entity.
  *
  * @param connection A connection to Mongo database
  */
class MongoProgressEventWriter(connection: MongoConnection) extends ProgressEventWriter with AutoCloseable with Logging {

  /**
    * The method stores a particular progress event to the persistence layer.
    *
    * @param event A progress event that will be stored
    */
  override def store(event: ProgressEvent)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val dbo = serializeWithVersion[ProgressEvent](event)
    dbo.put(idField, event.id)
    try {
      blocking(connection.eventsCollection.insert(dbo))
    } catch {
      case e: DuplicateKeyException => log.warn("Duplicate key ignored to tolarate potential duplicate insert to MongoDB.", e)
      case e: Throwable => throw e
    }
  }

  /**
    * The method releases resources used by the writer
    */
  override def close(): Unit = connection.close()
}
