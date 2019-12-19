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

package za.co.absa.spline.consumer.service

import java.util.UUID

import za.co.absa.spline.consumer.service.internal.model.Operation
import za.co.absa.spline.consumer.service.model.AttributeDependencies

import scala.collection.mutable
import collection.JavaConverters._

case class IOSchema(in: Seq[UUID], out: Seq[UUID]) {
  def both: Seq[UUID] = in.union(out).distinct
}

object AttributeDependencySolver {

  def resolveDependencies(allOperations: Seq[Operation], attributeID: UUID):
      AttributeDependencies = {

    val schemaMap = schemaMapOf(allOperations)

    val attributeDependencies = allOperations
      .map(resolveDependencies(_, schemaMap))
      .reduceOption(mergeDependencies)
      .getOrElse(Map.empty)

    val operationDependencies = resolveAttributeOperationDependencies(allOperations, attributeDependencies, schemaMap)

    AttributeDependencies(attributeDependencies(attributeID).toSeq, operationDependencies(attributeID).toSeq)
  }

  private def resolveDependencies(op: Operation, schemaMap: Map[String, IOSchema]): Map[UUID, Set[UUID]] =
    op.extra("name") match {
      case "Project" => resolveExpressionList(op.params("projectList"), schemaMap(op._id).out)
      case "Aggregate" => resolveExpressionList(op.params("aggregateExpressions"), schemaMap(op._id).out)
      case "SubqueryAlias" => resolveSubqueryAlias(schemaMap(op._id).in, schemaMap(op._id).out)
      case "Generate" => resolveGenerator(op)
      case _ => Map.empty
    }

  private def resolveExpressionList(exprList: Any, schema: Seq[UUID]): Map[UUID, Set[UUID]] = {
    val list = exprList.asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala

    list
      .zip(schema)
      .map{ case (expr, attrId) => attrId -> toAttrDependencies(expr.asScala) }
      .toMap
  }

  private def resolveSubqueryAlias(inputSchema: Seq[UUID], outputSchema: Seq[UUID]): Map[UUID, Set[UUID]] = {
    inputSchema
      .zip(outputSchema)
      .map{ case (inAtt, outAtt) => outAtt -> Set(inAtt)}
      .toMap
  }

  private def resolveGenerator(op: Operation): Map[UUID, Set[UUID]] = {

    val expression = op.params("generator").asInstanceOf[java.util.Map[String, Any]].asScala
    val dependencies = toAttrDependencies(expression)

    val keyId = op.params("generatorOutput")
      .asInstanceOf[java.util.List[java.util.Map[String, String]]].asScala
      .head.asScala("refId")

    Map(UUID.fromString(keyId) -> dependencies)
  }

  private def toAttrDependencies(expr: mutable.Map[String, Any]): Set[UUID] = expr("_typeHint") match {
    case "expr.AttrRef"                        => Set(UUID.fromString(expr("refId").asInstanceOf[String]))
    case "expr.Alias"                          => toAttrDependencies(expr("child").asInstanceOf[java.util.Map[String, Any]].asScala)
    case _ if expr.contains("children")        => expr("children")
                                                      .asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
                                                      .map(x => toAttrDependencies(x.asScala)).reduce(_ union _)
    case _                                     => Set.empty
  }

  def mergeDependencies(acc: Map[UUID, Set[UUID]], newDependencies:  Map[UUID, Set[UUID]]): Map[UUID, Set[UUID]] =
    newDependencies.foldLeft(acc) { case (acc, (newKey, newValue)) =>

      // add old dependencies to the new dependencies when they contain one of old keys
      val addToNewValue = acc.flatMap {
        case (k, v)  if newValue(k) => v
        case _                      => Nil
      }

      val updatedNewValue = newValue.union(addToNewValue.toSet)

      // add new dependencies to all dependencies that contains the new key
      val updatedAcc = acc.map {
        case (k, v) if v(newKey) => k -> v.union(updatedNewValue)
        case (k, v)              => k -> v
      }

      updatedAcc + (newKey -> updatedNewValue)
    }

  def schemaMapOf(operations: Seq[Operation]): Map[String, IOSchema] = {

    val operationMap = operations.map(op => op._id -> op).toMap

    def transitiveInputSchemaOf(op: Operation): Seq[UUID] = {
      if (op.childIds.isEmpty) {
        Seq.empty
      } else {
        val inputOp = operationMap(op.childIds.head)
        inputOp.schema
      }
    }

    operations.map { op =>
      val outputSchema = op.schema
      val inputSchema = transitiveInputSchemaOf(op)

      op._id -> IOSchema(inputSchema, outputSchema)
    }.toMap
  }

  def resolveAttributeOperationDependencies(
       operations: Seq[Operation],
       attributeDependencies: Map[UUID, Set[UUID]],
       schemaMap: Map[String, IOSchema]): Map[UUID, Set[String]] = {

    val attributeOperationMap = mutable.Map[UUID, Set[String]]()

    operations.foreach { op =>
      schemaMap(op._id).both.foreach { attr =>
        val opSet = attributeOperationMap.getOrElse(attr, Set.empty)
        attributeOperationMap.update(attr, opSet + op._id)
      }
    }

    val operationDependencies = attributeOperationMap.keys.map { attributeId =>
      val dependentAttributes = attributeDependencies.getOrElse(attributeId, Set.empty) + attributeId

      val dependentOperations = dependentAttributes
        .flatMap(attributeOperationMap.get)
        .fold(Set.empty)(_ union _)

      attributeId -> dependentOperations
    }

    operationDependencies.toMap
  }
}
