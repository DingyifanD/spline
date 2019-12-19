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

package za.co.absa.spline.consumer.service.internal.model

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}
import collection.JavaConverters._
import za.co.absa.spline.consumer.service.AttributeDependencySolver.resolveDependencies

class AttributeDependencySolverSpec extends FlatSpec with Matchers {

  private val A, B, C, D, E, F, G = UUID.randomUUID()
  private val attrNames = Map(A -> "A", B -> "B", C -> "C", D -> "D", E -> "E", F -> "F", G -> "G")

  private def assertEqualAttr(actual: Seq[UUID], expected: Set[UUID]): Unit = {

    def toPrettyNames(deps: Set[UUID]) = deps.map(attrNames)

    toPrettyNames(actual.toSet) shouldEqual toPrettyNames(expected)
  }

  private def assertEqualOp(actual: Seq[String], expected: Set[Int]): Unit = {

    actual.map(_.toInt).toSet shouldEqual expected
  }

  private def exprAsJava(expressions: Any): Any = expressions match {
    case e: Seq[Any] => e.map(exprAsJava).asJava
    case e: Map[String, Any] => mapToJava(e)
    case e: Any => e
  }

  private def mapToJava(m: Map[String, Any]): java.util.Map[String, Any] = m
    .map{ case (k, v) => k -> exprAsJava(v)}
    .asJava


  private def toInput(id: Int, schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray, Map("name" -> "Read"), Map.empty,Seq.empty)
    //ReadOperation(Seq(), id, Some(schema))

  private def toOutput(id: Int, childIds: Seq[Int]) =
    Operation(id.toString, Array.empty, Map("name" -> "Write"), Map.empty, childIds.map(_.toString))
    //WriteOperation("", true, id, childIds)

  private def toSelect(id: Int, childIds: Seq[Int], expressions: Seq[Map[String, Any]], schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray,
      Map("name" -> "Project"),
      Map("projectList" -> exprAsJava(expressions)),
      childIds.map(_.toString))
  /*
  DataOperation(id, childIds, if (schema.isEmpty) None else Some(schema),
      Map(OperationParams.Transformations -> Some(expressions)),
      Map(OperationExtras.Name -> "Project"))
  */

  private def toAggregate(id: Int, childIds: Seq[Int], expressions: Seq[Map[String, Any]], schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray,
      Map("name" -> "Aggregate"),
      Map("aggregateExpressions" -> exprAsJava(expressions)),
      childIds.map(_.toString))

  private def toGenerate(id: Int, childIds: Seq[Int], generator: Map[String, Any], outputId: UUID) =
    Operation(id.toString, Array(outputId),
      Map("name"-> "Generate"),
      Map("generator" -> exprAsJava(generator), "generatorOutput" -> exprAsJava(Seq(attrRef(outputId)))),
      childIds.map(_.toString))

  private def toFilter(id: Int, childIds: Seq[Int], schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray, Map("name" -> "Filter"), Map.empty, childIds.map(_.toString))
  //DataOperation(id, childIds, None, Map.empty, Map(OperationExtras.Name -> "Filter"))

  private def toJoin(id: Int, childIds: Seq[Int], schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray, Map("name" -> "Join"), Map.empty, childIds.map(_.toString))
  //DataOperation(id, childIds, Some(schema), Map.empty, Map(OperationExtras.Name -> "Join"))


  private def toSubqueryAlias(id: Int, childIds: Seq[Int], schema: Seq[UUID]) =
    Operation(id.toString, schema.toArray, Map("name" -> "SubqueryAlias"), Map.empty, childIds.map(_.toString))
  //DataOperation(id, childIds, Some(schema), Map.empty, Map(OperationExtras.Name -> "SubqueryAlias"))


  private def attrRef(attr: UUID) =
    Map("_typeHint" -> "expr.AttrRef", "refId" -> attr.toString)

  private def attrOperation(children: Seq[Any]) =
    Map("_typeHint" -> "dummt", "children" -> children)

  behavior of "AttributeDependencySolver"

  it should "resolve basic select with sum" in {

    val in = toInput(
      0,
      Seq(A, B))

    val op = toSelect(
      1,
      Seq(0),
      Seq(attrOperation(Seq(attrRef(A), attrRef(B)))),
      //Vector(Binary("+", null, Seq(AttrRef(A), AttrRef(B)))),
      Seq(C))

    val out = toOutput(
      2,
      Seq(1)
    )

    val dependencies = resolveDependencies(Seq(op, in, out), C)

    assertEqualAttr(dependencies.attributes, Set(A, B))

    assertEqualOp(dependencies.operations, Set(0, 1, 2))
  }

  it should "resolve three dependent operation" in {

    val in = toInput(
      0,
      Seq(A))

    val op1 = toSelect(
      1,
      Seq(0),
      Seq(attrRef(A)),
      Seq(B))

    val op2 = toSelect(
      2,
      Seq(1),
      Seq(attrRef(B)),
      Seq(C))

    val op3 = toSelect(
      3,
      Seq(2),
      Seq(attrRef(C)),
      Seq(D))

    val out = toOutput(
      4,
      Seq(3)
    )

    val dependencies = resolveDependencies(Seq(op1, op2, op3, in, out), D)


    assertEqualAttr(dependencies.attributes, Set(A, B, C))

    assertEqualOp(dependencies.operations, Set(0, 1, 2, 3, 4))
  }


  it should "not depend on operation order" in {

    val in = toInput(
      0,
      Seq(A))

    val op1 = toSelect(
      1,
      Seq(0),
      Seq(attrRef(A)),
      Seq(B))

    val op2 = toSelect(
      2,
      Seq(3),
      Seq(attrRef(C)),
      Seq(D))

    val op3 = toSelect(
      3,
      Seq(1),
      Seq(attrRef(B)),
      Seq(C))

    val out = toOutput(
      4,
      Seq(2)
    )

    val dependencies = resolveDependencies(Seq(op1, op2, op3, in, out), D)


    assertEqualAttr(dependencies.attributes, Set(A, B, C))

    assertEqualOp(dependencies.operations, Set(0, 1, 2, 3, 4))
  }

  it should "resolve chain of several operations including an expression" in {

    val op1 = toSelect(
      1,
      Seq.empty,
      Seq(attrRef(A), attrRef(B)),
      Seq(C, D))

    val op2 = toSelect(
      2,
      Seq(1),
      Seq(attrOperation(Seq(attrRef(C), attrRef(D)))),
      Seq(E))

    val op3 = toSelect(
      3,
      Seq(2),
      Seq(attrRef(E)),
      Seq(F))

    val dependencies = resolveDependencies(Seq(op1, op2, op3), F)

    assertEqualAttr(dependencies.attributes, Set(A, B, C, D, E))
    assertEqualOp(dependencies.operations, Set(1,2,3))

    //assertEqualAttr(dependencies.attributes, Map(C -> Set(A), D -> Set(B), E -> Set(A, B, C, D), F -> Set(A, B, C, D, E)))
    //assertEqualOp(dependencies.operations, Map(C -> Set(1, 2), D -> Set(1, 2), E -> Set(1, 2, 3), F -> Set(1,2,3)))
  }

  it should "resolve aggregation" in {

    val in = toInput(
      0,
      Seq(A, B))

    val op = toAggregate(
      1,
      Seq(0),
      Seq(attrRef(A), attrRef(B)),
      Seq(C, D))

    val out = toOutput(
      2,
      Seq(1)
    )


    val dependencies = resolveDependencies(Seq(op, in, out), D)

    assertEqualAttr(dependencies.attributes, Set(B))

    assertEqualOp(dependencies.operations, Set(0, 1, 2))
  }


  it should "resolve generation" in {

    val in = toInput(
      0,
      Seq(A))

    val op = toGenerate(
      1,
      Seq(0),
      attrRef(A), //Generic("explode", null, Seq(attrRef(A)), null,  None),
      B)

    val out = toOutput(
      2,
      Seq(1)
    )

    val dependencies = resolveDependencies(Seq(op, in, out), B)

    assertEqualAttr(dependencies.attributes, Set(A))
    assertEqualOp(dependencies.operations, Set(0, 1, 2))
  }

  it should "resolve subquery alias" in {

    val in = toInput(
      0,
      Seq(A, B))

    val op = toSubqueryAlias(
      1,
      Seq(0),
      Seq(C, D))

    val out = toOutput(
      2,
      Seq(1)
    )

    val dependencies = resolveDependencies(Seq(op, in, out), D)

    assertEqualAttr(dependencies.attributes, Set(B))
    assertEqualOp(dependencies.operations, Set(0, 1, 2))

    //assertEqualAttr(dependencies.attributes, Map(C -> Set(A), D -> Set(B)))
    //assertEqualOp(dependencies.operations, Map(A -> Set(0, 1), B -> Set(0, 1), C -> Set(0, 1, 2), D -> Set(0, 1, 2)))
  }

  it should "resolve io operation correctly" in {

    val in = toInput(
      1,
      Seq(A, B, C))

    val op = toSelect(
      2,
      Seq(1),
      Seq(attrOperation(Seq(attrRef(A), attrRef(C)))),
      Seq(D))

    val out = toOutput(
      3,
      Seq(2))


    val dependencies = resolveDependencies(Seq(op, in, out), D)

    assertEqualAttr(dependencies.attributes, Set(A, C))

    assertEqualOp(dependencies.operations, Set(1, 2, 3))

    // assertEqualAttr(dependencies.attributes, Map(D -> Set(A, C)))
    // assertEqualOp(dependencies.operations, Map(A -> Set(1, 2), B -> Set(1, 2), C -> Set(1, 2), D -> Set(1, 2, 3)))
  }

  it should "resolve filter operation correctly" in {

    val in = toInput(
      0,
      Seq(A))

    val op1 = toFilter(
      1,
      Seq(0),
      Seq(A)
    )

    val op2 = toSelect(
      2,
      Seq(1),
      Seq(attrRef(A)),
      Seq(B))

    val op3 = toFilter(
      3,
      Seq(2),
      Seq(A)
    )

    val op4 = toFilter(
      4,
      Seq(3),
      Seq(A)
    )

    val out = toOutput(
      5,
      Seq(4))


    val dependencies = resolveDependencies(Seq(op1, op2, op3, op4, in, out), B)

    assertEqualAttr(dependencies.attributes, Set(A))
    assertEqualOp(dependencies.operations, Set(0, 1, 2, 3, 4, 5))

    //assertEqualAttr(dependencies.attributes, Map(B -> Set(A)))
    //assertEqualOp(dependencies.operations, Map(A -> Set(0, 1, 2), B -> Set(0, 1, 2, 3, 4, 5)))
  }

  /**
   *
   *  (A)      (B)   (C)
   *   \       /     /
   *   (D)   (E)    /
   *    \    /     /
   *    (D, E)    /
   *      |      /
   *     (F)   (G)
   *       \   /
   *      (F, G)
   *
   */

  it should "resolve operation with several sources correctly" in {

    val inA = toInput(
      0,
      Seq(A))

    val inB = toInput(
      1,
      Seq(B))

    val inC = toInput(
      2,
      Seq(C))

    val opD = toSelect(
      3,
      Seq(0),
      Seq(attrRef(A)),
      Seq(D))

    val opE = toSelect(
      4,
      Seq(1),
      Seq(attrRef(B)),
      Seq(E))

    val joinDE = toJoin(
      5,
      Seq(opD._id.toInt, opE._id.toInt),
      Seq(D, E))

    val opF = toSelect(
      6,
      Seq(joinDE._id.toInt),
      Seq(attrOperation(Seq(attrRef(D), attrRef(E)))),
      Seq(F))

    val opG = toSelect(
      7,
      Seq(inC._id.toInt),
      Seq(attrRef(C)),
      Seq(G))


    val joinFG = toJoin(
      8,
      Seq(opF._id.toInt, opG._id.toInt),
      Seq(F, G))

    val out = toOutput(
      9,
      Seq(joinFG._id.toInt))


    val dependencies = resolveDependencies(Seq(opD, opE, joinDE, opF, opG, joinFG, inA, inB, inC, out), F)

    assertEqualAttr(dependencies.attributes, Set(A, B, E, D))
    assertEqualOp(dependencies.operations, Set(inA, opD, inB, opE, joinDE, opF, joinFG, out).map(_._id.toInt))

    /*
    assertEqualAttr(dependencies.attributes, Map(D -> Set(A), E -> Set(B), F -> Set(A, B, E, D), G -> Set(C)))

    assertEqualOp(dependencies.operations, Map(
      A -> Set(inA, opD).map(_.id),
      B -> Set(inB, opE).map(_.id),
      C -> Set(inC, opG).map(_.id),
      D -> Set(inA, opD, joinDE, opF).map(_.id),
      E -> Set(inB, opE, joinDE, opF).map(_.id),
      F -> Set(inA, opD, inB, opE, joinDE, opF, joinFG, out).map(_.id),
      G -> Set(inC, opG, joinFG, out).map(_.id)))
     */
  }


}
