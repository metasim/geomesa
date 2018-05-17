/***********************************************************************
 * Copyright (c) 2018 Astraea, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.spark.jts.expressions
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.PointUDT
import org.apache.spark.sql.types._
import org.locationtech.geomesa.jts.GeoMesaWKBReader



/**
 *
 *
 * @since 5/17/18
 */
case class JTSCentroidExpresssion(child: Expression) extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = PointUDT

  override protected def nullSafeEval(input: Any): Any = {
    val wkbReader = new GeoMesaWKBReader()
    val centroid = input match {
      case g: Geometry => g.getCentroid
      case r: InternalRow =>
        val geomRow = child.eval(r).asInstanceOf[InternalRow]
        val wkbRow = geomRow.getStruct(0, 1)
        val wkb= PointUDT.readWKB(wkbRow)
//        // TODO: Read this directly
        wkbReader.read(wkb).getCentroid
        //PointUDT.deserialize(geomRow.get(0, PointUDT)).getCentroid
        PointUDT.deserialize(geomRow).getCentroid
    }
    PointUDT.serialize(centroid)
  }
}
