/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * PageToColumnar
 */
public class PageToColumnar implements Serializable {
    StructType structType = null;
    Seq<Attribute> outPut = null;
    public PageToColumnar(StructType structType, Seq<Attribute> outPut) {
        this.structType = structType;
        this.outPut = outPut;
    }

    public List<Object> transPageToColumnar(Iterator<WritableColumnVector[]> writableColumnVectors,
                                            boolean isVectorizedReader) {
        scala.collection.Iterator<StructField> structFieldIterator = structType.iterator();
        List<DataType> columnType = new ArrayList<>();

        while (structFieldIterator.hasNext()) {
            columnType.add(structFieldIterator.next().dataType());
        }
        List<Object> internalRowList = new ArrayList<>();
        while (writableColumnVectors.hasNext()) {
            WritableColumnVector[] columnVector = writableColumnVectors.next();
            if (columnVector == null) {
                continue;
            }
            int positionCount = columnVector[0].getElementsAppended();
            if (positionCount > 0) {
                if (isVectorizedReader) {
                    ColumnarBatch columnarBatch = new ColumnarBatch(columnVector);
                    columnarBatch.setNumRows(positionCount);
                    internalRowList.add(columnarBatch);
                } else {
                    for (int j = 0; j < positionCount; j++) {
                        MutableColumnarRow mutableColumnarRow =
                                new MutableColumnarRow(columnVector);
                        mutableColumnarRow.rowId = j;
                        internalRowList.add(mutableColumnarRow);
                    }
                }
            }
        }
        return internalRowList;
    }
}




