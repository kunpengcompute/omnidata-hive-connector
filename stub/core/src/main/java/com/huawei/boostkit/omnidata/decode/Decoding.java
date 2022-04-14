/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package com.huawei.boostkit.omnidata.decode;

import com.huawei.boostkit.omnidata.type.DecodeType;

import io.airlift.slice.SliceInput;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/**
 * Decode Slice to type
 *
 * @param <T>
 * @since 2020-07-31
 */
public interface Decoding<T> {
    /**
     * decode
     *
     * @param type decode type
     * @param sliceInput content
     * @return T
     */
    T decode(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("ARRAY")
    T decodeArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("BYTE_ARRAY")
    T decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("BOOLEAN_ARRAY")
    T decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("INT_ARRAY")
    T decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("INT128_ARRAY")
    T decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("SHORT_ARRAY")
    T decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_ARRAY")
    T decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("FLOAT_ARRAY")
    T decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DOUBLE_ARRAY")
    T decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("MAP")
    T decodeMap(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("MAP_ELEMENT")
    T decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("VARIABLE_WIDTH")
    T decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DICTIONARY")
    T decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("RLE")
    T decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
        throws InvocationTargetException, IllegalAccessException;

    @Decode("ROW")
    T decodeRow(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DATE")
    T decodeDate(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_INT")
    T decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_SHORT")
    T decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_BYTE")
    T decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_FLOAT")
    T decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput);
}

