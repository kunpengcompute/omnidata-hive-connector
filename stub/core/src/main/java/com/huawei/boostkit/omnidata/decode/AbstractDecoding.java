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

import java.util.Optional;

/**
 * Abstract decoding
 *
 * @param <T> decoding type
 * @since 2021-07-31
 */
public abstract class AbstractDecoding<T> implements Decoding<T> {
    @Override
    public T decode(Optional<DecodeType> type, SliceInput sliceInput) {
        return null;
    }

    /**
     * decode Null Bits
     *
     * @param sliceInput sliceInput
     * @param positionCount positionCount
     * @return decoded boolean[]
     * @since 2021-07-31
     */
    public Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount) {
        return Optional.empty();
    }
}

