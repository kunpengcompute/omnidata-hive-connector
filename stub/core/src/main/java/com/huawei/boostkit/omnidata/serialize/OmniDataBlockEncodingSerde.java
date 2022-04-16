/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.serialize;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Block Encoding Serde
 *
 * @since 2021-07-31
 */
public final class OmniDataBlockEncodingSerde implements BlockEncodingSerde {

    @Override
    public Block readBlock(SliceInput input) {
        return null;
    }

    @Override
    public void writeBlock(SliceOutput output, Block block) {

    }
}

