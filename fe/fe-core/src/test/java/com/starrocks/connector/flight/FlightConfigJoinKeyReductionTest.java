// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.flight;

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FlightConfigJoinKeyReductionTest {
    private static FlightConfig config(String enabled, String maxKeys) {
        Map<String, String> props = new HashMap<>();
        props.put("flight.uri", "grpc://localhost:8815");
        if (enabled != null) {
            props.put("flight.join_key_reduction", enabled);
        }
        if (maxKeys != null) {
            props.put("flight.join_key_reduction.max_keys", maxKeys);
        }
        return FlightConfig.from(props);
    }

    @Test
    public void joinKeyReductionIsOnByDefaultWithAHundredThousandKeys() {
        Assertions.assertTrue(config(null, null).isJoinKeyReduction());
        Assertions.assertEquals(100_000, config(null, null).getJoinKeyReductionMaxKeys());
    }

    @Test
    public void joinKeyReductionCanBeSwitchedOffAndTuned() {
        Assertions.assertFalse(config("false", null).isJoinKeyReduction());
        Assertions.assertEquals(500, config(null, "500").getJoinKeyReductionMaxKeys());
    }

    @Test
    public void joinKeyReductionRejectsBadValues() {
        Assertions.assertThrows(StarRocksConnectorException.class, () -> config("maybe", null));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> config(null, "0"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> config(null, "many"));
    }
}
