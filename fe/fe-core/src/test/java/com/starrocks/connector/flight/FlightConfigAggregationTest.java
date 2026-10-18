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

import java.util.Map;

public class FlightConfigAggregationTest {
    private static FlightConfig config(String value) {
        return value == null
                ? FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815"))
                : FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815", "flight.aggregation_pushdown", value));
    }

    @Test
    public void aggregationPushdownIsOnByDefault() {
        Assertions.assertTrue(config(null).isAggregationPushdown());
        Assertions.assertTrue(config("true").isAggregationPushdown());
    }

    @Test
    public void aggregationPushdownCanBeSwitchedOff() {
        Assertions.assertFalse(config("false").isAggregationPushdown());
    }

    @Test
    public void aggregationPushdownRejectsOtherValues() {
        Assertions.assertThrows(StarRocksConnectorException.class, () -> config("maybe"));
    }
}
