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

public class FlightConfigTest {
    private static Map<String, String> props(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    @Test
    public void parsesDefaults() {
        FlightConfig config = FlightConfig.from(props("flight.uri", "grpc://localhost:8815"));
        Assertions.assertEquals("grpc://localhost:8815", config.getUri());
        Assertions.assertEquals(FlightConfig.AuthType.NONE, config.getAuthType());
        Assertions.assertEquals(300, config.getCacheTtlSeconds());
    }

    @Test
    public void requiresUriAndTokenForTokenAuth() {
        Assertions.assertThrows(StarRocksConnectorException.class, () -> FlightConfig.from(props()));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> FlightConfig.from(
                props("flight.uri", "grpc://h:1", "flight.auth.type", "token")));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> FlightConfig.from(
                props("flight.uri", "grpc://h:1", "flight.auth.type", "kerberos")));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> FlightConfig.from(
                props("flight.uri", "http://h:1")));
    }

    @Test
    public void toStringNeverShowsTheToken() {
        FlightConfig config = FlightConfig.from(props("flight.uri", "grpc+tls://h:1",
                "flight.auth.type", "token", "flight.auth.token", "s3cret-token"));
        Assertions.assertFalse(config.toString().contains("s3cret-token"));
        Assertions.assertEquals("s3cret-token", config.getToken());
    }
}
