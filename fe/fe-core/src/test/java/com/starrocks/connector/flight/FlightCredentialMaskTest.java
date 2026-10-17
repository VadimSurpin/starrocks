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

import com.starrocks.credential.CredentialUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FlightCredentialMaskTest {
    @Test
    public void showCreateCatalogNeverPrintsTheBearerToken() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FlightConfig.URI_KEY, "grpc://localhost:8815");
        properties.put(FlightConfig.AUTH_TYPE_KEY, "token");
        properties.put(FlightConfig.AUTH_TOKEN_KEY, "s3cret-token-value");

        CredentialUtil.maskCredential(properties);

        Assertions.assertFalse(properties.toString().contains("s3cret"), properties.toString());
        Assertions.assertEquals("grpc://localhost:8815", properties.get(FlightConfig.URI_KEY));
        Assertions.assertEquals("token", properties.get(FlightConfig.AUTH_TYPE_KEY));
    }
}
