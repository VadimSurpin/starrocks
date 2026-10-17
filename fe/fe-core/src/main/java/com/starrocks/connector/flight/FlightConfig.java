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

import com.google.common.base.Strings;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.net.URI;
import java.util.Map;

/** Catalog properties of a Flight SQL external catalog. Secrets are never printed. */
public final class FlightConfig {
    public static final String URI_KEY = "flight.uri";
    public static final String AUTH_TYPE_KEY = "flight.auth.type";
    public static final String AUTH_TOKEN_KEY = "flight.auth.token";
    public static final String CACHE_TTL_KEY = "flight.cache.ttl.seconds";

    /** Supported authentication schemes. */
    public enum AuthType { NONE, TOKEN }

    private final String uri;
    private final AuthType authType;
    private final String token;
    private final int cacheTtlSeconds;

    private FlightConfig(String uri, AuthType authType, String token, int cacheTtlSeconds) {
        this.uri = uri;
        this.authType = authType;
        this.token = token;
        this.cacheTtlSeconds = cacheTtlSeconds;
    }

    public static FlightConfig from(Map<String, String> properties) {
        String uri = properties.get(URI_KEY);
        if (Strings.isNullOrEmpty(uri)) {
            throw new StarRocksConnectorException("Missing " + URI_KEY + " in properties");
        }
        URI parsed;
        try {
            parsed = URI.create(uri);
        } catch (IllegalArgumentException e) {
            throw new StarRocksConnectorException("Invalid " + URI_KEY + ": " + uri);
        }
        String scheme = parsed.getScheme();
        if (!("grpc".equals(scheme) || "grpc+tcp".equals(scheme) || "grpc+tls".equals(scheme))
                || parsed.getHost() == null || parsed.getPort() < 0) {
            throw new StarRocksConnectorException(
                    URI_KEY + " must look like grpc://host:port or grpc+tls://host:port, got: " + uri);
        }

        AuthType authType;
        String authValue = properties.getOrDefault(AUTH_TYPE_KEY, "none");
        switch (authValue.toLowerCase()) {
            case "none":
                authType = AuthType.NONE;
                break;
            case "token":
                authType = AuthType.TOKEN;
                break;
            default:
                throw new StarRocksConnectorException(
                        AUTH_TYPE_KEY + " must be 'none' or 'token', got: " + authValue);
        }
        String token = properties.get(AUTH_TOKEN_KEY);
        if (authType == AuthType.TOKEN && Strings.isNullOrEmpty(token)) {
            throw new StarRocksConnectorException("Missing " + AUTH_TOKEN_KEY + " for token authentication");
        }

        int ttl = 300;
        String ttlValue = properties.get(CACHE_TTL_KEY);
        if (ttlValue != null) {
            try {
                ttl = Integer.parseInt(ttlValue);
            } catch (NumberFormatException e) {
                throw new StarRocksConnectorException(CACHE_TTL_KEY + " must be an integer, got: " + ttlValue);
            }
        }
        return new FlightConfig(uri, authType, token, ttl);
    }

    public String getUri() {
        return uri;
    }

    public AuthType getAuthType() {
        return authType;
    }

    public String getToken() {
        return token;
    }

    public int getCacheTtlSeconds() {
        return cacheTtlSeconds;
    }

    @Override
    public String toString() {
        return "FlightConfig{uri=" + uri + ", authType=" + authType
                + (token == null ? "" : ", token=***") + "}";
    }
}
