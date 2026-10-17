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

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/** Maps Arrow column types to StarRocks types; anything unsupported fails loudly. */
public final class FlightTypeMapper {
    private static final int MAX_DECIMAL_PRECISION = 38;

    private FlightTypeMapper() {
    }

    public static Type toStarRocks(Field field) {
        ArrowType arrow = field.getType();
        switch (arrow.getTypeID()) {
            case Int: {
                ArrowType.Int type = (ArrowType.Int) arrow;
                if (type.getIsSigned()) {
                    switch (type.getBitWidth()) {
                        case 8:
                            return Type.TINYINT;
                        case 16:
                            return Type.SMALLINT;
                        case 32:
                            return Type.INT;
                        case 64:
                            return Type.BIGINT;
                        default:
                            break;
                    }
                }
                break;
            }
            case FloatingPoint: {
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrow).getPrecision();
                if (precision == FloatingPointPrecision.SINGLE) {
                    return Type.FLOAT;
                }
                if (precision == FloatingPointPrecision.DOUBLE) {
                    return Type.DOUBLE;
                }
                break;
            }
            case Decimal: {
                ArrowType.Decimal type = (ArrowType.Decimal) arrow;
                if (type.getPrecision() <= MAX_DECIMAL_PRECISION && type.getScale() >= 0
                        && type.getScale() <= type.getPrecision()) {
                    return ScalarType.createUnifiedDecimalType(type.getPrecision(), type.getScale());
                }
                break;
            }
            case Bool:
                return Type.BOOLEAN;
            case Utf8:
            case LargeUtf8:
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
            case Binary:
            case LargeBinary:
                return Type.VARBINARY;
            case Date:
                if (((ArrowType.Date) arrow).getUnit() == DateUnit.DAY) {
                    return Type.DATE;
                }
                break;
            case Timestamp:
                if (((ArrowType.Timestamp) arrow).getTimezone() == null) {
                    return Type.DATETIME;
                }
                break;
            default:
                break;
        }
        throw new StarRocksConnectorException("Unsupported Arrow type %s for column %s",
                arrow, field.getName());
    }
}
