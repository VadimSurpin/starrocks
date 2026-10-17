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
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FlightTypeMapperTest {
    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    @Test
    public void mapsSupportedPrimitiveTypes() {
        Assertions.assertEquals(Type.TINYINT, FlightTypeMapper.toStarRocks(field("c", new ArrowType.Int(8, true))));
        Assertions.assertEquals(Type.SMALLINT, FlightTypeMapper.toStarRocks(field("c", new ArrowType.Int(16, true))));
        Assertions.assertEquals(Type.INT, FlightTypeMapper.toStarRocks(field("c", new ArrowType.Int(32, true))));
        Assertions.assertEquals(Type.BIGINT, FlightTypeMapper.toStarRocks(field("c", new ArrowType.Int(64, true))));
        Assertions.assertEquals(Type.FLOAT, FlightTypeMapper.toStarRocks(
                field("c", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        Assertions.assertEquals(Type.DOUBLE, FlightTypeMapper.toStarRocks(
                field("c", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
        Assertions.assertEquals(Type.BOOLEAN, FlightTypeMapper.toStarRocks(field("c", ArrowType.Bool.INSTANCE)));
        Assertions.assertEquals(Type.DATE, FlightTypeMapper.toStarRocks(
                field("c", new ArrowType.Date(DateUnit.DAY))));
        Assertions.assertEquals(Type.DATETIME, FlightTypeMapper.toStarRocks(
                field("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))));
        Assertions.assertEquals(Type.VARBINARY, FlightTypeMapper.toStarRocks(field("c", ArrowType.Binary.INSTANCE)));
    }

    @Test
    public void mapsStringsAndDecimals() {
        Type utf8 = FlightTypeMapper.toStarRocks(field("c", ArrowType.Utf8.INSTANCE));
        Assertions.assertTrue(utf8.isVarchar());
        Assertions.assertTrue(FlightTypeMapper.toStarRocks(field("c", ArrowType.LargeUtf8.INSTANCE)).isVarchar());

        Type decimal = FlightTypeMapper.toStarRocks(field("c", new ArrowType.Decimal(15, 2, 128)));
        Assertions.assertTrue(decimal.isDecimalV3());
        Assertions.assertEquals(15, ((ScalarType) decimal).getScalarPrecision());
        Assertions.assertEquals(2, ((ScalarType) decimal).getScalarScale());
    }

    @Test
    public void rejectsUnsupportedTypesNamingTheColumn() {
        List<ArrowType> unsupported = List.of(
                new ArrowType.Int(32, false),
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
                new ArrowType.Decimal(39, 2, 256),
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
                new ArrowType.Date(DateUnit.MILLISECOND),
                ArrowType.List.INSTANCE,
                ArrowType.Struct.INSTANCE);
        for (ArrowType type : unsupported) {
            StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> FlightTypeMapper.toStarRocks(field("weird_col", type)), type.toString());
            Assertions.assertTrue(e.getMessage().contains("weird_col"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains(type.toString()), e.getMessage());
        }
    }
}
