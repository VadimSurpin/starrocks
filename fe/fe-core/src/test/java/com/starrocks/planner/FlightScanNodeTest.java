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

package com.starrocks.planner;

import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.flight.FlightConfig;
import com.starrocks.connector.flight.FlightService;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlightScanNodeTest {
    private static final String TOKEN = "tok-123";
    private static final String TICKET = "secret-ticket";

    private static class RecordingService implements FlightService {
        List<String> columns;
        ScanPredicate where;
        ScanAggregate aggregate;
        int endpoints = 2;

        @Override
        public List<String> schemas() {
            return List.of();
        }

        @Override
        public List<String> tables(String schema) {
            return List.of();
        }

        @Override
        public Schema tableSchema(String schema, String table) {
            return null;
        }

        @Override
        public FlightInfo planScan(String schema, String table, ScanPredicate where, ScanAggregate aggregate) {
            this.where = where;
            this.aggregate = aggregate;
            return planScan(schema, table, List.of(), where);
        }

        @Override
        public FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where) {
            this.columns = columns;
            this.where = where;
            List<FlightEndpoint> list = new ArrayList<>();
            for (int i = 0; i < endpoints; i++) {
                list.add(new FlightEndpoint(new Ticket((TICKET + i).getBytes(StandardCharsets.UTF_8)),
                        Location.forGrpcInsecure("worker" + i, 8815)));
            }
            return new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[0]), list, -1, -1);
        }

        @Override
        public void close() {
        }
    }

    private RecordingService service = new RecordingService();

    private FlightScanNode node() {
        Column id = new Column("id", Type.BIGINT);
        Column name = new Column("name", Type.VARCHAR);
        FlightConfig config = FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815",
                "flight.auth.type", "token", "flight.auth.token", TOKEN));
        FlightTable table = new FlightTable(1, "orders", List.of(id, name), "sales", "flight", config, service);
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(1));
        tuple.setTable(table);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), "id", Type.BIGINT, true);
        slot.setColumn(id);
        slot.setIsMaterialized(true);
        tuple.addSlot(slot);
        return new FlightScanNode(new PlanNodeId(1), tuple, table);
    }

    private static List<ComputeNode> backends() {
        return List.of(new ComputeNode(10, "be1", 9050), new ComputeNode(11, "be2", 9050));
    }

    @Test
    public void plansProjectionAndFilterAndCreatesOneRangePerEndpoint() {
        FlightScanNode node = node();
        node.setPushedPredicate(new ScanPredicate.Compare("id", ScanPredicate.Op.EQ,
                new ScanPredicate.IntLit(5)));
        node.computeColumns();
        node.planScanRanges(backends());

        Assertions.assertEquals(List.of("id"), service.columns);
        Assertions.assertEquals("(id = 5)", service.where.toSql());
        List<TScanRangeLocations> ranges = node.getScanRangeLocations(0);
        Assertions.assertEquals(2, ranges.size());
        Assertions.assertEquals(TICKET + "0",
                new String(ranges.get(0).getScan_range().getFlight_scan_range().getTicket(), StandardCharsets.UTF_8));
        Assertions.assertEquals(1, ranges.get(0).getLocationsSize());
        Assertions.assertNotEquals(ranges.get(0).getLocations().get(0).getBackend_id(),
                ranges.get(1).getLocations().get(0).getBackend_id());
        Assertions.assertFalse(ranges.get(0).getScan_range().getFlight_scan_range().getLocations().isEmpty());
    }

    @Test
    public void explainShowsQueryButNeverCredentialsOrTickets() {
        FlightScanNode node = node();
        node.setPushedPredicate(new ScanPredicate.IsNull("id", true));
        node.computeColumns();
        node.planScanRanges(backends());
        String explain = node.getExplainString();
        Assertions.assertTrue(explain.contains("QUERY: SELECT id FROM sales.orders WHERE (id IS NOT NULL)"), explain);
        Assertions.assertFalse(explain.contains(TOKEN), explain);
        Assertions.assertFalse(explain.contains(TICKET), explain);
    }

    @Test
    public void toThriftCarriesColumnsAndTokenForTheBackend() {
        FlightScanNode node = node();
        node.computeColumns();
        node.planScanRanges(backends());
        TPlanNode msg = new TPlanNode();
        node.toThrift(msg);
        Assertions.assertEquals(TPlanNodeType.FLIGHT_SCAN_NODE, msg.node_type);
        Assertions.assertEquals(List.of("id"), msg.flight_scan_node.getColumns());
        Assertions.assertEquals(1, msg.flight_scan_node.getTuple_id());
        Assertions.assertEquals("grpc://localhost:8815", msg.flight_scan_node.getFlight_uri());
        Assertions.assertEquals(TOKEN, msg.flight_scan_node.getAuth_token());
    }

    @Test
    public void zeroEndpointsMeansNoScanRanges() {
        service.endpoints = 0;
        FlightScanNode node = node();
        node.computeColumns();
        node.planScanRanges(backends());
        Assertions.assertTrue(node.getScanRangeLocations(0).isEmpty());
    }

    @Test
    public void noAliveBackendsIsAnError() {
        FlightScanNode node = node();
        node.computeColumns();
        Assertions.assertThrows(IllegalStateException.class, () -> node.planScanRanges(List.of()));
    }

    private static ScanAggregate aggregate() {
        return new ScanAggregate(List.of("id"), List.of(
                ScanAggregate.Partial.column(ScanAggregate.Func.COUNT_STAR, null, "__agg0")));
    }

    @Test
    public void aggregatePushdownIsPlannedAndExplained() {
        FlightScanNode node = node();
        node.setPushedPredicate(new ScanPredicate.IsNull("id", true));
        node.setAggregation(aggregate());
        node.computeColumns();
        node.planScanRanges(backends());

        Assertions.assertNotNull(service.aggregate);
        Assertions.assertEquals("(id IS NOT NULL)", service.where.toSql());
        String explain = node.getExplainString();
        Assertions.assertTrue(explain.contains(
                "QUERY: SELECT id, COUNT(*) AS __agg0 FROM sales.orders WHERE (id IS NOT NULL) GROUP BY id"), explain);
        Assertions.assertEquals(2, node.getScanRangeLocations(0).size());
    }
}
