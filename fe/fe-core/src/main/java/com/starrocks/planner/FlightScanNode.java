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
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.flight.FlightConfig;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFlightScanNode;
import com.starrocks.thrift.TFlightScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;
import pro.surpin.data.arrowflight.starrocks.ScanSql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Scan of a table served by a Flight SQL server. The frontend runs the projected and filtered query once; every
 * Flight endpoint of the result becomes one scan range that a backend redeems with the endpoint's ticket.
 */
public class FlightScanNode extends ScanNode {
    private final FlightTable table;
    private final List<String> columns = new ArrayList<>();
    private final List<TScanRangeLocations> scanRanges = new ArrayList<>();
    private final Random random = new Random();
    private ScanPredicate pushedPredicate;
    private ScanAggregate aggregation;

    public FlightScanNode(PlanNodeId id, TupleDescriptor desc, FlightTable table) {
        super(id, desc, "SCAN FLIGHT");
        this.table = table;
    }

    public void setPushedPredicate(ScanPredicate pushedPredicate) {
        this.pushedPredicate = pushedPredicate;
    }

    /** Partial aggregation the Flight server computes per endpoint; the scan then yields keys and partials. */
    public void setAggregation(ScanAggregate aggregation) {
        this.aggregation = aggregation;
    }

    /** The remote columns to read; empty (for example count(*)) means the server's default projection. */
    public void computeColumns() {
        columns.clear();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.isMaterialized()) {
                columns.add(slot.getColumn().getName());
            }
        }
    }

    /** Alive backends or compute nodes of the query's compute resource. */
    public List<ComputeNode> assignNodes() throws StarRocksException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes;
        if (RunMode.isSharedDataMode()) {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            nodes = warehouseManager.getAllComputeNodeIds(computeResource).stream()
                    .map(systemInfoService::getBackendOrComputeNode).collect(Collectors.toList());
        } else {
            nodes = systemInfoService.backendAndComputeNodeStream().collect(Collectors.toList());
        }
        return nodes.stream().filter(ComputeNode::isAlive).collect(Collectors.toList());
    }

    /** Plans the query on the Flight server and spreads its endpoints over the given nodes. */
    public void planScanRanges(List<ComputeNode> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No alive backends or compute nodes");
        }
        FlightInfo info = aggregation != null
                ? table.getService().planScan(table.getCatalogDBName(), table.getName(), pushedPredicate, aggregation)
                : table.getService().planScan(table.getCatalogDBName(), table.getName(),
                        new ArrayList<>(columns), pushedPredicate);
        scanRanges.clear();
        int next = random.nextInt(nodes.size());
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            TFlightScanRange flightRange = new TFlightScanRange();
            flightRange.setLocations(endpoint.getLocations().stream()
                    .map(Location::getUri).map(Object::toString).collect(Collectors.toList()));
            flightRange.setTicket(ByteBuffer.wrap(endpoint.getTicket().getBytes()));

            ComputeNode node = nodes.get(next++ % nodes.size());
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(node.getId());
            location.setServer(new TNetworkAddress(node.getHost(), node.getBePort()));

            TScanRange scanRange = new TScanRange();
            scanRange.setFlight_scan_range(flightRange);
            TScanRangeLocations locations = new TScanRangeLocations();
            locations.addToLocations(location);
            locations.setScan_range(scanRange);
            scanRanges.add(locations);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRanges;
    }

    @Override
    public void finalizeStats() throws StarRocksException {
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getCatalogDBName()).append('.')
                .append(table.getName()).append('\n');
        String query = aggregation != null
                ? ScanSql.select(table.getCatalogDBName(), table.getName(), aggregation, pushedPredicate)
                : ScanSql.select(table.getCatalogDBName(), table.getName(), columns, pushedPredicate);
        output.append(prefix).append("QUERY: ").append(query).append('\n');
        output.append(prefix).append("FLIGHT ENDPOINTS: ").append(scanRanges.size()).append('\n');
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.FLIGHT_SCAN_NODE;
        TFlightScanNode flight = new TFlightScanNode();
        flight.setTuple_id(desc.getId().asInt());
        flight.setFlight_uri(table.getConfig().getUri());
        flight.setColumns(columns);
        if (table.getConfig().getAuthType() == FlightConfig.AuthType.TOKEN) {
            flight.setAuth_token(table.getConfig().getToken());
        }
        msg.flight_scan_node = flight;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
