/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for service provider statistics
 */
@Path(Constants.Gateway.API_VERSION_3 + "/system/serviceproviders")
public class ServiceProviderStatsHandler extends AbstractHttpHandler {

  private static class Info {
    private final String version;
    private final String url;
    private final String logsUrl;

    private Info(String version, String url, String logsUrl) {
      this.version = version;
      this.url = url;
      this.logsUrl = logsUrl;
    }
  }

  private static final Map<String, Info> MOCK_INFO = ImmutableMap.of(
    "hdfs", new Info("2.7.0", "http://localhost:50070", "http://localhost:50070/logs"),
    "yarn", new Info("2.7.0", "http://localhost:8088", "http://localhost:8088/logs"),
    "hbase", new Info("1.0.0", "http://localhost:60010", "http://localhost:60010/logs")
  );

  private static final Map<String, Integer> SERVICES_STATS = ImmutableMap.of(
    "masters", 2, "kafka-servers", 2, "routers", 1, "auth-servers", 1);
  private static final Map<String, Integer> CDAP_ENTITIES_STATS = new ImmutableMap.Builder<String, Integer>()
    .put("namespaces", 10)
    .put("apps", 46)
    .put("artifacts", 23)
    .put("datasets", 68)
    .put("streams", 34)
    .put("programs", 78)
    .build();
  private static final Map<String, Long> STORAGE_STATS = ImmutableMap.of(
    "total", 3452759234L, "used", 34525543L, "available", 3443555345L);
  private static final Map<String, Integer> HDFS_NODE_STATS = ImmutableMap.of(
    "total", 40, "healthy", 36, "decommissioned", 3, "decommissionInProgress", 1);
  private static final Map<String, Long> BLOCKS_STATS = ImmutableMap.of(
    "missing", 33L, "corrupt", 3L, "underreplicated", 5L);
  private static final Map<String, Integer> YARN_NODE_STATS = new ImmutableMap.Builder<String, Integer>()
    .put("total", 35)
    .put("new", 0)
    .put("running", 30)
    .put("unhealthy", 1)
    .put("decommissioned", 2)
    .put("lost", 1)
    .put("rebooted", 1)
    .build();
  private static final Map<String, Integer> APP_STATS = new ImmutableMap.Builder<String, Integer>()
    .put("total", 30)
    .put("submitted", 2)
    .put("accepted", 4)
    .put("running", 20)
    .put("failed", 1)
    .put("killed", 3)
    .put("new", 0)
    .put("new_saving", 0)
    .build();
  private static final Map<String, Integer> MEMORY_STATS = ImmutableMap.of(
    "total", 8192, "used", 7168, "available", 1024);
  private static final Map<String, Integer> VCORES_STATS = ImmutableMap.of(
    "total", 36, "used", 12, "available", 24);
  private static final Map<String, Integer> QUEUE_STATS = ImmutableMap.of(
    "total", 10, "stopped", 2, "running", 8, "maxCapacity", 32, "currentCapacity", 21);
  private static final Map<String, Integer> HBASE_NODE_STATS = ImmutableMap.of(
    "totalRegionServers", 37, "liveRegionServers", 34, "deadRegionServers", 3, "masters", 3);
  private static final Map<String, Integer> HBASE_ENTITY_STATS = ImmutableMap.of(
    "namepsaces", 8, "tables", 56);

  @GET
  @Path("/")
  public void getServiceProviders(HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, MOCK_INFO);
  }

  @GET
  @Path("/{service-provider}/stats")
  public void getServiceProviderStats(HttpRequest request, HttpResponder responder,
                                      @PathParam("service-provider") String serviceProvider) throws NotFoundException {
    Map<String, Object> stats = new HashMap<>();
    switch (serviceProvider) {
      case "cdap":
        stats.put("services", SERVICES_STATS);
        stats.put("entities", CDAP_ENTITIES_STATS);
        break;
      case "hdfs":
        stats.put("storage", STORAGE_STATS);
        stats.put("nodes", HDFS_NODE_STATS);
        stats.put("blocks", BLOCKS_STATS);
        break;
      case "yarn":
        stats.put("nodes", YARN_NODE_STATS);
        stats.put("apps", APP_STATS);
        stats.put("memory", MEMORY_STATS);
        stats.put("vcores", VCORES_STATS);
        stats.put("queues", QUEUE_STATS);
        break;
      case "hbase":
        stats.put("nodes", HBASE_NODE_STATS);
        stats.put("entities", HBASE_ENTITY_STATS);
        break;
      default:
        throw new NotFoundException("Service provider '" + serviceProvider + "' not found");
    }
    responder.sendJson(HttpResponseStatus.OK, stats);
  }
}
