package org.example.com.api;

import io.sinistral.proteus.server.ServerResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.example.com.repository.IVehiclesRepository;


@SuppressWarnings("ALL")
@Path("/operator")
@Produces((MediaType.APPLICATION_JSON))
@Consumes((MediaType.MEDIA_TYPE_WILDCARD))
public class OperatorAPI {

  @Inject
  @Named("RepositoryVehicles")
  private IVehiclesRepository repository;

  public OperatorAPI() {
  }

  @Inject
  public OperatorAPI(@Named("RepositoryVehicles") final IVehiclesRepository repository) {
    this.repository = repository;
  }

  @GET
  @Path("/{start}/{end}")
  @Produces((MediaType.APPLICATION_JSON))
  @Consumes((MediaType.MEDIA_TYPE_WILDCARD))
  @Operation(summary = "1 - Given a time frame [start-time, end-time], what is the list of running operators", responses = {
      @ApiResponse(description = "Get test's result details", content = @Content(schema = @Schema(implementation = String.class)))}, tags = "Operator API")
  public CompletableFuture<ServerResponse<Set<String>>> listOperators(
      @Parameter(description = "Start time in microseconds") @PathParam("start") final Long start,
      @Parameter(description = "End time in microseconds") @PathParam("end") final Long end
  ) {
    return CompletableFuture.supplyAsync(() -> repository.getOperators(start, end),
        IOPool.EXECUTORS
    )
        .thenApply(set -> ServerResponse.response(set).applicationJson());
  }

  @GET
  @Path("/{start}/{end}/{operatorId}/vehicles")
  @Produces((MediaType.APPLICATION_JSON))
  @Consumes((MediaType.MEDIA_TYPE_WILDCARD))
  @Operation(summary = "2 & 3 - Given a time frame [start-time, end-time] and an Operator, what is the list of vehicle IDs? AND Given a time frame [start-time, end-time] and a fleet, which vehicles are at a stop? Depends on the stopped value. If true will respond the question 2, otherwise 3", responses = {
      @ApiResponse(description = "Get test's result details", content = @Content(schema = @Schema(implementation = String.class)))}, tags = "Operator API")
  public CompletableFuture<ServerResponse<Set<String>>> listVehiclesOfOperator(
      @Parameter(description = "Start time in microseconds") @PathParam("start") final Long start,
      @Parameter(description = "End time in microseconds") @PathParam("end") final Long end,
      @Parameter(description = "Operator Id") @PathParam("operatorId") final String operatorId,
      @Parameter(description = "Optional query parameter, to query for stopped vehicles") @QueryParam("stopped") final Optional<Boolean> stopped
  ) {

    return CompletableFuture.supplyAsync(() -> stopped.filter(e -> e)
        .map(e -> getVehiclesStopped(repository, start, end, operatorId))
        .orElseGet(() -> getVehicles(repository, start, end, operatorId)))
        .thenApply(setOfIds -> ServerResponse.response(setOfIds).applicationJson());
  }

  public static Set<String> getVehicles(
      IVehiclesRepository repository, long from, long to, final String operatorId
  ) {
    return repository.getVehiclesOfOperator(from, to, operatorId)
        .parallelStream()
        .map(e -> e.getVehicleId())
        .collect(Collectors.toSet());
  }

  public static Set<String> getVehiclesStopped(
      IVehiclesRepository repository, long from, long to, final String operatorId
  ) {
    return repository.getVehiclesStoppedOfOperator(from, to, operatorId);
  }

}
