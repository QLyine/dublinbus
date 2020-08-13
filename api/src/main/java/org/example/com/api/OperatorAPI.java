package org.example.com.api;

import io.sinistral.proteus.server.ServerResponse;
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
import javax.ws.rs.WebApplicationException;
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
  public CompletableFuture<ServerResponse<Set<String>>> listOperators(
      @PathParam("start") final Long start, @PathParam("end") final Long end
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
  public CompletableFuture<ServerResponse<Set<String>>> listVehiclesOfOperator(
      @PathParam("start") final long start,
      @PathParam("end") final long end,
      @PathParam("operatorId") final String operatorId
  ) {
    return CompletableFuture.supplyAsync(() -> repository.getVehiclesOfOperator(start,
        end,
        operatorId
    ))
        .thenApply(set -> set.parallelStream()
            .map(e -> e.getVehicleId())
            .collect(Collectors.toSet()))
        .thenApply(setOfIds -> ServerResponse.response(setOfIds).applicationJson());
  }

}
