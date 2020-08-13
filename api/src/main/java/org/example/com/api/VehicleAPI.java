package org.example.com.api;

import io.sinistral.proteus.server.ServerResponse;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.example.com.api.model.VehicleGPSWithTS;
import org.example.com.api.model.VehicleTrace;
import org.example.com.data.VehicleData;
import org.example.com.repository.IVehiclesRepository;


@SuppressWarnings("ALL")
@Path("/vehicle")
@Produces((MediaType.APPLICATION_JSON))
@Consumes((MediaType.MEDIA_TYPE_WILDCARD))
public class VehicleAPI {

  @Inject
  @Named("RepositoryVehicles")
  private IVehiclesRepository repository;

  public VehicleAPI() {
  }

  @Inject
  public VehicleAPI(@Named("RepositoryVehicles") final IVehiclesRepository repository) {
    this.repository = repository;
  }

  @GET
  @Path("/{start}/{end}/{id}/gps")
  @Produces((MediaType.APPLICATION_JSON))
  @Consumes((MediaType.MEDIA_TYPE_WILDCARD))
  public CompletableFuture<ServerResponse<VehicleTrace>> traceVehicle(
      @PathParam("start") final Long start,
      @PathParam("end") final Long end,
      @PathParam("id") final String id
  ) {
    return CompletableFuture.supplyAsync(() -> getTraceOf(repository, start, end, id))
        .thenApply(vehicleTrace -> ServerResponse.response(vehicleTrace).applicationJson());
  }

  public static VehicleTrace getTraceOf(
      IVehiclesRepository repository, final long from, final long to, String vehicleId
  ) {
    final VehicleTrace vehicleTrace = new VehicleTrace(vehicleId);
    Iterator<VehicleData> iterator = repository.getVehiclesDataWithId(from, to, vehicleId)
        .iterator();
    while (iterator.hasNext()) {
      VehicleData next = iterator.next();
      VehicleGPSWithTS vehicleGPSWithTS = new VehicleGPSWithTS(next.getTs(),
          next.getLon(),
          next.getLat()
      );
      vehicleTrace.getTrace().add(vehicleGPSWithTS);
    }
    return vehicleTrace;
  }

}
