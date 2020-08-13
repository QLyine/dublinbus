package org.example.com.api;

import io.sinistral.proteus.ProteusApplication;

public class Main extends ProteusApplication {

  public static void main(final String[] args) {
    final Main simpleHttpServer = new Main();
    simpleHttpServer.addService(io.sinistral.proteus.openapi.services.OpenAPIService.class);

    simpleHttpServer.addModule(RepositoryModule.class);
    simpleHttpServer.addController(OperatorAPI.class);
    simpleHttpServer.addController(VehicleAPI.class);

    simpleHttpServer.start();
  }

}
