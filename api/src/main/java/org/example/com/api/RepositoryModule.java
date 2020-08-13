package org.example.com.api;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import javax.inject.Inject;
import javax.inject.Named;
import org.example.com.repository.IVehiclesRepository;
import org.example.com.repository.VehiclesRepositoryImpl;

public class RepositoryModule extends AbstractModule {

  @Inject
  @Named("repository.host")
  protected String host;

  @Inject
  @Named("repository.port")
  protected int port;

  @Override
  protected void configure() {
    super.configure();
    binder().requestInjection(this);
    bind(IVehiclesRepository.class).annotatedWith(Names.named("RepositoryVehicles"))
        .toInstance(new VehiclesRepositoryImpl(host, port));
  }
}
