/**
 * Copyright (C) 2012 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_realtime.trip_updates_producer_demo;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.onebusaway.cli.CommandLineInterfaceLibrary;
import org.onebusaway.guice.jsr250.LifecycleService;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeFileWriter;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeServlet;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class GtfsRealtimeTripUpdatesProducerDemoMain {

  private static final String ARG_TRIP_UPDATES_PATH = "tripUpdatesPath";

  private static final String ARG_TRIP_UPDATES_URL = "tripUpdatesUrl";

  private static final String ARG_VEHICLE_POSITIONS_PATH = "vehiclePositionsPath";

  private static final String ARG_VEHICLE_POSITIONS_URL = "vehiclePositionsUrl";

  private GtfsRealtimeSource _vehiclePositionsSource;

  private GtfsRealtimeSource _tripUpdatesSource;

  public static void main(String[] args) throws Exception {
    GtfsRealtimeTripUpdatesProducerDemoMain m = new GtfsRealtimeTripUpdatesProducerDemoMain();
    m.run(args);
  }

  private GtfsRealtimeProviderImpl _provider;

  private LifecycleService _lifecycleService;

  @Inject
  public void setProvider(GtfsRealtimeProviderImpl provider) {
    _provider = provider;
  }

  @Inject
  public void setLifecycleService(LifecycleService lifecycleService) {
    _lifecycleService = lifecycleService;
  }

  @Inject
  public void setTripUpdatesSource(@TripUpdates
                                           GtfsRealtimeSource tripUpdatesSource) {
    _tripUpdatesSource = tripUpdatesSource;
  }

  @Inject
  public void setVehiclePositionsSource(@VehiclePositions
                                                GtfsRealtimeSource vehiclePositionsSource) {
    _vehiclePositionsSource = vehiclePositionsSource;
  }

  public void run(String[] args) throws Exception {

    if (args.length == 0 || CommandLineInterfaceLibrary.wantsHelp(args)) {
      printUsage();
      System.exit(-1);
    }

    Options options = new Options();
    buildOptions(options);
    Parser parser = new GnuParser();
    CommandLine cli = parser.parse(options, args);

    Set<Module> modules = new HashSet<Module>();
    GtfsRealtimeTripUpdatesProducerDemoModule.addModuleAndDependencies(modules);

    Injector injector = Guice.createInjector(modules);
    injector.injectMembers(this);

    _provider.setUrl(new URL("http://www3.septa.org/hackathon/TrainView/"));

    if (cli.hasOption(ARG_TRIP_UPDATES_URL)) {
      GtfsRealtimeServlet servlet = injector.getInstance(GtfsRealtimeServlet.class);
      servlet.setSource(_tripUpdatesSource);
      servlet.setUrl(new URL(cli.getOptionValue(ARG_TRIP_UPDATES_URL)));
    }
    if (cli.hasOption(ARG_TRIP_UPDATES_PATH)) {
      GtfsRealtimeFileWriter fileWriter = injector.getInstance(GtfsRealtimeFileWriter.class);
      fileWriter.setSource(_tripUpdatesSource);
      fileWriter.setPath(new File(cli.getOptionValue(ARG_TRIP_UPDATES_PATH)));
    }

    if (cli.hasOption(ARG_VEHICLE_POSITIONS_URL)) {
      GtfsRealtimeServlet servlet = injector.getInstance(GtfsRealtimeServlet.class);
      servlet.setSource(_vehiclePositionsSource);
      servlet.setUrl(new URL(cli.getOptionValue(ARG_VEHICLE_POSITIONS_URL)));
    }
    if (cli.hasOption(ARG_VEHICLE_POSITIONS_PATH)) {
      GtfsRealtimeFileWriter fileWriter = injector.getInstance(GtfsRealtimeFileWriter.class);
      fileWriter.setSource(_vehiclePositionsSource);
      fileWriter.setPath(new File(
              cli.getOptionValue(ARG_VEHICLE_POSITIONS_PATH)));
    }

    _lifecycleService.start();
  }

  private void printUsage() {
    CommandLineInterfaceLibrary.printUsage(getClass());
  }

  protected void buildOptions(Options options) {
    options.addOption(ARG_TRIP_UPDATES_PATH, true, "trip updates path");
    options.addOption(ARG_TRIP_UPDATES_URL, true, "trip updates url");
    options.addOption(ARG_VEHICLE_POSITIONS_PATH, true,
        "vehicle positions path");
    options.addOption(ARG_VEHICLE_POSITIONS_URL, true, "vehicle positions url");

  }
}
