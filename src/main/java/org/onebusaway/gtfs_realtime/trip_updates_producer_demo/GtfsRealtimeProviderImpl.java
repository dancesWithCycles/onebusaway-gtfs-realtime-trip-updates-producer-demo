/**
 * Copyright (C) 2012 Google, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_realtime.trip_updates_producer_demo;

import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import de.swingbe.ifleet.controller.PgConnection;
import de.swingbe.ifleet.controller.PgPrepStatement;
import org.json.JSONException;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeExporterModule;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeLibrary;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeMutableProvider;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class produces GTFS-realtime trip updates and vehicle positions by
 * periodically polling the custom SEPTA vehicle data API and converting the
 * resulting vehicle data into the GTFS-realtime format.
 * <p>
 * Since this class implements {@link GtfsRealtimeProvider}, it will
 * automatically be queried by the {@link GtfsRealtimeExporterModule} to export
 * the GTFS-realtime feeds to file or to host them using a simple web-server, as
 * configured by the client.
 *
 * @author bdferris
 */
@Singleton
public class GtfsRealtimeProviderImpl {

    private static final Logger _log = LoggerFactory.getLogger(GtfsRealtimeProviderImpl.class);

    private ScheduledExecutorService _executor;

    private GtfsRealtimeMutableProvider _gtfsRealtimeProvider;

    private URL _url;

    /**
     * How often vehicle data will be downloaded, in seconds.
     */
    private int _refreshInterval = 30;

    @Inject
    public void setGtfsRealtimeProvider(GtfsRealtimeMutableProvider gtfsRealtimeProvider) {
        _gtfsRealtimeProvider = gtfsRealtimeProvider;
    }

    /**
     * @param url the URL for the SEPTA vehicle data API.
     */
    public void setUrl(URL url) {
        _url = url;
    }

    /**
     * @param refreshInterval how often vehicle data will be downloaded, in
     *                        seconds.
     */
    public void setRefreshInterval(int refreshInterval) {
        _refreshInterval = refreshInterval;
    }

    /**
     * The start method automatically starts up a recurring task that periodically
     * downloads the latest vehicle data from the SEPTA vehicle stream and
     * processes them.
     */
    @PostConstruct
    public void start() {
        _log.info("starting GTFS-realtime service");
        _executor = Executors.newSingleThreadScheduledExecutor();
        _executor.scheduleAtFixedRate(new VehiclesRefreshTask(), 0, _refreshInterval, TimeUnit.SECONDS);
    }

    /**
     * The stop method cancels the recurring vehicle data downloader task.
     */
    @PreDestroy
    public void stop() {
        _log.info("stopping GTFS-realtime service");
        _executor.shutdownNow();
    }

    /****
     * Private Methods - Here is where the real work happens
     ****/

    /**
     * This method downloads the latest vehicle data, processes each vehicle in
     * turn, and create a GTFS-realtime feed of trip updates and vehicle positions
     * as a result.
     */
    private void refreshVehicles() throws IOException, JSONException {

        /**
         * connect to pg
         */
        //connection URL for the postgres database
        String host = "host";
        //TODO CLEAN UP String host = "localhost";
        String port = "port";
        String db = "db";
        String usr = "usr";
        String key = "key";

        PgConnection pgCon = new PgConnection(host, port, db, usr, key);

        if (pgCon.getConnection() == null) {
            pgCon.setConnection();
            System.out.println("main() pgCon set");
        }

        PgPrepStatement pgPrepStatement = new PgPrepStatement(pgCon);

        /**
         * get location messages by date, tenant and trip
         */
        ArrayList<ArrayList<String>> aryLctMsgs = pgPrepStatement.get("%", "%GER%", "12:00:1%", "%", "%", "lct_msg_alb");
        if (aryLctMsgs == null) {
            _log.error("no reply from PostgreSQL API");
            return;
        }

        /**
         * The FeedMessage.Builder is what we will use to build up our GTFS-realtime
         * feeds. We create a feed for both trip updates and vehicle positions.
         */
        FeedMessage.Builder tripUpdates = GtfsRealtimeLibrary.createFeedMessageBuilder();
        FeedMessage.Builder vehiclePositions = GtfsRealtimeLibrary.createFeedMessageBuilder();

        /**
         * We iterate over every JSON vehicle object.
         */
        for (int i = 0; i < aryLctMsgs.size(); ++i) {

            ArrayList<String> aryRecord = aryLctMsgs.get(i);
            String trip = aryRecord.get(0);
            String route = aryRecord.get(1);
            String tenant = aryRecord.get(2);
            String stopId = "stopId";
            String strLat = aryRecord.get(5);
            String strLon = aryRecord.get(6);
            if (strLat.length() == 0 || strLon.length() == 0) {
                //TODO add exception handling
                continue;
            }
            double lat = Double.parseDouble(strLat);
            double lon = Double.parseDouble(strLon);
            int delay = 1;

            /**
             * We construct a TripDescriptor and VehicleDescriptor, which will be used
             * in both trip updates and vehicle positions to identify the trip and
             * vehicle. Ideally, we would have a trip id to use for the trip
             * descriptor, but the SEPTA api doesn't include it, so we settle for a
             * route id instead.
             */
            TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
            tripDescriptor.setTripId(trip);
            tripDescriptor.setRouteId(route);

            VehicleDescriptor.Builder vehicleDescriptor = VehicleDescriptor.newBuilder();
            vehicleDescriptor.setId(tenant);

            /**
             * To construct our TripUpdate, we create a stop-time arrival event for
             * the next stop for the vehicle, with the specified arrival delay. We add
             * the stop-time update to a TripUpdate builder, along with the trip and
             * vehicle descriptors.
             */
            StopTimeEvent.Builder arrival = StopTimeEvent.newBuilder();
            arrival.setDelay(delay * 60);

            StopTimeUpdate.Builder stopTimeUpdate = StopTimeUpdate.newBuilder();
            stopTimeUpdate.setArrival(arrival);
            stopTimeUpdate.setStopId(stopId);

            TripUpdate.Builder tripUpdate = TripUpdate.newBuilder();
            tripUpdate.addStopTimeUpdate(stopTimeUpdate);
            tripUpdate.setTrip(tripDescriptor);
            tripUpdate.setVehicle(vehicleDescriptor);

            /**
             * Create a new feed entity to wrap the trip update and add it to the
             * GTFS-realtime trip updates feed.
             */
            FeedEntity.Builder tripUpdateEntity = FeedEntity.newBuilder();
            tripUpdateEntity.setId(trip);
            tripUpdateEntity.setTripUpdate(tripUpdate);
            tripUpdates.addEntity(tripUpdateEntity);

            /**
             * To construct our VehiclePosition, we create a position for the vehicle.
             * We add the position to a VehiclePosition builder, along with the trip
             * and vehicle descriptors.
             */

            Position.Builder position = Position.newBuilder();
            position.setLatitude((float) lat);
            position.setLongitude((float) lon);

            VehiclePosition.Builder vehiclePosition = VehiclePosition.newBuilder();
            vehiclePosition.setPosition(position);
            vehiclePosition.setTrip(tripDescriptor);
            vehiclePosition.setVehicle(vehicleDescriptor);

            /**
             * Create a new feed entity to wrap the vehicle position and add it to the
             * GTFS-realtime vehicle positions feed.
             */
            FeedEntity.Builder vehiclePositionEntity = FeedEntity.newBuilder();
            vehiclePositionEntity.setId(tenant);
            vehiclePositionEntity.setVehicle(vehiclePosition);
            vehiclePositions.addEntity(vehiclePositionEntity);
        }

        /**
         * Build out the final GTFS-realtime feed messagse and save them.
         */
        _gtfsRealtimeProvider.setTripUpdates(tripUpdates.build());
        _gtfsRealtimeProvider.setVehiclePositions(vehiclePositions.build());

        _log.info("vehicles extracted: " + tripUpdates.getEntityCount());
    }

    /**
     * Task that will download new vehicle data from the remote data source when
     * executed.
     */
    private class VehiclesRefreshTask implements Runnable {

        @Override
        public void run() {
            try {
                _log.info("refreshing vehicles");
                refreshVehicles();
            } catch (Exception ex) {
                _log.warn("Error in vehicle refresh task", ex);
            }
        }
    }

}
