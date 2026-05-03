package com.xormios.usage_service.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.xormios.usage_service.client.DeviceClient;
import com.xormios.usage_service.client.UserClient;
import com.xormios.usage_service.dto.DeviceDto;
import com.xormios.usage_service.dto.UserDto;
import com.xormios.usage_service.kafka.event.AlertingEvent;
import com.xormios.usage_service.kafka.event.EnergyUsageEvent;
import com.xormios.usage_service.model.DeviceEnergy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UsageService {

    private final InfluxDBClient influxDBClient;
    private final DeviceClient deviceClient;
    private final UserClient userClient;
    private final KafkaTemplate<String, AlertingEvent> alertingKafkaTemplate;

    @Value("${influx.bucket}")
    private String influxBucket;

    @Value("${influx.org}")
    private String influxOrg;

    public UsageService(InfluxDBClient influxDBClient, DeviceClient deviceClient, UserClient userClient, KafkaTemplate<String, AlertingEvent> alertingKafkaTemplate) {
        this.influxDBClient = influxDBClient;
        this.deviceClient = deviceClient;
        this.userClient = userClient;
        this.alertingKafkaTemplate = alertingKafkaTemplate;
    }

    @KafkaListener(topics = "energy-usage", groupId = "usage-service")
    public void energyUsageEvent(EnergyUsageEvent energyUsageEvent) {
        log.info("Received energy usage event: {}", energyUsageEvent);

        Point point = Point.measurement("energy-usage")
                .addTag("deviceId", String.valueOf(energyUsageEvent.deviceId()))
                .addField("energyConsumed", energyUsageEvent.energyUsage())
                .time(energyUsageEvent.timestamp(), WritePrecision.MS);
        influxDBClient.getWriteApiBlocking().writePoint(influxBucket, influxOrg, point);
    }

    @Scheduled(cron = "*/10 * * * * *")
    public void aggregateDeviceEnergyUsage() {
    String fluxQuery = String.format("""
        from(bucket: "%s")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "energy-usage")
          |> filter(fn: (r) => r["_field"] == "energyConsumed")
          |> group(columns: ["deviceId"])
          |> sum(column: "_value")
        """, influxBucket);

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery, influxOrg);

        List<DeviceEnergy> deviceEnergies = new ArrayList<>();

        for (FluxTable table : tables) {
            for(FluxRecord record : table.getRecords()) {
                String deviceIdStr = (String) record.getValueByKey("deviceId");
                Double energyConsumed = record.getValueByKey("_value")
                        instanceof Number ? ((Number)record.getValueByKey("_value")).doubleValue() : 0.0;

                deviceEnergies.add(DeviceEnergy.builder()
                                .deviceId(Long.valueOf(deviceIdStr))
                                .energyConsumed(energyConsumed)
                                .build());
            }
        }

        log.info("Aggregate device energy usage over the past hour: {}", deviceEnergies);

        for (DeviceEnergy deviceEnergy : deviceEnergies) {
            try {
                final DeviceDto deviceResponse = deviceClient.getDeviceById(deviceEnergy.getDeviceId());

                if(deviceResponse == null || deviceResponse.userId() == null) {
                    log.warn("Device not found for id: {}", deviceEnergy.getDeviceId());
                    continue;
                }

                deviceEnergy.setUserId(deviceResponse.userId());
            } catch (Exception ex) {
                log.warn("Failed to fetch device for id: {}", deviceEnergy.getDeviceId());
            }
        }

        deviceEnergies.removeIf(d -> d.getUserId() == null);

        Map<Long, List<DeviceEnergy>> deviceEnergyMap = deviceEnergies.stream()
                .collect(Collectors.groupingBy(DeviceEnergy::getUserId));

        log.info("User-Device Energy Map: {}", deviceEnergyMap);

        List<Long> userIds = new ArrayList<>(deviceEnergyMap.keySet());
        final Map<Long, Double> userThresholdMap = new HashMap<>();
        final Map<Long, String> userEmailMap = new HashMap<>();

        for(final Long userId : userIds) {
            try {
                UserDto user = userClient.getUserById(userId);
                if (user == null || user.id() == null || !user.alerting()) {
                    log.warn("User not found or alerting disabled for ID: {}", userId);
                    continue;
                }

                userThresholdMap.put(userId, user.energyAlertingThreshold());
                userEmailMap.put(userId, user.email());
            } catch (Exception ex) {
                log.warn("Failed to fetch user for ID: {}", userId);
            }
        }

        log.info("User-Threshold Map: {}", userThresholdMap);

        final List<Long> alertedUsers = new ArrayList<>(userThresholdMap.keySet());
        for (final Long userId : alertedUsers) {
            final Double userThreshold = userThresholdMap.get(userId);
            final List<DeviceEnergy> devices = deviceEnergyMap.get(userId);

            final Double energyConsumed = devices.stream()
                    .mapToDouble(DeviceEnergy::getEnergyConsumed).sum();

            if(energyConsumed >= userThreshold) {
                log.info("ALERT: User Id {}, Theshold: {}", userId, energyConsumed);

                final AlertingEvent alertingEvent = AlertingEvent.builder()
                        .userId(userId)
                        .message("Energy consumption exceeded threshold")
                        .energyConsumed(energyConsumed)
                        .email(userEmailMap.get(userId))
                        .build();

                alertingKafkaTemplate.send("energy-alerts", alertingEvent);
            } else {
                log.info("User Id {} is within the energy threshold. Total energy consumed: {}, Threshhold {}", userId, energyConsumed, userThreshold);
            }
        }
    }
}
