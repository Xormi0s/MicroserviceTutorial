package com.xormios.ingestion_service.service;

import com.xormios.ingestion_service.dto.EnergyUsageDto;
import com.xormios.ingestion_service.kafka.event.EnergyUsageEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class IngestionService {

    private final KafkaTemplate<String, EnergyUsageEvent> kafkaTemplate;

    public IngestionService(KafkaTemplate<String, EnergyUsageEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void ingestEnergyUsage(EnergyUsageDto energyUsageDto) {
        EnergyUsageEvent event = EnergyUsageEvent.builder()
                .deviceId(energyUsageDto.deviceId())
                .energyUsage(energyUsageDto.energyUsage())
                .timestamp(energyUsageDto.timestamp())
                .build();

        kafkaTemplate.send("energy-usage", event);
        log.info("Ingested Energy Usage Event: {}", event);
    }
}
