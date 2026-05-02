package com.xormios.ingestion_service.simulation;

import com.xormios.ingestion_service.dto.EnergyUsageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Random;

@Slf4j
@Component
public class ContinuousDataSimulator implements CommandLineRunner {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Random random = new Random();

    @Value("${simulation.requests-per-interval}")
    private int requestsPerInterval;

    @Value("${simulation.endpoint}")
    private String ingestionEndpoint;

    @Override
    public void run(String... args) throws Exception {
        log.info("Continuous Data Simulator started.");
    }

    //@Scheduled(fixedRateString = "${simulation.interval-ms}")
    public void sendMockData() {
        for(int i = 0; i < requestsPerInterval; i++) {
            EnergyUsageDto dto = EnergyUsageDto.builder()
                    .deviceId(random.nextLong(1,10))
                    .energyUsage(Math.round(random.nextDouble(0,2) * 100) / 100.0)
                    .timestamp(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant())
                    .build();

            try {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<EnergyUsageDto> request = new HttpEntity<>(dto, headers);
                restTemplate.postForEntity(ingestionEndpoint, request, Void.class);

                log.info("Sent mock data: {}", dto);
            } catch (Exception ex) {
                log.error("Failed to send mock data: {}", ex.getMessage());
            }
        }
    }
}
