package com.xormios.ingestion_service.simulation;

import com.xormios.ingestion_service.dto.EnergyUsageDto;
import jakarta.annotation.PreDestroy;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Component
public class ParallelDataSimulator implements CommandLineRunner {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Random random = new Random();
    private final ExecutorService executor;

    @Value("${simulation.parallel-threads}")
    private int parallelThreads;

    @Value("${simulation.requests-per-interval}")
    private int requestsPerInterval;

    @Value("${simulation.endpoint}")
    private String ingestionEndpoint;

    public ParallelDataSimulator() {
        this.executor = Executors.newCachedThreadPool();
    }
    @Override
    public void run(String... args) throws Exception {
        log.info("Parallel Data Simulator started.");

        ((ThreadPoolExecutor) executor).setCorePoolSize(parallelThreads);
    }

    @Scheduled(fixedRateString = "${simulation.interval-ms}")
    public void sendMockData() {
        int batchSize = requestsPerInterval / parallelThreads;
        int remainder = requestsPerInterval % parallelThreads;

        for(int i = 0; i < parallelThreads; i++) {
            int requestsForThread = batchSize + (i < remainder ? 1 : 0);
            executor.submit(() -> {
                for(int j = 0; j < requestsForThread; j++) {
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
            });
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        log.info("Parallel Data Simulator stopped.");
    }
}
