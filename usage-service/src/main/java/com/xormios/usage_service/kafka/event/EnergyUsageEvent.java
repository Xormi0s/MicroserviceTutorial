package com.xormios.usage_service.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;

import java.time.Instant;

@Builder
public record EnergyUsageEvent (
        Long deviceId,
        double energyUsage,
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp) {}
