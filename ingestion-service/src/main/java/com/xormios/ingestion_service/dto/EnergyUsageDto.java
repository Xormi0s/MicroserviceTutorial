package com.xormios.ingestion_service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;

import java.time.Instant;

@Builder
public record EnergyUsageDto (
    long deviceId,
    double energyUsage,
    @JsonFormat(shape =  JsonFormat.Shape.STRING)
    Instant timestamp) {}
