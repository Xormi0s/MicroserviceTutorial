package com.xormios.insight_service.dto;

import lombok.Builder;

@Builder
public record InsightDto (
    long userId,
    String tips,
    double energyUsage)
{}
