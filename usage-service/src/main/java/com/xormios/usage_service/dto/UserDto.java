package com.xormios.usage_service.dto;

public record UserDto (
        Long id,
        String firstname,
        String lastname,
        String email,
        String address,
        boolean alerting,
        double energyAlertingThreshold){}
