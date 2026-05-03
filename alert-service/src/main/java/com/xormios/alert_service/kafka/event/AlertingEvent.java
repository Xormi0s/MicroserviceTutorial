package com.xormios.alert_service.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertingEvent {
        private Long userId;
        private String message;
        private double threshold;
        private double energyConsumed;
        private String email;
}
