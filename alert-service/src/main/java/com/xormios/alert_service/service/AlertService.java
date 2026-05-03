package com.xormios.alert_service.service;

import com.xormios.alert_service.kafka.event.AlertingEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertService {

    private final EmailService emailService;

    public AlertService(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "energy-alerts", groupId = "alert-service")
    public void energyUsageAlertEvent(AlertingEvent alertingEvent) {
        log.info("Received alerting event: {}", alertingEvent);

        final String subject = "Energy usage alert for user " + alertingEvent.getUserId();
        final String message = "Alert: " + alertingEvent.getMessage()
                + "\nThreshold: " + alertingEvent.getThreshold()
                + "\nEnergy Consumed: " + alertingEvent.getEnergyConsumed();

        emailService.sendEmail(alertingEvent.getEmail(), subject, message, alertingEvent.getUserId());
    }
}
