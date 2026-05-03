package com.xormios.alert_service.service;

import com.xormios.alert_service.entity.Alert;
import com.xormios.alert_service.kafka.event.AlertingEvent;
import com.xormios.alert_service.repository.AlertRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
public class EmailService {

    private final JavaMailSender mailSender;
    private final AlertRepository alertRepository;

    public EmailService(JavaMailSender mailSender, AlertRepository alertRepository) {
        this.mailSender = mailSender;
        this.alertRepository = alertRepository;
    }

    public void sendEmail(String to, String subject, String body, long userId) {
        log.info("Sending email to {}, subject {}", to, subject);

        SimpleMailMessage mailMessage = new SimpleMailMessage();
        mailMessage.setTo(to);
        mailMessage.setFrom("noreply@microservices.com");
        mailMessage.setSubject(subject);
        mailMessage.setText(body);

        try {
            mailSender.send(mailMessage);

            final Alert alert = getAlert(true, userId);

            alertRepository.saveAndFlush(alert);
        } catch (MailException e) {
            log.error("Error sending email to {}, subject {}", to, subject, e);

            final Alert alert = getAlert(false, userId);

            alertRepository.saveAndFlush(alert);
        }
    }

    private Alert getAlert(boolean sent, Long userId) {
        final Alert alert = Alert.builder()
                .sent(sent)
                .createdAt(LocalDateTime.now())
                .userId(userId)
                .build();
        return alert;
    }
}
