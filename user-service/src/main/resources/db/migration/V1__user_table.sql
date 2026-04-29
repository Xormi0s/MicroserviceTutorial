CREATE TABLE `user` (
    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `firstname` VARCHAR(100) NOT NULL,
    `lastname` VARCHAR(100),
    `email` VARCHAR(255) NOT NULL,
    `address` TEXT,
    `alerting` TINYINT(1) NOT NULL DEFAULT 0,
    `energyAlertingThreshold` DOUBLE NOT NULL DEFAULT 0,
    UNIQUE KEY `uk_user_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;