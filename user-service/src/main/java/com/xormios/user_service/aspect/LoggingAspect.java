package com.xormios.user_service.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class LoggingAspect {

    @Pointcut("execution(* com.xormios.user_service.service.*.*(..))")
    public void serviceMethods(){}

    @Before("serviceMethods()")
    public void beforeServiceMethods(JoinPoint joinPoint) throws Throwable {
        log.info("Service method called: {} with arguments {}", joinPoint.getSignature().getName(), joinPoint.getArgs());
    }

    @AfterReturning(pointcut = "serviceMethods()", returning = "result")
    public void afterServiceMethods(JoinPoint joinPoint, Object result) throws Throwable {
        log.info("Service method called: {} with result: {}", joinPoint.getSignature().getName(), result);
    }
}
