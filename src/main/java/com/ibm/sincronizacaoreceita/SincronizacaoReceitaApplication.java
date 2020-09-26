package com.ibm.sincronizacaoreceita;

import com.ibm.sincronizacaoreceita.exception.FileInvalidOrNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

@SpringBootApplication
public class SincronizacaoReceitaApplication {

    private static final Log logger = LogFactory.getLog("Exception");

    public static void main(String[] args) throws FileInvalidOrNotFoundException {

        // According with the business case CSV file is ideally sent before 10am GMT-3 (since Banco Central is in Brasília)
        // If it's more them 10 o'clock I'll trigger a warning but I won't stop the application for two reasons
        // 1) Not sure if I should stop it, it isn't precisely specified
        // 2) It's a sample application, I want it to be runnable at anytime

        LocalTime tenOClock = LocalTime.parse("10:00:00");
        LocalTime timeNowInBrazil = LocalTime.ofInstant(Instant.now(), ZoneId.of("America/Sao_Paulo"));
        // I didn't use LocalTime.now() because It would get time from the system where this application is running
        // So if it's running on the cloud it could get time from anywhere and not from Brasília
        // Instant.now() gets the UTC time

        // With this implementation I'm assuming the system's time is right

        // to make it fully automated I would upload it in a cloud platform (like AWS) and set up a Cron Job

        if(timeNowInBrazil.compareTo(tenOClock) == 1)
            logger.warn("Cuidado: o arquivo está sendo enviado depois das 10:00am! Horario atual em Brasília: "
                    + timeNowInBrazil.truncatedTo(ChronoUnit.SECONDS));

        if (args.length == 0)
            throw new FileInvalidOrNotFoundException();

        String[] newArgs = new String[]{"pathToResource=" + args[0]};
        ApplicationContext context = SpringApplication.run(SincronizacaoReceitaApplication.class, newArgs);
        System.exit(SpringApplication.exit(context));

    }

    // Some choices made for this application and why:
    // IBM is a multinational company, this code should be able to be maintained internationally so everything is in English
    // CSV input file is in portuguese so I kept the same language for what will show up for the user (errors and results), that's the only language exception
    // Sicredi has over 4 millions accounts so I decided to implement scalable app with Spring Batch
    // I used multithreaded steps, async itemProcessor and async itemWriter as methods of scaling this application
    // Multithreaded steps: each chuck will have its own thread for better performance
    // Async itemProcessor/itemWriter: The processing piece of a step will be able to run in parallel
    // Spring Batch stores metadata so I needed a database, I used H2 but it should never be used in production


    // TODO: it runs on single JVM, for more multiple JVMs and more processing power I would implement most likely remote chucking or maybe partitioning
    // TODO: fix o.s.b.f.support.DisposableBeanAdapter warning
}


