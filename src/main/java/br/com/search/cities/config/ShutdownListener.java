package br.com.search.cities.config;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

//@Component
//@AllArgsConstructor
//public class ShutdownListener implements ApplicationListener<ContextClosedEvent> {
//    private SparkSession sparkSession;
//    @Override
//    public void onApplicationEvent(ContextClosedEvent event) {
//        sparkSession.close();
//        System.out.println("Contexto do Spring está sendo encerrado...");
//
//    }
//}
