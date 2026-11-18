package sn.mndiaye.orders;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String STOCK_RESULTS = "stock.results";

    @Bean
    NewTopic ordersCreated() {
        return TopicBuilder.name(ORDERS_CREATED).partitions(3).replicas(1).build();
    }
}
