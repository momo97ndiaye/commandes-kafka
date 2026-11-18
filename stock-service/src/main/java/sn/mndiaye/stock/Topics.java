package sn.mndiaye.stock;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String STOCK_RESULTS = "stock.results";

    @Bean
    NewTopic stockResults() {
        return TopicBuilder.name(STOCK_RESULTS).partitions(3).replicas(1).build();
    }
}
