package sn.mndiaye.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;

@SpringBootTest
class GatewayRoutesTest {

    @Autowired RouteLocator routes;

    @Test
    void routesAreDeclaredForBothServices() {
        assertThat(routes.getRoutes().map(Route::getId).collectList().block())
                .containsExactlyInAnyOrder("orders", "stock");
    }
}
