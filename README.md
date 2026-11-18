# commandes-kafka

Deux microservices Spring Boot qui communiquent par événements Kafka, derrière une passerelle Spring Cloud Gateway.

![CI](https://github.com/momo97ndiaye/commandes-kafka/actions/workflows/ci.yml/badge.svg)

## Fonctionnement

```
client ──► gateway :8080 ──► order-service :8081 ──(orders.created)──► stock-service :8082
                                    ▲                                        │
                                    └────────────(stock.results)─────────────┘
```

1. `POST /api/orders` enregistre la commande (`PENDING`) et répond `202 Accepted`.
2. Après validation de la transaction, `order-service` publie `orders.created`, avec la référence produit comme clé : les commandes d'un même produit restent ordonnées.
3. `stock-service` réserve le stock et publie `stock.results` (`RESERVED` ou `REJECTED` avec le motif).
4. `order-service` passe la commande à `CONFIRMED` ou `REJECTED`.

Un message rejoué par Kafka ne réserve pas deux fois : `stock-service` garde les commandes déjà traitées et renvoie la même réponse.

## Stack

Java 17 · Spring Boot 3.3 · Spring Kafka · Spring Cloud Gateway · Spring Data JPA (H2) · JUnit 5 · Kafka embarqué pour les tests

## Lancer en local

```bash
docker compose up -d                          # Kafka
mvn -pl stock-service spring-boot:run
mvn -pl order-service spring-boot:run
mvn -pl gateway spring-boot:run
```

```bash
curl -X POST localhost:8080/api/orders -H 'Content-Type: application/json' -d '{"sku":"SKU-001","quantity":2}'
curl localhost:8080/api/orders/1
curl localhost:8080/api/stock/SKU-001
```

`SKU-001` (10 unités) et `SKU-002` (0 unité) sont chargés au démarrage du service du stock.

## Tests

```bash
mvn verify
```

Les tests démarrent un broker Kafka embarqué : création, publication, confirmation et rejet d'une commande, rejeu d'un message, produit inconnu.

## Limites connues

- La publication se fait après la validation de la transaction, sans table « outbox » : si Kafka est indisponible à cet instant, la commande reste `PENDING`.
- Bases H2 en mémoire, pour garder l'exemple simple.
