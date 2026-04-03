# Inventory Pipeline 

## Project Title
Design and Evaluation of an Intelligent, Scalable, and Cost-Aware Cloud Data
Pipeline for Real-Time and Batch Inventory Monitoring & Auto-Restocking

## Use Case
E-commerce inventory monitoring. When customers place orders, stock levels
drop in real time. The system detects low stock and automatically triggers
a restock order to the supplier.

## Problem Statement (Plain English)
- **What data is coming?** Customer orders, warehouse scans, supplier files
- **How fast?** Bursty — slow on normal days, 10,000+ orders/min during sales
- **What insights are needed?** Low stock alerts, restock forecasts, demand patterns
- **What if it fails?** Out-of-stock items still sold → failed deliveries → revenue loss

## Success Metrics
| # | Metric | Target |
|---|--------|--------|
| 1 | Stock update delay after order | < 5 seconds |
| 2 | Pipeline survives traffic spike | 10x normal volume |
| 3 | Cost per GB processed | 20% cheaper after optimization |

## Architecture Decision
- **Stream processing:** Real-time stock level updates (can't wait)
- **Batch processing:** Nightly restock forecast (can wait)
- **Type:** Hybrid (Lambda Architecture)

## Tech Stack
| Layer | Tool |
|-------|------|
| Ingestion | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Batch Processing | PySpark Batch |
| Hot Storage | PostgreSQL |
| Cold Storage | Local folder → AWS S3 |
| Orchestration | Apache Airflow |
| Dashboard | Grafana |
| Deployment | Docker + docker-compose |
| Cloud | AWS Free Tier |

## Project Phases
- [x] Phase 1 — Problem Definition
- [x] Phase 2 — Pipeline Design
- [x] Phase 3 — Technology Stack locked
- [x] Phase 4 — Smallest Working Pipeline
- [ ] Phase 5 — Batch Processing + Airflow
- [ ] Phase 6 — Monitoring & Fault Testing
- [ ] Phase 7 — Intelligence & Optimization
- [ ] Phase 8 — Evaluation & Analysis
- [ ] Phase 9 — Documentation & Presentation

## Results So Far
- 1000 order events generated and streamed
- 470 restock alerts detected and saved to PostgreSQL
- Full pipeline working end-to-end locally

## Repository Structure
- data-generator — fake data generator (1000 orders)
- ingestion — Kafka producer
- stream-processor — reads Kafka, saves to PostgreSQL
- batch-processor — coming in Phase 5
- orchestration — Airflow DAGs coming in Phase 5
- monitoring — Grafana config coming in Phase 6
- experiments — benchmark results coming in Phase 8
- infra — Terraform AWS config coming in Phase 5

