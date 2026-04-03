# Phase 6 — Fault Testing Results

## Test 1: Kafka Container Killed
- Time of failure: 02:28:00
- What broke: New messages cannot be sent to topic
- Monitor impact: None — monitor reads from PostgreSQL directly
- Recovery command: docker start kafka
- Recovery time: ~15 seconds
- Data lost: 0 (Kafka persists messages on disk)
- Conclusion: Kafka failure is invisible to read operations

## Test 2: PostgreSQL Container Killed
- Time of failure: 02:31:35
- What broke: Monitor crashed immediately
- Error: psycopg2.OperationalError — Connection refused on port 5432
- Recovery command: docker start postgres
- Recovery time: ~112 seconds (02:31:35 → 02:33:27)
- Data lost: Any writes during downtime window
- Conclusion: PostgreSQL is single point of failure for reads/writes

## Test 3: Burst Traffic Spike (Flash Sale Simulation)
- Mode: burst (1000 orders at 0.001s delay)
- Result: All 1000 orders sent successfully
- Restock alerts fired: 470
- Pipeline survived: YES
- Data lost: 0
- Conclusion: Pipeline handles 10x traffic spike — Success Metric 2 achieved ✅

## Key Findings
1. Kafka failure = no impact on monitoring (resilient read layer)
2. PostgreSQL failure = immediate crash (needs auto-restart in production)
3. Burst traffic = handled perfectly (Kafka buffers the spike)
4. Kubernetes would auto-restart crashed containers (Phase improvement)

## Recovery Recommendations
- Add PostgreSQL health check with auto-restart in docker-compose
- Add Kafka consumer retry logic in stream_processor.py
- Kubernetes deployment handles this automatically in production