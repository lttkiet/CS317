.PHONY: up down init simulator training streaming dashboard grafana logs ps clean light normal heavy

# ──────────────────────────────────────────────────────────────────────
# Local Docker commands — real Kafka + real Spark
#
# Data Rate Profiles:
#   make light      — 2-10 TPS, 200 customers  (low resource)
#   make normal     — 10-50 TPS, 1K customers  (default)
#   make heavy      — 200 TPS (fixed), 10K customers (stress test)
# ──────────────────────────────────────────────────────────────────────

# Start all services with default (normal) data rate
up:
	@DATA_RATE=$(or $(DATA_RATE),normal) docker compose up -d
	@echo ""
	@echo "Services started (DATA_RATE=$(or $(DATA_RATE),normal))"
	@echo "  Kafka:       localhost:9093 (external) / kafka:9092 (internal)"
	@echo "  Spark UI:    http://localhost:8080"
	@echo "  PostgreSQL:  localhost:5432"
	@echo "  MinIO:       http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  Grafana:     http://localhost:3000 (admin/admin)"
	@echo "  Dashboard:   http://localhost:8501"
	@echo ""
	@echo "Run 'make logs' to follow output"

# Light profile — low resource usage
light:
	@DATA_RATE=light docker compose up -d
	@echo ""
	@echo "Services started (DATA_RATE=light)"
	@echo "  TPS: 2-10 | Customers: 200 | Terminals: 1,000"
	@echo "  Spark: 1g memory, 1 executor"

# Normal profile — medium load
normal:
	@DATA_RATE=normal docker compose up -d
	@echo ""
	@echo "Services started (DATA_RATE=normal)"
	@echo "  TPS: 10-50 | Customers: 1,000 | Terminals: 10,000"
	@echo "  Spark: 2g memory, 2 executors"

# Heavy profile — high throughput
heavy:
	@DATA_RATE=heavy docker compose up -d
	@echo ""
	@echo "Services started (DATA_RATE=heavy)"
	@echo "  TPS: 200 (fixed) | Customers: 10,000 | Terminals: 100,000"
	@echo "  Spark: 4g memory, 4 executors"

# Stop everything
down:
	docker compose down -v

# Stop and remove all data
clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# Seed Kafka topic + PostgreSQL schema + MinIO buckets
init:
	docker compose run --rm init

# Run simulator only
simulator:
	docker compose up simulator

# Run training (one-shot)
training:
	docker compose run --rm training

# Run streaming consumer
streaming:
	docker compose up streaming

# Run Streamlit dashboard
dashboard:
	docker compose up -d dashboard
	@echo ""
	@echo "Dashboard: http://localhost:8501"

# Run Grafana dashboard
grafana:
	docker compose up -d grafana
	@echo ""
	@echo "Grafana: http://localhost:3000 (admin/admin)"

# Tail all logs
logs:
	docker compose logs -f

# Show running services
ps:
	docker compose ps

# Restart a specific service
restart-%:
	docker compose restart $*

# View logs for a specific service
log-%:
	docker compose logs -f $*

# Kafka: list topics
kafka-topics:
	docker exec fraud-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Kafka: check consumer lag
kafka-lag:
	docker exec fraud-kafka kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group fraud-streaming --describe

# Kafka: produce test message
kafka-test:
	docker exec -it fraud-kafka kafka-console-producer.sh \
		--bootstrap-server localhost:9092 \
		--topic transactions_topic

# Build technical report (PDF + DOCX) from docs/report.md
report: docs/report.pdf docs/report.docx

docs/report.pdf: docs/report.md docs/refs.bib docs/flow.pdf
	@docker run --rm -v "$(PWD):/work" -w /work texlive/texlive:latest bash -c \
		"apt-get update -qq 2>/dev/null && apt-get install -y -qq pandoc poppler-utils >/dev/null 2>&1 && \
		pandoc docs/report.md --pdf-engine=xelatex --toc --number-sections --top-level-division=chapter --citeproc --bibliography=docs/refs.bib --resource-path=docs -V toc-depth=2 -o docs/report.pdf"
	@echo "PDF built: docs/report.pdf ($$(docker run --rm -v '$(PWD):/work' -w /work texlive/texlive:latest bash -c 'apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq poppler-utils >/dev/null 2>&1 && pdfinfo docs/report.pdf 2>/dev/null | grep Pages' 2>/dev/null))"

docs/report.docx: docs/report.md docs/refs.bib docs/flow.pdf
	@docker run --rm -v "$(PWD):/work" -v "/tmp/template_en.docx:/template.docx" -w /work texlive/texlive:latest bash -c \
		"apt-get update -qq 2>/dev/null && apt-get install -y -qq pandoc >/dev/null 2>&1 && \
		pandoc docs/report.md --citeproc --bibliography=docs/refs.bib --resource-path=docs --reference-doc=/template.docx -o docs/report.docx"
	@echo "DOCX built: docs/report.docx"

docs/flow.pdf: docs/flow.tex
	@docker run --rm -v "$(PWD)/docs/flow.tex:/work/flow.tex" -v "$(PWD)/docs:/out" -w /work texlive/texlive:latest bash -c \
		"pdflatex flow.tex 2>&1 | tail -1 && cp flow.pdf /out/flow.pdf"

# PostgreSQL: connect
psql:
	docker exec -it fraud-postgres psql -U fraud -d fraud_detection

# Spark UI
spark-ui:
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark Worker UI: http://localhost:8081"

# Check service health
health:
	@echo "=== Service Health ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"

# Show available data rate profiles
profiles:
	@echo ""
	@echo "Available DATA_RATE profiles:"
	@echo "─────────────────────────────────────────────────────────────"
	@echo "  light   | Low resource — laptops, small demos"
	@echo "          | TPS: 2-10 | Customers: 200 | Terminals: 1,000"
	@echo "          | Spark: 1g memory, 1 executor, 20 partitions"
	@echo "─────────────────────────────────────────────────────────────"
	@echo "  normal  | Medium load — desktops, full demo (default)"
	@echo "          | TPS: 10-50 | Customers: 1,000 | Terminals: 10,000"
	@echo "          | Spark: 2g memory, 2 executors, 100 partitions"
	@echo "─────────────────────────────────────────────────────────────"
	@echo "  heavy   | High throughput — servers, stress testing"
	@echo "          | TPS: 200 (fixed) | Customers: 10,000 | Terminals: 100,000"
	@echo "          | Spark: 4g memory, 4 executors, 200 partitions"
	@echo "─────────────────────────────────────────────────────────────"
	@echo ""
	@echo "Usage:"
	@echo "  make light          # Start with light profile"
	@echo "  make heavy          # Start with heavy profile"
	@echo "  DATA_RATE=light make up   # Alternative syntax"
