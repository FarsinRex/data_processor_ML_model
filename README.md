# ML-Powered Real-Time Data Pipeline with Predictive Cost Optimization

A production-grade streaming data pipeline that uses machine learning to dynamically optimize batch processing sizes, reducing cloud processing costs through intelligent workload adaptation.

## Live Demo
- **API:** https://dataprocessormlmodel-production.up.railway.app
- **Dashboard:** https://datadog-blush.vercel.app
- **API Docs:** https://dataprocessormlmodel-production.up.railway.app/docs

---

## Project Overview

objective: Most data pipelines use fixed batch sizes to process regardless of workload conditions, leading to inefficient resource usage and unnecessary costs. This project solves that by:

1. Ingesting streaming events in real-time
2. Processing them in batches
3. Learning from historical processing data using ML
4. Dynamically predicting optimal batch sizes to minimize cost per event
5. use the optimal batch size for further cloud processing needs

---

## Architecture

```
Event Generator → FastAPI → PostgreSQL → Batch Processor → Cost Metrics
                                               ↑
                               Smart Worker (ML Predictions)
                                               ↑
                               Random Forest Model (R² score=0.989)
                                               ↑
                               React Dashboard (Live Monitoring)
```

---

## Tech Stack

| Layer : Technology |
|-------|-----------|
| Backend API: FastAPI, Python 3.11 
| Database: PostgreSQL 
| ML Model: Scikit-learn (Random Forest), Pickle, Numpy
| Background Processing: Python Threading, Asynchronous programming
| Containerization: Docker
| Deployment: Railway (Backend), Vercel (Frontend)
| Frontend | React, Chart.js, Tailwind CSS |

---

## Features

### Data Pipeline
- Real-time event ingestion via RESTful API architecture (5 events/second)
- Automated batch processing with configurable batch sizes
- Event prioritization (low, medium, high)
- Duplicate event detection with conflict handling
- Background worker with automatic retry on failure

### Machine Learning
- Random Forest regression model (R²=0.989)
- Feature engineering from historical batch metrics
- Dynamic batch size prediction (range: 20-200 events)
- Model persistence with pickle serialization
- Automatic fallback to default size if model unavailable

### Cost Optimization
- Fixed + variable cost model per batch
- Real-time cost tracking per event
- Historical cost metrics storage
- Cost efficiency trends visualization

### Monitoring Dashboard
- Real-time stat cards (events, batches, cost, ML predictions)
- Cost trends line chart
- ML batch size optimization bar chart
- Auto-refresh every 5-10 seconds

---

## Project Structure

```
ml-data-pipeline/
├── main.py                    # FastAPI application + lifecycle management
├── batch_processor.py         # Core batch processing logic
├── worker.py                  # Fixed-size background worker (baseline)
├── optimized_worker.py        # ML-powered dynamic batch worker
├── ml_model.py                # Random Forest training + prediction
├── event_generator.py         # Synthetic event stream generator
├── database.py                # PostgreSQL connection manager
├── schema.sql                 # Database schema (3 tables, batches, events, cost_metrics)
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container configuration
├── docker-compose.yml         # Local development setup
└── dashboard/
    ├── index.html             # Entry point
    ├── App.jsx                # Main React component
    ├── StatsCards.jsx         # Real-time metrics cards
    ├── CostChart.jsx          # Cost trends visualization
    ├── BatchSizeChart.jsx     # ML optimization chart
    └── api.js                 # API communication layer
```

---

## Database Schema

### events
Stores all incoming events with processing status.
```sql
id, event_id, timestamp, event_type, data_size_kb, priority, processed, batch_id
```

### batches
Tracks batch processing history and costs.
```sql
id, batch_size, total_data_size_kb, processing_time_seconds, processing_cost, status
```

### cost_metrics
ML training data - one record per completed batch.
```sql
id, batch_id, batch_size, total_data_kb, processing_time_seconds, cost_per_event
```

---

## Cost Model

```
Total Cost = Fixed Cost + (Variable Cost × Events)
           = $0.10      + ($0.005        × batch_size)

Cost per Event = Total Cost / batch_size

Example:
- Batch of 10:  $0.15 total → $0.015/event
- Batch of 50:  $0.35 total → $0.007/event
- Batch of 100: $0.60 total → $0.006/event
```

Larger batches amortize the fixed overhead cost, reducing cost per event. The ML model learns the optimal batch size for current workload conditions.

---

## ML Model

### Training Data
Historical batch records from `cost_metrics` table.

### Features
| Feature | Description |
|---------|-------------|
| total_data_kb | Total data volume in queue |
| avg_data_per_event | Average event size |
| hour_of_day | Time-based pattern |
| processing_time_seconds | Recent processing speed |
| cost_per_event | Current efficiency metric |

### Model Performance
- **Algorithm:** Random Forest Regressor
- **R² Score:** 0.989
- **Prediction Range:** 20-200 events (rounded to nearest 10)

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check |
| POST | `/ingest/event` | Ingest single event |
| POST | `/ingest/batch` | Ingest multiple events |
| GET | `/stats` | Ingestion statistics |
| GET | `/database/stats` | Full database metrics |
| GET | `/worker/stats` | Worker + ML statistics |
| GET | `/ml/status` | ML model status |
| POST | `/simulator/start` | Start event generator |
| GET | `/simulator/status` | Simulator status |
| GET | `/batches/recent` | Recent batch history |

---

## Local Development

### Prerequisites
- Python 3.11
- PostgreSQL 15+
- Docker (optional)

### Setup

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/ml-data-pipeline.git
cd ml-data-pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Initialize database
python database.py

# Start application
python main.py
```

### Using Docker

```bash
# Start PostgreSQL
docker-compose up -d

# Build and run app
docker build -t ml-pipeline .
docker run -p 8000:8000 ml-pipeline
```

### Generate Training Data

```bash
# Start API
python main.py

# Start event simulator in bash/terminal
curl -X POST http://localhost:8000/simulator/start

# Wait 5 minutes for 10+ batches, then train, if not enough batches, wait for a few more seconds
python ml_model.py
```

---

## Deployment

### Backend (Railway)
1. Push code to GitHub
2. Connect Railway to repository
3. Add PostgreSQL service
4. Link DATABASE_URL reference variable
5. Deploy automatically on push

### Frontend (Vercel)
1. Update `api.js` with Railway URL in 
2. Drag `dashboard/` folder to Vercel
3. Get public URL instantly

---

## Key Engineering Decisions

**Why FastAPI over Flask?**
Background workers and async event simulation require non-blocking I/O. FastAPI's async function support handles concurrent operations (API + simulator + worker) without blocking.

**Why Random Forest over Linear Regression?**
Batch processing costs have non-linear relationships with batch size (fixed overhead amortization). Random Forest captures these patterns better than linear models.

**Why PostgreSQL over NoSQL?**
Relational structure suits our data (events → batches → metrics relationships). JOIN queries for ML training data extraction are more efficient with SQL.

**Why FAISS-style batching over individual processing?**
Fixed overhead per batch means processing 1 event costs almost the same as 50 events. Batching amortizes this fixed cost, dramatically reducing cost per event.

---

## Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Worker processing speed vs event ingestion rate | Configurable batch size + interval |
| ML model sklearn version mismatch in production | Pin exact versions in requirements.txt |
| Database sequence not resetting after TRUNCATE | ALTER SEQUENCE RESTART WITH 1 |
| React dashboard CORS errors | Added CORSMiddleware to FastAPI |

---

## Future Improvements

- WebSocket support for true real-time dashboard updates
- Authentication (API key middleware)
- Unit tests (pytest)
- Prometheus metrics + Grafana monitoring
- Auto-retraining when model performance degrades
- Rate limiting for API endpoints

---

## Author

**Farsin Pangat**  
ML Developer | Kozhikode, India  
pangatfarsinfarsin0@gmail.com  
[Linkedin](http://www.linkedin.com/in/farsin-pangat-128b9918a)