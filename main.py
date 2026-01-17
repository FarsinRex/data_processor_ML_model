#this is a FastAPI application which build a REST API that accepts streaming events
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List
import asyncio
from db_glue import Database
from event_gen import EventGenerator
import uvicorn


from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from optimized_worker import SmartBackGroundWorker
from contextlib import asynccontextmanager
#introducing a type hint for the worker instance
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages application lifecycle
    - Startup: Initialize worker instance
    - Shutdown: stop the worker instance

    """
    global worker_instance
    print("Starting background worker"+"\n"+"="*60)
    db = Database()
    db.initialize_schema()
    worker_instance = SmartBackGroundWorker(interval_seconds=10)
    worker_instance.start()
    print("Background worker started"+"\n"+"="*60)
    print("\n"+"="*60)
    #application runs here
    yield
    print("application shutdown")
    if worker_instance:
        worker_instance.stop()
        print("worker stopped")
    print("="*60)

app = FastAPI(title="data_pipeline API", version="1.0", lifespan=lifespan)
db = Database()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EventInput(BaseModel):
    "schema for incoming events"
    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime
    event_type: str = Field(..., description="Type of event")
    data_size_kb: float = Field(..., description="Data size in kb")
    priority: str = Field(...,pattern="^(low|medium|high)$")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id":"evt_12345",
                "timestamp": "2025-01-01T10:00:00",
                "event_type":"order",
                "data_size_kb": 25.5,
                "priority":"medium"
                
            }
        }
class EventResponse(BaseModel):
    "Response after event ingestion"
    success: bool
    event_id: str
    message: str
"""    
class WorkerStats(BaseModel):
    worker statistics response model
    total_batches_processed: int
    total_events_processed: int
    total_cost: float
    runtime_formatted: Optional[str] = None
    is_running: bool
"""
#global counter for monitoring ingestion stats
ingestion_stats = {
    'total_events':0,
    'total_data_kb':0.0,
    'events_per_type':{}
}

@app.get("/")
async def root():
    "Health check endpoint"
    return {
        "status":"running",
        "service":"data_pipeline API",
        "version":"1.0",
        "worker_running": worker_instance.is_alive() if worker_instance else False
    }
@app.post("/ingest_event", response_model = EventResponse)
async def ingest_event(event: EventInput):
    """
        event (EventInput): Ingest a single event into the pipeline
        this is your main data entry point
    """
    try:
        #convert to dict for database
        event_data = event.model_dump()
        event_data['timestamp'] = event.timestamp.isoformat()
        
        #Insert into database
        event_id = db.insert_event(event_data)
        
        if event_id:
            ingestion_stats['total_events'] +=1
            ingestion_stats["total_data_kb"] += event.data_size_kb
            ingestion_stats["events_per_type"][event.event_type]= \
                ingestion_stats['events_per_type'].get(event.event_type, 0) + 1    
            return EventResponse(
                success = True,
                event_id=event.event_id,
                message="Event ingested successfully"
            )
        else:
            return EventResponse(
                success = False,
                event_id = event.event_id,
                message = "Event already exists (duplicate)"
                
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/batch")
async def ingest_batch_events(events: List[EventInput]):
    """
    ingest multiple events at once
    """
    results = {
        'successful':0,
        'duplicates':0,
        'failed':0
    }
    for event in events:
        try:
            event_data = event.model_dump()
            event_data['timestamp'] = event.timestamp.isoformat()
            
            event_id = db.insert_event(event_data)
            
            if event_id:
                results['successful'] += 1
                ingestion_stats['total_events'] +=1
                ingestion_stats["total_data_kb"] += event.data_size_kb
            else:
                results['duplicates'] += 1
        except Exception as e:
            results['failed'] += 1
            print(f"failed to insert event {event.event_id}: {e}")
            
    return {
        "total_recieved":len(events),
        "successful":results['successful'],
        "duplicates":results['duplicates'],
        "failed": results['failed']
        
    }

@app.get("/stats")
async def get_ingestion_stats():
    """Get current ingestion statistics
    """
    unprocessed = db.execute_query(
        "SELECT COUNT(*) FROM events WHERE processed = FALSE",
        fetch=True
    )[0]['count']
    
    return {
        **ingestion_stats,
        'unprocessed_events':unprocessed
    }

@app.get("/events/unprocessed")
async def get_unprocessed_events(limit: int = 100):
    """
    Fetch unprocessed events -  used by batch processer
    """
    events = db.get_unprocessed_events(limit=limit)
    
    return {
        "count":len(events),
        "events":events
    }
# worker management endpoint
@app.get("/worker/stats")
async def get_worker_stats():
    """Get background worker statistics"""
    if not worker_instance:
        raise HTTPException(status_code=503, detail="Worker not initialized")
    
    stats = worker_instance.get_stats()
    stats['is_running'] = worker_instance.is_alive()
    stats['ml_enabled'] = getattr(worker_instance, 'ml_enabled', False)
    return stats
  
@app.get("/debug/worker")
async def debug_worker():
    """Debug worker stats"""
    if not worker_instance:
        return {"error": "No worker"}
    
    return {
        "has_ml_enabled": hasattr(worker_instance, 'ml_enabled'),
        "ml_enabled_value": getattr(worker_instance, 'ml_enabled', None),
        "stats_keys": list(worker_instance.stats.keys()),
        "raw_stats": worker_instance.stats
    }
    
@app.get("/ml/stats")
async def get_ml_stats():
    """
    check ML model statistics
    """
    if not worker_instance:
        return {"ml_enabled": False, "reason": "Worker not initialized"}
    
    return {
        "ml enabled": worker_instance.ml_enabled if hasattr(worker_instance, 'ml_enabled') else False,
        "ml_predictions_used": worker_instance.stats.get('ml_predictions_used',0),
        "message":"ML optimization active" if worker_instance.ml_enabled else "Using fixed batch size"
    }
    
@app.get("/worker/status")
async def get_worker_status():
    """check if worker is running

    Returns:
        _type_:dict
    """
    if not worker_instance:
        return {"status":"not_initialized"}
    stats = worker_instance.get_stats()
    stats['is_running'] = worker_instance.is_alive()
    stats['ml_enabled'] = getattr(worker_instance, 'ml_enabled', False)
    
    return stats
# Background Task to simulate continuous event generation
simulator_running = False
async def simulate_event_stream():
    """
    Simulates a real data source generating event
    in production, this would be Apache Kafka, webhooks, etc
    """
    global simulator_running
    if simulator_running:
        print("simulator already running")
        return
    simulator_running =  True
    generator = EventGenerator()
    print("\n"+"="*60)
    print("generating 5 events per second")
    print("="*60 +"\n")
    for event in generator.stream_events(events_per_second=5):
        try:
            event_data = event.to_dict()
            db.insert_event(event_data)
            ingestion_stats['total_events'] += 1
            ingestion_stats['total_data_kb'] += event.data_size_kb
        except Exception as e:
            print(f"Error in event stream: {e}")
            
        await asyncio.sleep(0.2) #5 events per second

@app.post("/simulator/start")
async def start_simulator(background_tasks: BackgroundTasks):
    """
    start the event generator (for testing) 
    """
    if simulator_running:
        return {"message":"simulator already running"}
    background_tasks.add_task(simulate_event_stream)
    return {"message":"Event simulator started"}
@app.get("/simulator/status")
async def get_simulator_status():
    """Check simulator status"""
    return {
        "running": simulator_running,
        "events_generated": ingestion_stats['total_events']
    }
@app.get("/database/stats")
async def get_database_stats():
    #total events
    total_events = db.execute_query(
        "SELECT COUNT(*) as count FROM events",
        fetch=True
    )[0]['count']
    #processed vs unprocessed
    processed = db.execute_query(
        "SELECT COUNT(*) AS count FROM events WHERE processed = TRUE",
        fetch = True
    )[0]['count']
    unprocessed = total_events - processed
    #total batches
    total_batches = db.execute_query(
        "SELECT COUNT(*) AS count FROM batches",
        fetch=True
    )[0]['count']
    
    total_cost = float(db.execute_query(
        "SELECT COALESCE(SUM(processing_cost), 0) AS total_cost FROM batches",
        fetch=True
    )[0]['total_cost']
    )
    #average batch metrics
    avg_metrics = db.execute_query(
    """
    SELECT
    AVG(batch_size) as avg_batch_size,
    AVG(processing_time_seconds) as avg_processing_time,
    AVG(processing_cost) as avg_cost
    FROM batches
    """,
    fetch=True
    )[0]
    
    return{
        "events":
            {
                "total":total_events,
                "processed":processed,
                "unprocessed":unprocessed,
                "processing_rate":f"{(processed/total_events*100):.1f}%" if total_events > 0 else "0%"
            },
        "batches":
            {
                "total" : total_batches,
                "avg_size": round(float(avg_metrics['avg_batch_size'])),
                "avg_processing_time":round(float(avg_metrics['avg_processing_time'] or 0),2),
                "avg_cost" : round(float(avg_metrics['avg_cost'] or 0), 4)
            },
        "costs": {
            "total":round(total_cost,3),
            "per_event": round(total_cost / processed, 4) if processed > 0 else 0
        }
    } 
@app.get("/batches/recent")
async def get_recent_batches(limit: int = 10):
    """Fetch recent processed batches"""
    query = """
    SELECT
    id,
    batch_size,
    processing_cost,
    processing_time_seconds,
    started_at
    FROM batches
    WHERE status = 'completed'
    ORDER BY id DESC
    LIMIT %s
    """
    batches = db.execute_query(query, (limit,), fetch=True)
    # Reverse to show oldest first (left to right on chart)
    return list(reversed(batches))
"""
integrating worker logic to the FastAPI app
Designing a lifespan context manager for handling
startup and shutdown events of the background worker
"""

if __name__ == "__main__":
    #Initialize database
    db.initialize_schema()
    
    #start server
    uvicorn.run(app, host='0.0.0.0', port=8000)