import time
import random
from datetime import datetime
from typing import List, Dict, Optional
from db_glue import Database

class BatchProcessor:
    """
    processes events in batches for ML training
    Handles fetching, grouping and processing unprocessed events in batches
    
    this is the core component

    """
    def __init__(self, batch_size: int = 50):
        self.db = Database()
        self.batch_size = batch_size
        
        self.FIXED_COST = 0.10
        self.VARIABLE_COST_PER_EVENT = 0.005
        
        self.PROCESSING_SPEED  = 0.02  # seconds per KB
        
        print(f"batch processor initialized with batch_size={batch_size}")
    
    def fetch_unprocessed_events(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch events from database that havent been processed yet.

        Args:
            limit: maximum number of events to fetch. if none, uses self.batch_size.

        Returns:
            List[Dict]:
        """
        if limit is None:
            limit = self.batch_size
        events = self.db.get_unprocessed_events(limit=limit)
        
        if events:
            print(f" fetched {len(events)} unprocessed events")
        else:
            print("no unprocessed event found")
        
        return events
    
    def calculate_batch_metrics(self, events:List[Dict]) -> Dict:
        total_data_kb = sum(event['data_size_kb'] for event in events)
        event_types = {}
        
        for event in events:
            event_type = event['event_type']
            event_types[event_type] = event_types.get(event_type,0) + 1
            
        return {
            'batch_size' : len(events),
            'total_data_kb' : round(total_data_kb,2),
            'event_types' : event_types,
            'avg_data_per_event' : round(total_data_kb/len(events), 2) if events else 0
        }
            
    def simulate_processing(self, total_data_kb: float, batch_size: int) -> float:
        base_time = total_data_kb * self.PROCESSING_SPEED
        
        randomness = random.uniform(0.8,1.2)
        processing_time = base_time * randomness
        
        processing_time += 0.5
        print(f"processing {batch_size} events ({total_data_kb} KB)..")
        
        time.sleep(processing_time)
        
        return round(processing_time,2)
    
    def calculate_cost(self,batch_size: int, processing_time: float) -> float:
        fixed_cost = self.FIXED_COST
        variable_cost = self.VARIABLE_COST_PER_EVENT * batch_size
        total_cost = fixed_cost + variable_cost
        cost_per_event = total_cost / batch_size if batch_size >0 else 0
        print(f"Batch cost: ${total_cost:.3f} (${cost_per_event:.4f} per event)")
        
        return round(total_cost,4)
    
    def create_batch_record(self, batch_size: int, total_data_kb: float) -> int:
        query = """
        INSERT INTO batches (batch_size, total_data_size_kb, started_at, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    batch_size,
                    total_data_kb,
                    datetime.now(),
                    'processing'
                ))
                batch_id = cursor.fetchone()[0]
                
        print(f"created batch record {batch_id}")
        return batch_id
    def mark_events_as_processed(self, event_ids: List[int], batch_id: int):
        ids_tuple = tuple(event_ids)
        query = """
        UPDATE events
        SET processed = TRUE, batch_id = %s
        WHERE id IN %s;
        """
        rows_updated = self.db.execute_query(query, (batch_id, ids_tuple))
        print(f"Marked {rows_updated} events as processed")
        
    def update_batch_record(self, batch_id: int, processing_time: float, cost: float):
        query = """
        UPDATE batches
        SET processing_time_seconds = %s,
            processing_cost = %s,
            completed_at = %s,
            status = %s
        
        WHERE id = %s;
        """
        self.db.execute_query(query, (
            processing_time,
            cost,
            datetime.now(),
            'completed',
            batch_id
        ))
        
        print(f"updated batch {batch_id} with results")
        
    def save_cost_metrics(self, batch_id: int, batch_size: int, total_data_kb: float, processing_time: float, cost:float):
        print(f"DEBUG: saving cost for batch_id = {batch_id}")
        cost_per_event = cost/batch_size if batch_size >0 else 0
        
        query = """
        INSERT INTO cost_metrics
        (batch_id, batch_size, total_data_kb, processing_time_seconds, cost_per_event, timestamp)
        VALUES (%s, %s, %s,%s, %s, %s);
        
        """
        self.db.execute_query(query, (
            batch_id,
            batch_size,
            total_data_kb,
            processing_time,
            cost_per_event,
            datetime.now()
        ))
        print(f"saved cost metrics for ML training")
        
    def process_batch(self) -> Optional[Dict]:
        """
        Main method: Process one complete batch from start to finish.
        
        This orchestrates the entire workflow:
        1. Fetch unprocessed events
        2. Calculate metrics
        3. Create batch record
        4. Simulate processing
        5. Calculate costs
        6. Mark events as processed
        7. Save all metrics
        
        Returns:
            Dictionary with batch results, or None if no events to process
        """ 
        print("\n"+ "="*60)
        
        #step1 fetch events
        events = self.fetch_unprocessed_events()
        
        if not events:
            print("No events to process")
            return None
        #calculate metrics
        metrics = self.calculate_batch_metrics(events)
        print(f"Batch metrics: {metrics['batch_size']} events, "
              f"{metrics['total_data_kb']} KB total")
        #step 3: create batch record (marks start of processing)
        batch_id = self.create_batch_record(
            metrics['batch_size'],
            metrics['total_data_kb']
        )
        #step 4: simulate processing
        processing_time = self.simulate_processing(
            metrics['total_data_kb'],
            metrics['batch_size']
        )
        
        #step 5: calculate processing cost
        cost = self.calculate_cost(metrics['batch_size'], processing_time)
        
        #step 6: Mark all events as processed
        event_ids = [event['id'] for event in events]
        self.mark_events_as_processed(event_ids, batch_id)
        
        #step 7: update batch record with results
        self.update_batch_record(batch_id, processing_time, cost)
        
        #step 8: save cost metrics for ML training
        self.save_cost_metrics(
            batch_id,
            metrics['batch_size'],
            metrics['total_data_kb'],
            processing_time,
            cost
        )
        
        result = {
            'batch_id': batch_id,
            'events_processed': metrics['batch_size'],
            'total_data_kb': metrics['total_data_kb'],
            'processing_time': processing_time,
            'cost': cost,
            'cost_per_event': round(cost/metrics['batch_size'],4)
        }
        
        print("\n" + "="*60)
        print(f" Batch #{batch_id} completed successfully")
        print(f"processed: {result['events_processed']} events")
        print(f" Time: {result['processing_time']}s")
        print(f" Cost: ${result['cost']}")
        print("="*60 + "\n")
        
        return result
    
if __name__ == "__main__":
    processor = BatchProcessor(batch_size=20)
    print("\n Testing batch processor")
    result = processor.process_batch()
    
    if result:
        print("\n Test successful")
    
    else:
        print("No events to process in test")
        print("Pro tip: run the event simulator first to generate events") 