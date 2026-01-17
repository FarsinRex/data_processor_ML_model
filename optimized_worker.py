import time
import threading
from datetime import datetime
from typing import Optional

from batch_processor import BatchProcessor
from ml_model import BatchOptimizer
from db_glue import Database

class SmartBackGroundWorker:
    """
    Background worker that uses ML to optimize batch sizes dynamically
    """
    def __init__(self, interval_seconds: int = 30):
        self.db = Database()
        self.interval_seconds = interval_seconds
        self.is_running = False
        self.thread = None
        
        #ML optimizer
        self.optimizer = BatchOptimizer()
        self.ml_enabled = False
        
        #try to load existing model
        if self.optimizer.load_model():
            self.ml_enabled = True
            print("ML model loaded successfully, smart optimization enabled")
        else:
            print("No existing ML model found, running with default settings")
            
        # stats tracking:
        self.stats = {
            'total_batches_processed': 0,
            'total_events_processed': 0,
            'total_cost': 0.0,
            'ml_predictions_used':0,
            'started_at': None,
            'last_batch_at': None
        }
        
        print("optimized background worker initialized")
        print(f"   Check interval: {interval_seconds}s")
        
    def get_smart_batch_size(self) -> int:
        """
        Use ML to predict optimal batch size based on current conditions
        Falls back to 50 if Ml not available
        """
        unprocessed_count = self.db.execute_query(
            "SELECT COUNT(*) as count FROM events WHERE processed = FALSE",
            fetch = True
            
        )[0]['count']
        
        if unprocessed_count == 0:
            return 50
        
        recent_batch = self.db.execute_query(
        """
        SELECT
        processing_time_seconds,
        total_data_size_kb / batch_size as avg_data_per_event
        FROM batches
        WHERE status = 'completed'
        ORDER BY id DESC
        LIMIT 1
        """
        , fetch = True)
        
        if not recent_batch:
            return 50
        
        last_batch = recent_batch[0]
        
        #get unprocessed event stats
        unprocessed_stats = self.db.execute_query(
        """
        SELECT
        SUM(data_size_kb) as total_data_kb,
        AVG(data_size_kb) as avg_data_kb
        FROM events
        WHERE processed = FALSE
        LIMIT 200
        """, fetch = True)[0]
        
        total_data_kb = float(unprocessed_stats['total_data_kb'] or 0)
        avg_data_kb = float(unprocessed_stats['avg_data_kb'] or 15.0)
        
        #predict optimal batch size
        predicted_size = self.optimizer.predict_optimal_batch_size(
            total_data_kb = total_data_kb,
            avg_data_per_event = avg_data_kb,
            hour_of_day = datetime.now().hour,
            last_processing_time = float(last_batch['processing_time_seconds'] or 5.0),
            last_cost_per_event = 0.007 #average from training
        )
        self.stats['ml_predictions_used'] +=1
        
        print(f"ML prediction: batch_size = {predicted_size}"
                f"(queue: {unprocessed_count} events, {total_data_kb:.1f}KB)")
        
        return predicted_size
    
    def _worker_loop(self):
        """
        Main worker loop with ML-based batch sizing
        
        """
        print(f"\n{'='*60}")
        print(f"smart worker at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}\n")
        
        self.stats['started_at'] = datetime.now()
        
        while self.is_running:
            try:
                batch_size = self.get_smart_batch_size()
                
                processor = BatchProcessor(batch_size=batch_size)
                result = processor.process_batch()
                
                if result:
                    self.stats['total_batches_processed'] += 1
                    self.stats['total_events_processed'] += result['events_processed']
                    self.stats['total_cost'] += result['cost']
                    self.stats['last_batch_at'] = datetime.now()
                    
                    print("worker stats")
                    print(f"  total batches: {self.stats['total_batches_processed']}")
                    print(f"  total events: {self.stats['total_events_processed']}")
                    print(f"  total cost: ${self.stats['total_cost']: .3f}")
                    print(f"  ML predictions: {self.stats['ml_predictions_used']}\n")
                    
                else:
                    print(f"   no events to process.waiting {self.interval_seconds}s..\n")
                    
                time.sleep(self.interval_seconds)
            
            except KeyboardInterrupt:
                print(f" worker interrupted by user: {e}")
                
            except Exception as e:
                print(f"  Error in worker loop: {e}")
                print(f" retrying in {self.interval_seconds}s..\n")
                time.sleep(self.interval_seconds)
                
        
        print(f"\n {'='*60}")
        print(f" smart worker stopped at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        
    def start(self):
        """
        start the background worker
        """            
        if self.is_running:
            print("worker already running")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        print("Smart worker thread started")
        
    def stop(self):
        """Stop the background worker."""
        if not self.is_running:
            print("Worker not running!")
            return
    
        print("\nStopping smart worker...")
        self.is_running = False
    
        if self.thread:
            self.thread.join(timeout=5)
    
        print("Smart worker stopped")
        
        
    def get_stats(self):
        """Get worker statistics

        Returns:
            _dict_: _returns a dictionary of statistics of the worker_
        """
        stats = self.stats.copy()
        
        if stats['started_at']:
            runtime = datetime.now() - stats['started_at']
            stats['runtime_seconds'] = runtime.total_seconds()
            stats['runtime_formatted'] = str(runtime).split('.')[0]
    
        return stats
    
    def is_alive(self):
        """Check if worker thread is running."""
        return self.thread is not None and self.thread.is_alive()
    
if __name__ == "__main__":
    print("testing optimized background worker")
    print('='*60)
    
    worker = SmartBackGroundWorker(interval_seconds=10)
    worker.start()
    
    
    print("\n smart worker is running \n Press Ctrcl+C to stop\n")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        worker.stop()
        print("worker stopped by user")
        
        final_stats = worker.get_stats()
        print('\n'+'='*60)
        print("final worker stats:")
        print(f"Batches: {final_stats['total_batches_processed']}")
        print(f"Events: {final_stats['total_events_processed']}")
        print(f"Total Cost: ${final_stats['total_cost']:.3f}")
        print(f"ML predictions: {final_stats['ml_predictions_used']}")
        print('='*60+'\n')
        
    
                   
                        
                        
                    
            