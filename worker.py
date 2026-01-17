import time
import threading
from datetime import datetime
from batch_processor import BatchProcessor

class BackgroundWorker:
    """
    Runs batch processing automatically in the background.
    
    This worker continuously checks for unprocessed events and
    processes them in batches at regular intervals.
    """
    
    def __init__(self, batch_size: int = 50, interval_seconds: int = 30):
        """
        Initialize the background worker.
        
        Args:
            batch_size: Number of events to process per batch
            interval_seconds: How often to check for new events (in seconds)
        """
        self.processor = BatchProcessor(batch_size=batch_size)
        self.interval_seconds = interval_seconds
        self.is_running = False
        self.thread = None
        
        # Statistics tracking
        self.stats = {
            'total_batches_processed': 0,
            'total_events_processed': 0,
            'total_cost': 0.0,
            'started_at': None,
            'last_batch_at': None
        }
        
        print(f"BackgroundWorker initialized")
        print(f"   Batch size: {batch_size}")
        print(f"   Check interval: {interval_seconds}s")
        
        
    def _worker_loop(self):
        """
        Main worker loop that runs in background thread.
        
        This method runs continuously, checking for events and
        processing them at regular intervals.
        """
        print(f"\n{'='*60}")
        print(f"Background worker started at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}\n")
        
        self.stats['started_at'] = datetime.now()
        
        while self.is_running:
            try:
                # Process one batch
                result = self.processor.process_batch()
                
                if result:
                    # Update statistics
                    self.stats['total_batches_processed'] += 1
                    self.stats['total_events_processed'] += result['events_processed']
                    self.stats['total_cost'] += result['cost']
                    self.stats['last_batch_at'] = datetime.now()
                    
                    print(f"Worker Stats:")
                    print(f" Total batches: {self.stats['total_batches_processed']}")
                    print(f"Total events: {self.stats['total_events_processed']}")
                    print(f"Total cost: ${self.stats['total_cost']:.3f}\n")
                else:
                    print(f"No events to process. Waiting {self.interval_seconds}s...\n")
                
                # Wait before next check
                time.sleep(self.interval_seconds)
                
            except KeyboardInterrupt:
                print("\n Worker interrupted by user")
                break
            except Exception as e:
                print(f"Error in worker loop: {e}")
                print(f"Retrying in {self.interval_seconds}s...\n")
                time.sleep(self.interval_seconds)
        
        print(f"\n{'='*60}")
        print(f"Background worker stopped at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}\n")
    
    
    def start(self):
        """
        start the background worked ina a seperate thread
        convention to allow the worker run independently without blocking the main program
        
        """
        if self.is_running:
            print(" Worker is already running")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        
        print("Background worker thread started")
        
    def stop(self):
        """
        stop the background worker
        """
        if not self.is_running:
            print("Worker is not running")
            return
        
        print("\n stopping background worker")
        self.is_running = False
        
        if self.thread:
            self.thread.join(timeout=5)
            
        print("background worker stopped")
        
    def get_stats(self):
        """
        get current statistics of the background worker
        """
        stats = self.stats.copy()
        
        if stats['started_at']:
            runtime = datetime.now() - stats['started_at']
            stats['runtime_seconds'] = runtime.total_seconds()
            stats['runtime_formatted'] = str(runtime).split('.')[0]  # Remove microseconds
        
        return stats
    
    def is_alive(self):
        
        return self.thread is not None and self.thread.is_alive()


if __name__ == "__main__":
    worker = BackgroundWorker(batch_size=200, interval_seconds=30)
    worker.start()
    
    print("\n Worker is now running in the background, It will check for events every 10 seconds.")
    print("   Press Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
            
            stats  = worker.get_stats()
            if stats['total_batches_processed'] > 0:
                print(f"Quick stats: {stats['total_batches_processed']},\n {stats['total_events_processed']} events, ${stats['total_cost']} cost")
    except KeyboardInterrupt:
        print("\n stopping worker")
        worker.stop()
        
        final_stats = worker.get_stats()
        print("\n" + "="*60)
        print("final statistics")
        print("="*60)
        print(f"Runtime: {final_stats.get('runtime_formatted', 'N/A')}")
        print(f"Batches processed: {final_stats['total_batches_processed']}")
        print(f"Events processed: {final_stats['total_events_processed']}")
        print(f"Total cost: ${final_stats['total_cost']:.3f}")
        print("="*60)