# db_glue.py - PostgreSQL connection manager
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os

class Database:
    """Manages PostgreSQL connections"""
    
    def __init__(self):
        # For local development - later we'll use environment variables
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'pipeline_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'Farsin'),
            'port': int(os.getenv('DB_PORT', 5432))
        }
        print(f"Database config: {self.config['host']}:{self.config['port']}/{self.config['database']}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connection"""
        conn = psycopg2.connect(**self.config)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute a single query"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                return cursor.rowcount
    
    def insert_event(self, event_data: dict):
        """Insert a single event"""
        query = """
            INSERT INTO events (event_id, timestamp, event_type, data_size_kb, priority)
            VALUES (%(event_id)s, %(timestamp)s, %(event_type)s, %(data_size_kb)s, %(priority)s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, event_data)
                result = cursor.fetchone()
                return result[0] if result else None
    
    def get_unprocessed_events(self, limit: int = 100):
        """Fetch events waiting to be processed"""
        query = """
            SELECT * FROM events 
            WHERE processed = FALSE 
            ORDER BY timestamp ASC 
            LIMIT %s;
        """
        return self.execute_query(query, (limit,), fetch=True)
    
    def initialize_schema(self):
        """Create all tables"""
        try:
            with open('schema.sql', 'r') as f:
                schema = f.read()
        
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema)
            print("Database schema initialized")
        except FileNotFoundError:
            print("Schema file not found. Please ensure 'schema.sql' exists.")
        except Exception as e:
            print(f"Error initializing schema: {e}")

# Test connection
if __name__ == "__main__":
    db = Database()
    db.initialize_schema()
    
    # Test insert
    test_event = {
        'event_id': 'test_001',
        'timestamp': '2025-01-01 10:00:00',
        'event_type': 'test',
        'data_size_kb': 5.5,
        'priority': 'low'
    }
    
    event_id = db.insert_event(test_event)
    print(f"Inserted test event with ID: {event_id}")
    
    # Test fetch
    events = db.get_unprocessed_events(limit=5)
    print(f"Found {len(events)} unprocessed events")