import random
import time
from datetime import datetime, timezone
from dataclasses import dataclass
import uuid
from typing import Iterator

@dataclass
class Event:
    event_id: str
    timestamp: datetime
    event_type: str
    data_size_kb: float
    priority: str

    def to_dict(self):
        return {
            'event_id': self.event_id,
            'timestamp': self.timestamp.isoformat(),
            'event_type': self.event_type,
            'data_size_kb': self.data_size_kb,
            'priority': self.priority
        }

class EventGenerator:
    """Generates realistic streaming events."""
    def __init__(self):
        self.event_types = ['order', 'payment', 'login', 'search', 'view', 'refund']
        self.priorities = ['low', 'medium', 'high']
        self.event_count = 0

    def generate_event(self) -> Event:
        """Create one realistic event."""
        self.event_count += 1
        event_type = random.choice(self.event_types)

        size_ranges = {
            'order': (10, 50),
            'payment': (5, 15),
            'login': (1, 3),
            'search': (2, 8),
            'view': (3, 10),
            'refund': (12,30)
        }
        data_size_kb = round(random.uniform(*size_ranges[event_type]), 2)

        priority = random.choices(
            self.priorities,
            weights=[0.6, 0.3, 0.1],
            k=1
        )[0]

        # Use uuid to avoid collisions across processes; still keep event_count if you want readable IDs
        event_id = f"evt_{self.event_count}_{uuid.uuid4().hex[:8]}"

        return Event(
            event_id=event_id,
            timestamp=datetime.now(timezone.utc),
            event_type=event_type,
            data_size_kb=data_size_kb,
            priority=priority
        )

    def stream_events(self, events_per_second: float = 10.0, max_events: int | None = None) -> Iterator[Event]:
        """
        Continuously generate events at the specified rate; yields Event objects.
        If max_events is provided, stop after that many events (for testing purpose).
        
        """
        if events_per_second <= 0:
            raise ValueError("events_per_second must be > 0")
        delay = 1.0 / events_per_second
        n = 0
        while True:
            event = self.generate_event()
            yield event
            n += 1
            if max_events is not None and n >= max_events:
                break
            time.sleep(delay)

if __name__ == "__main__":
    generator = EventGenerator()
    print("generating 20 sample events:\n")
    for i, event in enumerate(generator.stream_events(events_per_second=20, max_events=100)):
        print(f"Event {i+1}: {event.to_dict()}")
