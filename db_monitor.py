import requests
import time
import json

while True:
    try:
        response  = requests.get("http://localhost:8000/database/stats")
        if response.status_code == 200:
            data = response.json()
    
        print("\n"+'='*60)
        print(f" events: {data['events']['total']} (Processed: {data['events']['processed']})")
        print(f"batches: {data['batches']['total']}")
        print(f"Total cost: ${data['costs']['total']}")
        print("="*50)
        time.sleep(5)
    
    except KeyboardInterrupt:
        print("Monitoring stopped by user.")
        break
    
    except Exception as e:
        print(f"Error fetching stats: {e}")
        time.sleep(5)
    