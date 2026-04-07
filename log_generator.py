import time
import random
import os
from faker import Faker

fake = Faker()

OUTPUT_DIR = "input_logs"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def generate_logs():
    methods = ["GET", "POST", "DELETE", "PUT"]
    endpoints = ["/login", "/api/data", "/admin/config", "/home", "/user/profile"]
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", 
        "Python-requests/2.25", 
        "Nmap-Scanner", 
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "curl/7.68.0"
    ]
    
    print(f"Starting log generation into {OUTPUT_DIR}/...")
    try:
        while True:
            filename = os.path.join(OUTPUT_DIR, f"server_logs_{int(time.time())}.txt")
            with open(filename, "w") as f:
                # generate a batch of 50 logs at a time
                for _ in range(50):
                    ip = fake.ipv4()
                    method = random.choice(methods)
                    endpoint = random.choice(endpoints)
                    
                    # 5% chance of SQL Injection
                    if random.random() < 0.05:
                        endpoint = "/admin/config?query=DROP+TABLE+users"
                    
                    status = random.choice([200, 200, 200, 401, 404, 500])
                    agent = random.choice(user_agents)
                    
                    # Apache Log format: %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"
                    # Example: 192.168.1.1 - - [15/Mar/2026:10:00:00 +0000] "GET /login HTTP/1.1" 200 512 "Mozilla/5.0"
                    timestamp = time.strftime('%d/%b/%Y:%H:%M:%S +0000', time.gmtime())
                    log_line = f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} 512 "{agent}"\n'
                    f.write(log_line)
            
            print(f"Generated {filename}")
            time.sleep(5)  # wait 5 seconds before next log file is written
            
    except KeyboardInterrupt:
        print("\nLog generation stopped.")

if __name__ == "__main__":
    generate_logs()
