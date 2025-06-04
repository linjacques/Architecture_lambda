from datetime import datetime
import random

def generate_log():
    ip = f"192.168.0.{random.randint(1, 255)}"
    user_agents = ["Mozilla/5.0", "Chrome/91.0", "Safari/14.0", "Edge/91.0"]
    user_agent = random.choice(user_agents)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (timestamp, ip, user_agent)
