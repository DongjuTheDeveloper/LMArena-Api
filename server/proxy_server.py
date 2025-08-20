import asyncio
import json
import logging
import uuid
import re
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional, Set
from dataclasses import dataclass, field, asdict
from enum import Enum
import signal
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.responses import StreamingResponse
import typing
import os
import traceback
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading
from pathlib import Path
import aiohttp  # If you want to send HTTP alerts, you need to run `pip install aiohttp` first
import gzip
import shutil
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# --- Configuration ---
# Centralized configuration management
class Config:
    # Log configuration
    LOG_DIR = Path("logs")
    REQUEST_LOG_FILE = "requests.jsonl"
    ERROR_LOG_FILE = "errors.jsonl"
    MAX_LOG_SIZE = 50 * 1024 * 1024  # 50MB
    MAX_LOG_FILES = 50  # Keep up to 10 historical log files
    
    # Server configuration
    HOST = "0.0.0.0"
    PORT = 9080
    
    # Request configuration
    BACKPRESSURE_QUEUE_SIZE = 5
    REQUEST_TIMEOUT_SECONDS = 180
    MAX_CONCURRENT_REQUESTS = 20  # Maximum number of concurrent requests
    
    # Monitoring configuration
    STATS_UPDATE_INTERVAL = 5  # Statistics update interval (seconds)
    CLEANUP_INTERVAL = 300  # Cleanup interval (seconds)
    
    # Memory limits
    MAX_ACTIVE_REQUESTS = 100
    MAX_LOG_MEMORY_ITEMS = 1000  # Maximum number of log entries kept in memory
    MAX_REQUEST_DETAILS = 500  # Number of request details to keep

    # Network configuration
    MANUAL_IP = None  # Manually specify IP address, e.g., "192.168.0.1"

# Ensure log directory exists
Config.LOG_DIR.mkdir(exist_ok=True)

# --- Dynamic Configuration Management ---
class ConfigManager:
    """Manage dynamically modifiable configuration"""

    def __init__(self):
        self.config_file = Config.LOG_DIR / "config.json"
        self.dynamic_config = {
            "network": {
                "manual_ip": Config.MANUAL_IP,
                "port": Config.PORT,
                "auto_detect_ip": True
            },
            "request": {
                "timeout_seconds": Config.REQUEST_TIMEOUT_SECONDS,
                "max_concurrent_requests": Config.MAX_CONCURRENT_REQUESTS,
                "backpressure_queue_size": Config.BACKPRESSURE_QUEUE_SIZE
            },
            "monitoring": {
                "error_rate_threshold": 0.1,
                "response_time_threshold": 30,
                "active_requests_threshold": 50,
                "cleanup_interval": Config.CLEANUP_INTERVAL
            },
            "quick_links": [
                {"name": "monitoring_panel", "url": "/monitor", "icon": "📊"},
                {"name": "health_check", "url": "/api/health/detailed", "icon": "🏥"},
                {"name": "Prometheus", "url": "/metrics", "icon": "📈"},
                {"name": "API_Documentation", "url": "/monitor#api-docs", "icon": "📚"}
            ]
        }
        self.load_config()

    def load_config(self):
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    saved_config = json.load(f)
                    # Deep merge configuration
                    self._deep_merge(self.dynamic_config, saved_config)
                    logging.info("Loaded saved configuration")
            except Exception as e:
                logging.error(f"Failed to load config file: {e}")

    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.dynamic_config, f, ensure_ascii=False, indent=2)
            logging.info("Configuration saved")
        except Exception as e:
            logging.error(f"Failed to save config file: {e}")

    def _deep_merge(self, target, source):
        """Deep merge dictionaries"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    def get(self, path: str, default=None):
        """Get config value, supports dot path like 'network.manual_ip'"""
        keys = path.split('.')
        value = self.dynamic_config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def set(self, path: str, value):
        """Set config value"""
        keys = path.split('.')
        target = self.dynamic_config
        for key in keys[:-1]:
            if key not in target:
                target[key] = {}
            target = target[key]
        target[keys[-1]] = value
        self.save_config()

    def get_display_ip(self):
        """Get display IP address"""
        if self.get('network.manual_ip'):
            return self.get('network.manual_ip')
        elif self.get('network.auto_detect_ip', True):
            return get_local_ip()
        else:
            return "localhost"


# Create global configuration manager
config_manager = ConfigManager()


def get_local_ip():
    """Get local LAN IP address"""
    import socket
    import platform

    # Check if config_manager exists and manual IP is set
    if 'config_manager' in globals():
        manual_ip = config_manager.get('network.manual_ip')
        if manual_ip:
            return manual_ip

    # Get all possible IP addresses
    ips = []

    try:
        # Method 1: Get all network interface IPs
        hostname = socket.gethostname()
        all_ips = socket.gethostbyname_ex(hostname)[2]

        # Filter out LAN IPs (exclude virtual network cards)
        for ip in all_ips:
            if not ip.startswith('127.') and not ip.startswith('198.18.'):
                parts = ip.split('.')
                if len(parts) == 4:
                    first_octet = int(parts[0])
                    second_octet = int(parts[1])
                    if (first_octet == 10 or
                        (first_octet == 172 and 16 <= second_octet <= 31) or
                        (first_octet == 192 and second_octet == 168)):
                        ips.append(ip)

        # If found LAN IP, return the first (usually the main one)
        if ips:
            # Prefer 192.168 addresses
            for ip in ips:
                if ip.startswith('192.168.'):
                    return ip
            return ips[0]

        # Method 2: If above fails, try connecting to external server
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("223.5.5.5", 80))  # Use AliDNS instead of Google
        local_ip = s.getsockname()[0]
        s.close()

        # Check if it is a Clash address
        if not local_ip.startswith('198.18.'):
            return local_ip

    except Exception as e:
        logging.warning(f"Failed to get IP address: {e}")

    # If all methods fail, return localhost
    return "127.0.0.1"

# Configure Python logging system
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler(Config.LOG_DIR / "server.log", encoding='utf-8')  # File output
    ]
)

# --- Prometheus Metrics ---
# Request counter
request_count = Counter(
    'lmarena_requests_total', 
    'Total number of requests',
    ['model', 'status', 'type']
)

# Request duration histogram
request_duration = Histogram(
    'lmarena_request_duration_seconds',
    'Request duration in seconds',
    ['model', 'type'],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, float("inf"))
)

# Number of active requests
active_requests_gauge = Gauge(
    'lmarena_active_requests',
    'Number of active requests'
)

# Token usage counter
token_usage = Counter(
    'lmarena_tokens_total',
    'Total number of tokens used',
    ['model', 'token_type']  # token_type: input/output
)

# WebSocket connection status
websocket_status = Gauge(
    'lmarena_websocket_connected',
    'WebSocket connection status (1=connected, 0=disconnected)'
)

# Error counter
error_count = Counter(
    'lmarena_errors_total',
    'Total number of errors',
    ['error_type', 'model']
)

# Number of registered models
model_registry_gauge = Gauge(
    'lmarena_models_registered',
    'Number of registered models'
)

# --- Request Details Storage ---
@dataclass
class RequestDetails:
    """Store detailed request information"""
    request_id: str
    timestamp: float
    model: str
    status: str
    duration: float
    input_tokens: int
    output_tokens: int
    error: Optional[str]
    request_params: dict
    request_messages: list
    response_content: str
    headers: dict
    
class RequestDetailsStorage:
    """Manage storage of request details"""
    def __init__(self, max_size: int = Config.MAX_REQUEST_DETAILS):
        self.details: Dict[str, RequestDetails] = {}
        self.order: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()
    
    def add(self, details: RequestDetails):
        """Add request details"""
        with self._lock:
            if details.request_id in self.details:
                return
            
            # If reached maximum capacity, delete the oldest
            if len(self.order) >= self.order.maxlen:
                oldest_id = self.order[0]
                if oldest_id in self.details:
                    del self.details[oldest_id]
            
            self.details[details.request_id] = details
            self.order.append(details.request_id)
    
    def get(self, request_id: str) -> Optional[RequestDetails]:
        """Get request details"""
        with self._lock:
            return self.details.get(request_id)
    
    def get_recent(self, limit: int = 100) -> list:
        """Get recent request details"""
        with self._lock:
            recent_ids = list(self.order)[-limit:]
            return [self.details[id] for id in reversed(recent_ids) if id in self.details]

# Create request details storage
request_details_storage = RequestDetailsStorage()

# --- Log Manager ---
class LogManager:
    """Manage JSON Lines format log files"""
    
    def __init__(self):
        self.request_log_path = Config.LOG_DIR / Config.REQUEST_LOG_FILE
        self.error_log_path = Config.LOG_DIR / Config.ERROR_LOG_FILE
        self._lock = threading.Lock()
        self._check_and_rotate()
    
    def _check_and_rotate(self):
        """Check and rotate log files"""
        for log_path in [self.request_log_path, self.error_log_path]:
            if log_path.exists() and log_path.stat().st_size > Config.MAX_LOG_SIZE:
                self._rotate_log(log_path)
    
    def _rotate_log(self, log_path: Path):
        """Rotate log file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rotated_path = log_path.with_suffix(f".{timestamp}.jsonl")
        
        # Move current log file
        shutil.move(log_path, rotated_path)
        
        # Compress old log
        with open(rotated_path, 'rb') as f_in:
            with gzip.open(f"{rotated_path}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Delete uncompressed file
        rotated_path.unlink()
        
        # Clean up old log files
        self._cleanup_old_logs()
    
    def _cleanup_old_logs(self):
        """Clean up old log files"""
        log_files = sorted(Config.LOG_DIR.glob("*.jsonl.gz"), key=lambda x: x.stat().st_mtime)
        
        # Keep the latest N files
        while len(log_files) > Config.MAX_LOG_FILES:
            oldest_file = log_files.pop(0)
            oldest_file.unlink()
            logging.info(f"Deleted old log file: {oldest_file}")
    
    def write_request_log(self, log_entry: dict):
        """Write request log"""
        with self._lock:
            self._check_and_rotate()
            with open(self.request_log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    
    def write_error_log(self, log_entry: dict):
        """Write error log"""
        with self._lock:
            self._check_and_rotate()
            with open(self.error_log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    
    def read_request_logs(self, limit: int = 100, offset: int = 0, model: str = None) -> list:
        """Read request log"""
        logs = []
        
        # Read current log file
        if self.request_log_path.exists():
            with open(self.request_log_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                
                # Read in reverse order (latest first)
                for line in reversed(all_lines):
                    try:
                        log = json.loads(line.strip())
                        if log.get('type') == 'request_end':  # Only return completed requests
                            if model and log.get('model') != model:
                                continue
                            logs.append(log)
                            if len(logs) >= limit + offset:
                                break
                    except json.JSONDecodeError:
                        continue
        
        # Return logs in the specified range
        return logs[offset:offset + limit]
    
    def read_error_logs(self, limit: int = 50) -> list:
        """Read error log"""
        logs = []
        
        if self.error_log_path.exists():
            with open(self.error_log_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                
                # Read in reverse order (latest first)
                for line in reversed(all_lines[-limit:]):
                    try:
                        log = json.loads(line.strip())
                        logs.append(log)
                    except json.JSONDecodeError:
                        continue
        
        return logs

# Create global log manager
log_manager = LogManager()

# --- Performance Monitor ---
class PerformanceMonitor:
    """Performance monitor"""
    
    def __init__(self):
        self.request_times = deque(maxlen=1000)  # Recent 1000 requests' response time
        self.model_performance = defaultdict(lambda: {
            'count': 0,
            'total_time': 0,
            'errors': 0,
            'last_hour_requests': deque(maxlen=3600)  # Recent one hour's requests
        })
    
    def record_request(self, model: str, duration: float, success: bool):
        """Record request performance"""
        self.request_times.append(duration)
        
        perf = self.model_performance[model]
        perf['count'] += 1
        perf['total_time'] += duration
        if not success:
            perf['errors'] += 1
        
        # Record timestamp for calculating QPS
        perf['last_hour_requests'].append(time.time())
    
    def get_stats(self) -> dict:
        """Get performance statistics"""
        if not self.request_times:
            return {
                'avg_response_time': 0,
                'p50_response_time': 0,
                'p95_response_time': 0,
                'p99_response_time': 0,
                'qps': 0
            }
        
        sorted_times = sorted(self.request_times)
        n = len(sorted_times)
        
        # Calculate current QPS
        current_time = time.time()
        recent_requests = sum(1 for t in self.request_times if current_time - t < 60)
        
        return {
            'avg_response_time': sum(sorted_times) / n,
            'p50_response_time': sorted_times[n // 2],
            'p95_response_time': sorted_times[int(n * 0.95)],
            'p99_response_time': sorted_times[int(n * 0.99)],
            'qps': recent_requests / 60.0
        }
    
    def get_model_stats(self) -> dict:
        """Get model statistics"""
        stats = {}
        current_time = time.time()
        
        for model, perf in self.model_performance.items():
            # Calculate recent one hour's QPS
            recent_count = sum(1 for t in perf['last_hour_requests'] 
                             if current_time - t < 3600)
            
            stats[model] = {
                'total_requests': perf['count'],
                'avg_response_time': perf['total_time'] / max(1, perf['count']),
                'error_rate': perf['errors'] / max(1, perf['count']),
                'qps': recent_count / 3600.0
            }
        
        return stats

# Create performance monitor
performance_monitor = PerformanceMonitor()

# --- WebSocket Heartbeat Management ---
class WebSocketHeartbeat:
    def __init__(self, interval: int = 30):
        self.interval = interval
        self.last_ping = time.time()
        self.last_pong = time.time()
        self.missed_pongs = 0
        self.max_missed_pongs = 3

    async def start_heartbeat(self, ws: WebSocket):
        """Start heartbeat task"""
        while not SHUTTING_DOWN and ws:
            try:
                current_time = time.time()

                # Check if pong response received
                if current_time - self.last_pong > self.interval * 2:
                    self.missed_pongs += 1
                    if self.missed_pongs >= self.max_missed_pongs:
                        logging.warning("Heartbeat timeout, browser may be disconnected")
                        await self.notify_disconnect()
                        break

                # Send ping
                await ws.send_text(json.dumps({"type": "ping", "timestamp": current_time}))
                self.last_ping = current_time

                await asyncio.sleep(self.interval)

            except Exception as e:
                logging.error(f"Failed to send heartbeat: {e}")
                break

    def handle_pong(self):
        """Handle pong response"""
        self.last_pong = time.time()
        self.missed_pongs = 0

    async def notify_disconnect(self):
        """Notify monitor dashboard of disconnection"""
        await broadcast_to_monitors({
            "type": "alert",
            "severity": "warning",
            "message": "Browser WebSocket heartbeat timeout",
            "timestamp": time.time()
        })


# --- Monitoring Alert System ---
class MonitoringAlerts:
    def __init__(self):
        self.alert_history = deque(maxlen=100)
        self.alert_thresholds = {
            "error_rate": 0.1,  # 10% error rate
            "response_time_p95": 30,  # 30 seconds
            "active_requests": 50,  # 50 concurrent requests
            "websocket_disconnect_time": 300  # 5 minutes
        }
        self.last_check = time.time()
        self.last_disconnect_time = 0

    async def check_system_health(self):
        """Periodically check system health"""
        while not SHUTTING_DOWN:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                alerts = []

                # Check error rate
                error_rate = self.calculate_error_rate()
                if error_rate > self.alert_thresholds["error_rate"]:
                    alerts.append({
                        "type": "high_error_rate",
                        "severity": "warning",
                        "message": f"High error rate: {error_rate:.1%}",
                        "value": error_rate
                    })

                # Check response time
                perf_stats = performance_monitor.get_stats()
                p95_time = perf_stats.get("p95_response_time", 0)
                if p95_time > self.alert_thresholds["response_time_p95"]:
                    alerts.append({
                        "type": "slow_response",
                        "severity": "warning",
                        "message": f"Slow P95 response time: {p95_time:.1f} seconds",
                        "value": p95_time
                    })

                # Check active request count
                active_count = len(realtime_stats.active_requests)
                if active_count > self.alert_thresholds["active_requests"]:
                    alerts.append({
                        "type": "high_load",
                        "severity": "warning",
                        "message": f"High active requests: {active_count}",
                        "value": active_count
                    })

                # Check WebSocket connection
                if not browser_ws:
                    if self.last_disconnect_time == 0:
                        self.last_disconnect_time = time.time()
                    disconnect_time = time.time() - self.last_disconnect_time
                    if disconnect_time > self.alert_thresholds["websocket_disconnect_time"]:
                        alerts.append({
                            "type": "browser_disconnected",
                            "severity": "critical",
                            "message": f"Browser disconnected {int(disconnect_time / 60)} minutes ago",
                            "value": disconnect_time
                        })
                else:
                    self.last_disconnect_time = 0

                # Send alert to monitor dashboard
                for alert in alerts:
                    await self.send_alert(alert)

            except Exception as e:
                logging.error(f"Health check failed: {e}")

    def calculate_error_rate(self) -> float:
        """Calculate error rate for the last 5 minutes"""
        current_time = time.time()
        recent_requests = [
            req for req in realtime_stats.recent_requests
            if current_time - req.get('end_time', 0) < 300  # within 5 minutes
        ]

        if not recent_requests:
            return 0.0

        failed = sum(1 for req in recent_requests if req.get('status') == 'failed')
        return failed / len(recent_requests)

    async def send_alert(self, alert: dict):
        """Send alert to monitor dashboard"""
        alert["timestamp"] = time.time()
        self.alert_history.append(alert)

        # Broadcast to all monitor clients
        await broadcast_to_monitors({
            "type": "alert",
            "alert": alert
        })

        logging.warning(f"System alert: {alert['message']}")


# Create instance
heartbeat = WebSocketHeartbeat()
monitoring_alerts = MonitoringAlerts()


# --- Real-time Statistics Data Structure ---
@dataclass
class RealtimeStats:
    active_requests: Dict[str, dict] = field(default_factory=dict)
    recent_requests: deque = field(default_factory=lambda: deque(maxlen=Config.MAX_LOG_MEMORY_ITEMS))
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=50))
    model_usage: Dict[str, dict] = field(default_factory=lambda: defaultdict(lambda: {
        'requests': 0, 'tokens': 0, 'errors': 0, 'avg_duration': 0
    }))
    
    def cleanup_old_requests(self):
        """Clean up timed-out active requests"""
        current_time = time.time()
        timeout_requests = []
        
        for req_id, req in self.active_requests.items():
            if current_time - req['start_time'] > Config.REQUEST_TIMEOUT_SECONDS:
                timeout_requests.append(req_id)
        
        for req_id in timeout_requests:
            logging.warning(f"Cleaning up timed out request: {req_id}")
            del self.active_requests[req_id]

realtime_stats = RealtimeStats()

# --- Cleanup Task ---
async def periodic_cleanup():
    """Periodic cleanup task"""
    while not SHUTTING_DOWN:
        try:
            # Clean up timed-out active requests
            realtime_stats.cleanup_old_requests()
            
            # Trigger log rotation check
            log_manager._check_and_rotate()
            
            # Update Prometheus metrics
            active_requests_gauge.set(len(realtime_stats.active_requests))
            model_registry_gauge.set(len(MODEL_REGISTRY))
            
            logging.info(f"Cleanup task completed. Active requests: {len(realtime_stats.active_requests)}")
            
        except Exception as e:
            logging.error(f"Cleanup task error: {e}")
        
        await asyncio.sleep(Config.CLEANUP_INTERVAL)

# --- Custom Streaming Response with Immediate Flush ---
class ImmediateStreamingResponse(StreamingResponse):
    """Custom streaming response that forces immediate flushing of chunks"""

    async def stream_response(self, send: typing.Callable) -> None:
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })

        async for chunk in self.body_iterator:
            if chunk:
                # Send the chunk immediately
                await send({
                    "type": "http.response.body",
                    "body": chunk.encode(self.charset) if isinstance(chunk, str) else chunk,
                    "more_body": True,
                })
                # Force a small delay to ensure the chunk is sent
                await asyncio.sleep(0)

        # Send final empty chunk to close the stream
        await send({
            "type": "http.response.body",
            "body": b"",
            "more_body": False,
        })


# --- Request State Management ---
class RequestStatus(Enum):
    PENDING = "pending"
    SENT_TO_BROWSER = "sent_to_browser"
    PROCESSING = "processing"
    COMPLETED = "completed"
    TIMEOUT = "timeout"
    ERROR = "error"


@dataclass
class PersistentRequest:
    request_id: str
    openai_request: dict
    response_queue: asyncio.Queue
    status: RequestStatus = RequestStatus.PENDING
    created_at: float = field(default_factory=time.time)
    sent_to_browser_at: Optional[float] = None
    last_activity_at: Optional[float] = None
    model_name: str = ""
    is_streaming: bool = True
    accumulated_response: str = ""  # Store response content


class PersistentRequestManager:
    def __init__(self):
        self.active_requests: Dict[str, PersistentRequest] = {}
        self._lock = asyncio.Lock()

    async def add_request(self, request_id: str, openai_request: dict, response_queue: asyncio.Queue,
                    model_name: str, is_streaming: bool) -> PersistentRequest:
        """Add a new request to be tracked"""
        async with self._lock:
            # Check if exceeds maximum concurrent number
            if len(self.active_requests) >= Config.MAX_CONCURRENT_REQUESTS:
                raise HTTPException(status_code=503, detail="Too many concurrent requests")
            
            persistent_req = PersistentRequest(
                request_id=request_id,
                openai_request=openai_request,
                response_queue=response_queue,
                model_name=model_name,
                is_streaming=is_streaming
            )
            self.active_requests[request_id] = persistent_req
            
            # Update Prometheus metrics
            active_requests_gauge.inc()
            
            logging.info(f"REQUEST_MGR: Added request {request_id} for tracking")
            return persistent_req

    def get_request(self, request_id: str) -> Optional[PersistentRequest]:
        """Get a request by ID"""
        return self.active_requests.get(request_id)

    def update_status(self, request_id: str, status: RequestStatus):
        """Update request status"""
        if request_id in self.active_requests:
            self.active_requests[request_id].status = status
            self.active_requests[request_id].last_activity_at = time.time()
            logging.debug(f"REQUEST_MGR: Updated request {request_id} status to {status.value}")

    def mark_sent_to_browser(self, request_id: str):
        """Mark request as sent to browser"""
        if request_id in self.active_requests:
            self.active_requests[request_id].sent_to_browser_at = time.time()
            self.update_status(request_id, RequestStatus.SENT_TO_BROWSER)

    async def timeout_request(self, request_id: str):
        """Timeout a request and send error to client"""
        if request_id in self.active_requests:
            req = self.active_requests[request_id]
            req.status = RequestStatus.TIMEOUT

            # Send timeout error to client
            try:
                await req.response_queue.put({
                    "error": f"Request timed out after {Config.REQUEST_TIMEOUT_SECONDS} seconds. Browser may have disconnected during Cloudflare challenge."
                })
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error(f"REQUEST_MGR: Error sending timeout to queue for {request_id}: {e}")

            # Remove from active requests
            del self.active_requests[request_id]
            active_requests_gauge.dec()
            logging.warning(f"REQUEST_MGR: Request {request_id} timed out and removed")

    def complete_request(self, request_id: str):
        """Mark request as completed and remove from tracking"""
        if request_id in self.active_requests:
            self.active_requests[request_id].status = RequestStatus.COMPLETED
            del self.active_requests[request_id]
            active_requests_gauge.dec()
            logging.info(f"REQUEST_MGR: Request {request_id} completed and removed")

    def get_pending_requests(self) -> Dict[str, PersistentRequest]:
        """Get all requests that were sent to browser but not completed"""
        return {
            req_id: req for req_id, req in self.active_requests.items()
            if req.status in [RequestStatus.SENT_TO_BROWSER, RequestStatus.PROCESSING]
        }

    async def request_timeout_watcher(self, requests_to_watch: Dict[str, PersistentRequest]):
        """A background task to watch for and time out disconnected requests."""
        try:
            await asyncio.sleep(Config.REQUEST_TIMEOUT_SECONDS)

            logging.info(f"WATCHER: Timeout reached. Checking {len(requests_to_watch)} requests.")
            for request_id, req in requests_to_watch.items():
                # Check if the request is still pending (i.e., not completed by a reconnect)
                if self.get_request(request_id) and req.status in [RequestStatus.SENT_TO_BROWSER,
                                                                   RequestStatus.PROCESSING]:
                    logging.warning(f"WATCHER: Request {request_id} timed out after browser disconnect.")
                    await self.timeout_request(request_id)
        except asyncio.CancelledError:
            logging.info("WATCHER: Request timeout watcher was cancelled, likely due to server shutdown.")
        except Exception as e:
            logging.error(f"WATCHER: Error in request timeout watcher: {e}", exc_info=True)

    async def handle_browser_disconnect(self):
        """Handle browser WebSocket disconnect - spawn timeout watchers for pending requests."""
        pending_requests = self.get_pending_requests()
        if not pending_requests:
            return

        logging.warning(f"REQUEST_MGR: Browser disconnected with {len(pending_requests)} pending requests.")

        # Check if we're shutting down
        global SHUTTING_DOWN
        if SHUTTING_DOWN:
            # During shutdown, timeout immediately to avoid hanging
            logging.info("REQUEST_MGR: Server shutting down, timing out all pending requests immediately.")
            for request_id in list(pending_requests.keys()):
                logging.info(f"REQUEST_MGR: Timing out request {request_id} due to shutdown.")
                await self.timeout_request(request_id)
        else:
            # During normal operation, spawn a watcher task for the timeout
            logging.info(f"REQUEST_MGR: Spawning timeout watcher for {len(pending_requests)} pending requests.")
            watcher_task = asyncio.create_task(self.request_timeout_watcher(pending_requests.copy()))
            background_tasks.add(watcher_task)
            watcher_task.add_done_callback(background_tasks.discard)


# --- Logging Functions ---
def log_request_start(request_id: str, model: str, params: dict, messages: list = None):
    """Log request start"""
    request_info = {
        'id': request_id,
        'model': model,
        'start_time': time.time(),
        'status': 'active',
        'params': params,
        'messages': messages or []
    }
    
    realtime_stats.active_requests[request_id] = request_info
    
    # Write to log file
    log_entry = {
        'type': 'request_start',
        'timestamp': time.time(),
        'request_id': request_id,
        'model': model,
        'params': params
    }
    log_manager.write_request_log(log_entry)
    
def log_request_end(request_id: str, success: bool, input_tokens: int = 0, 
                   output_tokens: int = 0, error: str = None, response_content: str = ""):
    """Log request end"""
    if request_id not in realtime_stats.active_requests:
        return
        
    req = realtime_stats.active_requests[request_id]
    duration = time.time() - req['start_time']
    
    # Update real-time statistics
    req['status'] = 'success' if success else 'failed'
    req['duration'] = duration
    req['input_tokens'] = input_tokens
    req['output_tokens'] = output_tokens
    req['error'] = error
    req['end_time'] = time.time()
    req['response_content'] = response_content
    
    # Add to recent requests list
    realtime_stats.recent_requests.append(req.copy())
    
    # Update model statistics
    model = req['model']
    stats = realtime_stats.model_usage[model]
    stats['requests'] += 1
    if success:
        stats['tokens'] += input_tokens + output_tokens
    else:
        stats['errors'] += 1
    
    # Record performance
    performance_monitor.record_request(model, duration, success)
    
    # Update Prometheus metrics
    request_count.labels(model=model, status='success' if success else 'failed', type='chat').inc()
    request_duration.labels(model=model, type='chat').observe(duration)
    token_usage.labels(model=model, token_type='input').inc(input_tokens)
    token_usage.labels(model=model, token_type='output').inc(output_tokens)
    
    # Save request details
    details = RequestDetails(
        request_id=request_id,
        timestamp=req['start_time'],
        model=model,
        status='success' if success else 'failed',
        duration=duration,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        error=error,
        request_params=req.get('params', {}),
        request_messages=req.get('messages', []),
        response_content=response_content[:5000],  # Limit length
        headers={}
    )
    request_details_storage.add(details)
    
    # Write to log file
    log_entry = {
        'type': 'request_end',
        'timestamp': time.time(),
        'request_id': request_id,
        'model': model,
        'status': 'success' if success else 'failed',
        'duration': duration,
        'input_tokens': input_tokens,
        'output_tokens': output_tokens,
        'error': error,
        'params': req.get('params', {})
    }
    log_manager.write_request_log(log_entry)
    
    # Remove from active requests
    del realtime_stats.active_requests[request_id]

def log_error(request_id: str, error_type: str, error_message: str, stack_trace: str = ""):
    """Log error"""
    error_data = {
        'timestamp': time.time(),
        'request_id': request_id,
        'error_type': error_type,
        'error_message': error_message,
        'stack_trace': stack_trace
    }
    
    realtime_stats.recent_errors.append(error_data)
    
    # Update Prometheus metrics
    model = realtime_stats.active_requests.get(request_id, {}).get('model', 'unknown')
    error_count.labels(error_type=error_type, model=model).inc()
    
    # Write to error log file
    log_manager.write_error_log(error_data)

# --- Model Registry ---
MODEL_REGISTRY = {}  # Will be populated dynamically


def update_model_registry(models_data: dict) -> None:
    """Update the model registry with data from browser, inferring type from capabilities."""
    global MODEL_REGISTRY

    try:
        if not models_data or not isinstance(models_data, dict):
            logging.warning(f"Received empty or invalid model data: {models_data}")
            return

        new_registry = {}
        for public_name, model_info in models_data.items():
            if not isinstance(model_info, dict):
                continue

            # Determine type from outputCapabilities
            model_type = "chat"  # Default
            capabilities = model_info.get("capabilities", {})
            if isinstance(capabilities, dict):
                output_caps = capabilities.get("outputCapabilities", {})
                if isinstance(output_caps, dict):
                    if "image" in output_caps:
                        model_type = "image"
                    elif "video" in output_caps:
                        model_type = "video"

            # Store the processed model info with the determined type
            processed_info = model_info.copy()
            processed_info["type"] = model_type
            new_registry[public_name] = processed_info

        MODEL_REGISTRY = new_registry
        model_registry_gauge.set(len(MODEL_REGISTRY))
        logging.info(f"Updated and processed model registry with {len(MODEL_REGISTRY)} models.")

    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.error(f"Error updating model registry: {e}", exc_info=True)


def get_fallback_registry():
    """Fallback registry in case dynamic fetching fails."""
    return {
    "amazon-nova-experimental-chat-05-14": {"id": "d799a034-0ab6-48c1-817a-62e591143f39", "type": "chat"},
    "amazon.nova-pro-v1:0": {"id": "a14546b5-d78d-4cf6-bb61-ab5b8510a9d6", "type": "chat"},
    "api-gpt-4o-search": {"id": "14dbdb19-708f-4210-8e12-ce52b5c5296a", "type": "chat"},
    "chatgpt-4o-latest-20250326": {"id": "9513524d-882e-4350-b31e-e4584440c2c8", "type": "chat"},
    "claude-3-5-haiku-20241022": {"id": "f6fbf06c-532c-4c8a-89c7-f3ddcfb34bd1", "type": "chat"},
    "claude-3-5-sonnet-20241022": {"id": "f44e280a-7914-43ca-a25d-ecfcc5d48d09", "type": "chat"},
    "claude-3-7-sonnet-20250219": {"id": "c5a11495-081a-4dc6-8d9a-64a4fd6f7bbc", "type": "chat"},
    "claude-3-7-sonnet-20250219-thinking-32k": {"id": "be98fcfd-345c-4ae1-9a82-a19123ebf1d2", "type": "chat"},
    "claude-opus-4-1-20250805": {"id": "96ae95fd-b70d-49c3-91cc-b58c7da1090b", "type": "chat"},
    "claude-opus-4-1-20250805-thinking-16k": {"id": "f1a2eb6f-fc30-4806-9e00-1efd0d73cbc4", "type": "chat"},
    "claude-opus-4-1-search": {"id": "d942b564-191c-41c5-ae22-400a930a2cfe", "type": "chat"},
    "claude-opus-4-20250514": {"id": "ee116d12-64d6-48a8-88e5-b2d06325cdd2", "type": "chat"},
    "claude-opus-4-20250514-thinking-16k": {"id": "3b5e9593-3dc0-4492-a3da-19784c4bde75", "type": "chat"},
    "claude-opus-4-search": {"id": "25bcb878-749e-49f4-ac05-de84d964bcee", "type": "chat"},
    "claude-sonnet-4-20250514": {"id": "ac44dd10-0666-451c-b824-386ccfea7bcc", "type": "chat"},
    "claude-sonnet-4-20250514-thinking-32k": {"id": "4653dded-a46b-442a-a8fe-9bb9730e2453", "type": "chat"},
    "command-a-03-2025": {"id": "0f785ba1-efcb-472d-961e-69f7b251c7e3", "type": "chat"},
    "dall-e-3": {"id": "bb97bc68-131c-4ea4-a59e-03a6252de0d2", "type": "image"},
    "deepseek-r1-0528": {"id": "30ab90f5-e020-4f83-aff5-f750d2e78769", "type": "chat"},
    "deepseek-v3-0324": {"id": "2f5253e4-75be-473c-bcfc-baeb3df0f8ad", "type": "chat"},
    "flux-1-kontext-dev": {"id": "eb90ae46-a73a-4f27-be8b-40f090592c9a", "type": "image"},
    "flux-1-kontext-max": {"id": "0633b1ef-289f-49d4-a834-3d475a25e46b", "type": "image"},
    "flux-1-kontext-pro": {"id": "43390b9c-cf16-4e4e-a1be-3355bb5b6d5e", "type": "image"},
    "flux-1.1-pro": {"id": "9e8525b7-fe50-4e50-bf7f-ad1d3d205d3c", "type": "image"},
    "gemma-3-27b-it": {"id": "789e245f-eafe-4c72-b563-d135e93988fc", "type": "chat"},
    "gemma-3n-e4b-it": {"id": "896a3848-ae03-4651-963b-7d8f54b61ae8", "type": "chat"},
    "gemini-2.0-flash-001": {"id": "7a55108b-b997-4cff-a72f-5aa83beee918", "type": "chat"},
    "gemini-2.0-flash-preview-image-generation": {"id": "69bbf7d4-9f44-447e-a868-abc4f7a31810", "type": "image"},
    "gemini-2.5-flash": {"id": "ce2092c1-28d4-4d42-a1e0-6b061dfe0b20", "type": "chat"},
    "gemini-2.5-flash-lite-preview-06-17-thinking": {"id": "04ec9a17-c597-49df-acf0-963da275c246", "type": "chat"},
    "gemini-2.5-pro": {"id": "e2d9d353-6dbe-4414-bf87-bd289d523726", "type": "chat"},
    "gemini-2.5-pro-grounding": {"id": "b222be23-bd55-4b20-930b-a30cc84d3afd", "type": "chat"},
    "gemini-2.5-pro-grounding-exp": {"id": "51a47cc6-5ef9-4ac7-a59c-4009230d7564", "type": "chat"},
    "glm-4.5": {"id": "d079ef40-3b20-4c58-ab5e-243738dbada5", "type": "chat"},
    "glm-4.5-air": {"id": "7bfb254a-5d32-4ce2-b6dc-2c7faf1d5fe8", "type": "chat"},
    "glm-4.5v": {"id": "9dab0475-a0cc-4524-84a2-3fd25aa8c768", "type": "chat"},
    "gpt-4.1-2025-04-14": {"id": "14e9311c-94d2-40c2-8c54-273947e208b0", "type": "chat"},
    "gpt-4.1-mini-2025-04-14": {"id": "6a5437a7-c786-467b-b701-17b0bc8c8231", "type": "chat"},
    "gpt-5-chat": {"id": "4b11c78c-08c8-461c-938e-5fc97d56a40d", "type": "chat"},
    "gpt-5-high": {"id": "983bc566-b783-4d28-b24c-3c8b08eb1086", "type": "chat"},
    "gpt-5-mini-high": {"id": "5fd3caa8-fe4c-41a5-a22c-0025b58f4b42", "type": "chat"},
    "gpt-5-nano-high": {"id": "2dc249b3-98da-44b4-8d1e-6666346a8012", "type": "chat"},
    "gpt-5-search": {"id": "d14d9b23-1e46-4659-b157-a3804ba7e2ef", "type": "chat"},
    "gpt-image-1": {"id": "6e855f13-55d7-4127-8656-9168a9f4dcc0", "type": "image"},
    "gpt-oss-120b": {"id": "c6f8955f-8601-4eb4-b797-43506b49feca", "type": "chat"},
    "gpt-oss-20b": {"id": "ec3beb4b-7229-4232-bab9-670ee52dd711", "type": "chat"},
    "grok-3-mini-beta": {"id": "7699c8d4-0742-42f9-a117-d10e84688dab", "type": "chat"},
    "grok-3-mini-high": {"id": "149619f1-f1d5-45fd-a53e-7d790f156f20", "type": "chat"},
    "grok-3-preview-02-24": {"id": "bd2c8278-af7a-4ec3-84db-0a426c785564", "type": "chat"},
    "grok-4-0709": {"id": "b9edb8e9-4e98-49e7-8aaf-ae67e9797a11", "type": "chat"},
    "grok-4-search": {"id": "86d767b0-2574-4e47-a256-a22bcace9f56", "type": "chat"},
    "hunyuan-t1-20250711": {"id": "ba8c2392-4c47-42af-bfee-c6c057615a91", "type": "chat"},
    "hunyuan-turbos-20250416": {"id": "2e1af1cb-8443-4f3e-8d60-113992bfb491", "type": "chat"},
    "ideogram-v2": {"id": "34ee5a83-8d85-4d8b-b2c1-3b3413e9ed98", "type": "image"},
    "ideogram-v3-quality": {"id": "f7e2ed7a-f0b9-40ef-853a-20036e747232", "type": "image"},
    "imagen-3.0-generate-002": {"id": "51ad1d79-61e2-414c-99e3-faeb64bb6b1b", "type": "image"},
    "imagen-4.0-generate-preview-06-06": {"id": "2ec9f1a6-126f-4c65-a102-15ac401dcea4", "type": "image"},
    "imagen-4.0-ultra-generate-preview-06-06": {"id": "f8aec69d-e077-4ed1-99be-d34f48559bbf", "type": "image"},
    "kimi-k2-0711-preview": {"id": "7a3626fc-4e64-4c9e-821f-b449a4b43b6a", "type": "chat"},
    "llama-3.3-70b-instruct": {"id": "dcbd7897-5a37-4a34-93f1-76a24c7bb028", "type": "chat"},
    "llama-4-maverick-03-26-experimental": {"id": "49bd7403-c7fd-4d91-9829-90a91906ad6c", "type": "chat"},
    "llama-4-maverick-17b-128e-instruct": {"id": "b5ad3ab7-fc56-4ecd-8921-bd56b55c1159", "type": "chat"},
    "llama-4-scout-17b-16e-instruct": {"id": "c28823c1-40fd-4eaf-9825-e28f11d1f8b2", "type": "chat"},
    "magistral-medium-2506": {"id": "6337f479-2fc8-4311-a76b-8c957765cd68", "type": "chat"},
    "minimax-m1": {"id": "87e8d160-049e-4b4e-adc4-7f2511348539", "type": "chat"},
    "mistral-medium-2505": {"id": "27b9f8c6-3ee1-464a-9479-a8b3c2a48fd4", "type": "chat"},
    "mistral-small-2506": {"id": "bbad1d17-6aa5-4321-949c-d11fb6289241", "type": "chat"},
    "mistral-small-3.1-24b-instruct-2503": {"id": "69f5d38a-45f5-4d3a-9320-b866a4035ed9", "type": "chat"},
    "nano-banana": {"id": "e4e58f18-c04f-47cd-8d11-4b2ece7b617e", "type": "image"},
    "o3-2025-04-16": {"id": "cb0f1e24-e8e9-4745-aabc-b926ffde7475", "type": "chat"},
    "o3-mini": {"id": "c680645e-efac-4a81-b0af-da16902b2541", "type": "chat"},
    "o3-search": {"id": "fbe08e9a-3805-4f9f-a085-7bc38e4b51d1", "type": "chat"},
    "o4-mini-2025-04-16": {"id": "f1102bbf-34ca-468f-a9fc-14bcf63f315b", "type": "chat"},
    "photon": {"id": "17e31227-36d7-4a7a-943a-7ebffa3a00eb", "type": "image"},
    "ppl-sonar-pro-high": {"id": "c8711485-d061-4a00-94d2-26c31b840a3d", "type": "chat"},
    "ppl-sonar-reasoning-pro-high": {"id": "24145149-86c9-4690-b7c9-79c7db216e5c", "type": "chat"},
    "qwen-image": {"id": "231e684e-55e7-4bfb-b124-767ed19ba1aa", "type": "image"},
    "qwen3-235b-a22b": {"id": "2595a594-fa54-4299-97cd-2d7380d21c80", "type": "chat"},
    "qwen3-235b-a22b-instruct-2507": {"id": "ee7cb86e-8601-4585-b1d0-7c7380f8f6f4", "type": "chat"},
    "qwen3-235b-a22b-no-thinking": {"id": "1a400d9a-f61c-4bc2-89b4-a9b7e77dff12", "type": "chat"},
    "qwen3-235b-a22b-thinking-2507": {"id": "16b8e53a-cc7b-4608-a29a-20d4dac77cf2", "type": "chat"},
    "qwen3-30b-a3b": {"id": "9a066f6a-7205-4325-8d0b-d81cc4b049c0", "type": "chat"},
    "qwen3-30b-a3b-instruct-2507": {"id": "a8d1d310-e485-4c50-8f27-4bff18292a99", "type": "chat"},
    "qwen3-coder-480b-a35b-instruct": {"id": "af033cbd-ec6c-42cc-9afa-e227fc12efe8", "type": "chat"},
    "qwq-32b": {"id": "885976d3-d178-48f5-a3f4-6e13e0718872", "type": "chat"},
    "recraft-v3": {"id": "b70ab012-18e7-4d6f-a887-574e05de6c20", "type": "image"},
    "seedream-3": {"id": "0dde746c-3dbc-42be-b8f5-f38bd1595baa", "type": "image"},
    "step-3": {"id": "1ea13a81-93a7-4804-bcdd-693cd72e302d", "type": "chat"},
    "toad": {"id": "7575d626-1b40-4bcd-846b-f125928d64fa", "type": "chat"}
}


# --- Global State ---
browser_ws: WebSocket | None = None
response_channels: dict[str, asyncio.Queue] = {}  # Keep for backward compatibility
request_manager = PersistentRequestManager()
background_tasks: Set[asyncio.Task] = set()
SHUTTING_DOWN = False
monitor_clients: Set[WebSocket] = set()  # Monitor client connections
startup_time = time.time()  # Server startup time

# --- Helper Functions for Monitoring ---
async def broadcast_to_monitors(data: dict):
    """Broadcast data to all monitor clients"""
    if not monitor_clients:
        return
        
    disconnected = []
    for client in monitor_clients:
        try:
            await client.send_json(data)
        except:
            disconnected.append(client)
    
    # Clean up disconnected clients
    for client in disconnected:
        monitor_clients.discard(client)

# --- FastAPI App and Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global MODEL_REGISTRY, request_manager, startup_time
    logging.info(f"Server is starting...")
    startup_time = time.time()

    # Show access addresses
    local_ip = get_local_ip()
    logging.info(f"🌐 Server access URLs:")
    logging.info(f"  - Local: http://localhost:{Config.PORT}")
    logging.info(f"  - Network: http://{local_ip}:{Config.PORT}")
    logging.info(f"📱 Use the Network URL to access from your phone on the same WiFi")

    # === Add detailed endpoint descriptions ===
    logging.info(f"\n📋 Available Endpoints:")
    logging.info(f"  🖥️  Monitor Dashboard: http://{local_ip}:{Config.PORT}/monitor")
    logging.info(f"     Real-time monitoring dashboard for system status, request logs, and performance metrics")

    logging.info(f"\n  📊 Metrics & Health:")
    logging.info(f"     - Prometheus Metrics: http://{local_ip}:{Config.PORT}/metrics")
    logging.info(f"       Prometheus format performance metrics, can be integrated with Grafana")
    logging.info(f"     - Health Check: http://{local_ip}:{Config.PORT}/health")
    logging.info(f"       Basic health check")
    logging.info(f"     - Detailed Health: http://{local_ip}:{Config.PORT}/api/health/detailed")
    logging.info(f"       Detailed health status with score and suggestions")

    logging.info(f"\n  🤖 AI API:")
    logging.info(f"     - Chat Completions: POST http://{local_ip}:{Config.PORT}/v1/chat/completions")
    logging.info(f"       OpenAI-compatible chat API")
    logging.info(f"     - List Models: GET http://{local_ip}:{Config.PORT}/v1/models")
    logging.info(f"       Get available model list")
    logging.info(f"     - Refresh Models: POST http://{local_ip}:{Config.PORT}/v1/refresh-models")
    logging.info(f"       Refresh model list")

    logging.info(f"\n  📈 Statistics:")
    logging.info(f"     - Stats Summary: http://{local_ip}:{Config.PORT}/api/stats/summary")
    logging.info(f"       24-hour statistics summary")
    logging.info(f"     - Request Logs: http://{local_ip}:{Config.PORT}/api/logs/requests")
    logging.info(f"       Request logs API")
    logging.info(f"     - Error Logs: http://{local_ip}:{Config.PORT}/api/logs/errors")
    logging.info(f"       Error logs API")
    logging.info(f"     - Alerts: http://{local_ip}:{Config.PORT}/api/alerts")
    logging.info(f"       System alert history")

    logging.info(f"\n  🛠️  OpenAI Client Config:")
    logging.info(f"     base_url='http://{local_ip}:{Config.PORT}/v1'")
    logging.info(f"     api_key='sk-any-string-you-like'")
    logging.info(f"\n{'=' * 60}\n")
    # === End add ===

    # Use fallback registry on startup - models will be updated by browser script
    MODEL_REGISTRY = get_fallback_registry()
    logging.info(f"Loaded {len(MODEL_REGISTRY)} fallback models")

    # Start cleanup task
    cleanup_task = asyncio.create_task(periodic_cleanup())
    background_tasks.add(cleanup_task)
    # Start health check task
    health_check_task = asyncio.create_task(monitoring_alerts.check_system_health())
    background_tasks.add(health_check_task)

    logging.info("Server startup complete")

    try:
        yield
    finally:
        global SHUTTING_DOWN
        SHUTTING_DOWN = True
        logging.info(f"Lifespan: Server is shutting down. Cancelling {len(background_tasks)} background tasks...")

        # Cancel all background tasks
        cancelled_tasks = []
        for task in list(background_tasks):
            if not task.done():
                logging.info(f"Lifespan: Cancelling task: {task}")
                task.cancel()
                cancelled_tasks.append(task)

        # Wait for cancelled tasks to finish
        if cancelled_tasks:
            logging.info(f"Lifespan: Waiting for {len(cancelled_tasks)} cancelled tasks to finish...")
            results = await asyncio.gather(*cancelled_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.info(f"Lifespan: Task {i} finished with result: {type(result).__name__}")
                else:
                    logging.info(f"Lifespan: Task {i} finished normally")

        logging.info("Lifespan: All background tasks cancelled. Shutdown complete.")


app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Use * for development, specify domain for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- WebSocket Handler (Producer) ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global browser_ws, request_manager
    await websocket.accept()
    logging.info("✅ Browser WebSocket connected")
    browser_ws = websocket
    websocket_status.set(1)
    # Reset disconnect time
    monitoring_alerts.last_disconnect_time = 0

    # Start heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat.start_heartbeat(websocket))
    background_tasks.add(heartbeat_task)

    # Handle reconnection - check for pending requests
    pending_requests = request_manager.get_pending_requests()
    if pending_requests:
        logging.info(f"🔄 Browser reconnected, {len(pending_requests)} pending requests found")

        # Send reconnection acknowledgment with pending request IDs
        await websocket.send_text(json.dumps({
            "type": "reconnection_ack",
            "pending_request_ids": list(pending_requests.keys()),
            "message": f"Reconnected. Found {len(pending_requests)} pending requests."
        }))

    try:
        while True:
            message_str = await websocket.receive_text()
            message = json.loads(message_str)
            # Handle pong response
            if message.get("type") == "pong":
                heartbeat.handle_pong()
                continue


            # Handle reconnection handshake from browser
            if message.get("type") == "reconnection_handshake":
                browser_pending_ids = message.get("pending_request_ids", [])
                logging.info(f"🤝 Received reconnection handshake, browser has {len(browser_pending_ids)} pending requests")

                # Restore request channels for matching requests
                restored_count = 0
                for request_id in browser_pending_ids:
                    persistent_req = request_manager.get_request(request_id)
                    if persistent_req:
                        # Restore the response channel
                        response_channels[request_id] = persistent_req.response_queue
                        request_manager.update_status(request_id, RequestStatus.PROCESSING)
                        restored_count += 1
                        logging.info(f"🔄 Restored request channel: {request_id}")

                # Send restoration acknowledgment
                await websocket.send_text(json.dumps({
                    "type": "restoration_ack",
                    "restored_count": restored_count,
                    "message": f"Restored {restored_count} request channels"
                }))
                continue

            # Handle model registry updates
            if message.get("type") == "model_registry":
                models_data = message.get("models", {})
                update_model_registry(models_data)

                # Send acknowledgment
                await websocket.send_text(json.dumps({
                    "type": "model_registry_ack",
                    "count": len(MODEL_REGISTRY)
                }))
                continue

            # Handle regular chat requests
            request_id = message.get("request_id")
            data = message.get("data")
            logging.debug(f"⬅️ Browser [ID: {request_id}]: Received data: {data}")

            # Update request status to processing when we receive data
            if request_id:
                request_manager.update_status(request_id, RequestStatus.PROCESSING)

            # Handle both old and new request tracking systems
            if request_id in response_channels:
                queue = response_channels[request_id]
                logging.debug(f"Browser [ID: {request_id}]: Queue size before put: {queue.qsize()}")
                await queue.put(data)
                logging.debug(f"Browser [ID: {request_id}]: Data put into queue. New size: {queue.qsize()}")

                # Check if this is the end of the request
                if data == "[DONE]":
                    request_manager.complete_request(request_id)

            else:
                # Check if this is a persistent request
                persistent_req = request_manager.get_request(request_id)
                if persistent_req:
                    logging.info(f"🔄 Restoring persistent request queue: {request_id}")
                    response_channels[request_id] = persistent_req.response_queue
                    await persistent_req.response_queue.put(data)
                    request_manager.update_status(request_id, RequestStatus.PROCESSING)

                    if data == "[DONE]":
                        request_manager.complete_request(request_id)
                else:
                    logging.warning(f"⚠️ Browser: Received unknown/closed request message: {request_id}")

    except WebSocketDisconnect:
        logging.warning("❌ Browser client disconnected")
    finally:
        browser_ws = None
        websocket_status.set(0)

        # Handle browser disconnect - keep persistent requests alive
        await request_manager.handle_browser_disconnect()

        # Only send errors to non-persistent requests
        for request_id, queue in response_channels.items():
            persistent_req = request_manager.get_request(request_id)
            if not persistent_req:  # Only error out non-persistent requests
                try:
                    await queue.put({"error": "Browser disconnected"})
                except KeyboardInterrupt:
                    raise
                except:
                    pass

        response_channels.clear()
        logging.info("WebSocket cleaned up. Persistent requests kept alive.")

# --- Monitor WebSocket ---
@app.websocket("/ws/monitor")
async def monitor_websocket(websocket: WebSocket):
    """WebSocket connection for monitor dashboard"""
    await websocket.accept()
    monitor_clients.add(websocket)
    
    try:
        # Send initial data
        await websocket.send_json({
            "type": "initial_data",
            "active_requests": dict(realtime_stats.active_requests),
            "recent_requests": list(realtime_stats.recent_requests),
            "recent_errors": list(realtime_stats.recent_errors),
            "model_usage": dict(realtime_stats.model_usage)
        })
        
        while True:
            # Keep connection alive
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        monitor_clients.remove(websocket)

# --- API Handler ---
@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    global request_manager

    if not browser_ws:
        raise HTTPException(status_code=503, detail="Browser client not connected.")

    openai_req = await request.json()
    request_id = str(uuid.uuid4())
    is_streaming = openai_req.get("stream", True)
    model_name = openai_req.get("model")

    model_info = MODEL_REGISTRY.get(model_name)
    if not model_info:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    model_type = model_info.get("type", "chat")

    # Add request start log
    request_params = {
        "temperature": openai_req.get("temperature"),
        "top_p": openai_req.get("top_p"),
        "max_tokens": openai_req.get("max_tokens"),
        "streaming": is_streaming
    }
    messages = openai_req.get("messages", [])
    log_request_start(request_id, model_name, request_params, messages)
    
    # Broadcast to monitor clients
    await broadcast_to_monitors({
        "type": "request_start",
        "request_id": request_id,
        "model": model_name,
        "timestamp": time.time()
    })

    # Create response queue and add to both systems for compatibility
    response_queue = asyncio.Queue(maxsize=Config.BACKPRESSURE_QUEUE_SIZE)
    response_channels[request_id] = response_queue

    # Add to persistent request manager
    try:
        persistent_req = await request_manager.add_request(
            request_id=request_id,
            openai_request=openai_req,
            response_queue=response_queue,
            model_name=model_name,
            is_streaming=is_streaming
        )
    except HTTPException:
        # Concurrent limit
        log_request_end(request_id, False, 0, 0, "Too many concurrent requests")
        raise

    logging.info(f"API [ID: {request_id}]: Created persistent request for model type '{model_type}'.")

    try:
        task = asyncio.create_task(send_to_browser_task(request_id, openai_req))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

        media_type = "text/event-stream" if is_streaming else "application/json"
        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked"
        } if is_streaming else {}

        logging.info(f"API [ID: {request_id}]: Returning {media_type} response to client.")

        if is_streaming:
            # Use custom streaming response for immediate flush
            return ImmediateStreamingResponse(
                stream_generator(request_id, model_name, is_streaming=is_streaming, model_type=model_type),
                media_type=media_type,
                headers=headers
            )
        else:
            # Use regular response for non-streaming
            return StreamingResponse(
                stream_generator(request_id, model_name, is_streaming=is_streaming, model_type=model_type),
                media_type=media_type,
                headers=headers
            )
    except KeyboardInterrupt:
        # Clean up on keyboard interrupt
        if request_id in response_channels:
            del response_channels[request_id]
        request_manager.complete_request(request_id)
        raise
    except Exception as e:
        # Clean up both tracking systems
        if request_id in response_channels:
            del response_channels[request_id]
        request_manager.complete_request(request_id)
        logging.error(f"API [ID: {request_id}]: Exception: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def send_to_browser_task(request_id: str, openai_req: dict):
    """This task runs in the background, sending the request to the browser."""
    global request_manager

    if not browser_ws:
        logging.error(f"TASK [ID: {request_id}]: Cannot send, browser disconnected.")
        # Mark request as error if browser is not connected
        persistent_req = request_manager.get_request(request_id)
        if persistent_req:
            await persistent_req.response_queue.put({"error": "Browser not connected"})
        return

    try:
        lmarena_payload, files_to_upload = create_lmarena_request_body(openai_req)

        message_to_browser = {
            "request_id": request_id,
            "payload": lmarena_payload,
            "files_to_upload": files_to_upload
        }

        logging.info(f"TASK [ID: {request_id}]: Sending payload and {len(files_to_upload)} file(s) to browser.")
        await browser_ws.send_text(json.dumps(message_to_browser))

        # Mark as sent to browser in persistent request manager
        request_manager.mark_sent_to_browser(request_id)
        logging.info(f"TASK [ID: {request_id}]: Payload sent and marked as sent to browser.")

    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.error(f"Error creating or sending request body: {e}", exc_info=True)

        # Send error to both tracking systems
        if request_id in response_channels:
            await response_channels[request_id].put({"error": f"Failed to process request: {e}"})

        persistent_req = request_manager.get_request(request_id)
        if persistent_req:
            await persistent_req.response_queue.put({"error": f"Failed to process request: {e}"})
            request_manager.update_status(request_id, RequestStatus.ERROR)


# Simple token estimation function
def estimateTokens(text: str) -> int:
    """Simple token estimation function"""
    if not text:
        return 0
    # Rough estimate: average 4 characters per token
    return len(str(text)) // 4


# --- Stream Consumer ---
async def stream_generator(request_id: str, model: str, is_streaming: bool, model_type: str):
    global request_manager, browser_ws
    start_time = time.time()  # Add start time recording

    # Get queue from either response_channels or persistent request
    queue = response_channels.get(request_id)
    persistent_req = request_manager.get_request(request_id)

    if not queue and persistent_req:
        queue = persistent_req.response_queue
        # Restore to response_channels for compatibility
        response_channels[request_id] = queue

    if not queue:
        logging.error(f"STREAMER [ID: {request_id}]: Queue not found!")
        return

    logging.info(f"STREAMER [ID: {request_id}]: Generator started for model type '{model_type}'.")
    await asyncio.sleep(0)

    response_id = f"chatcmpl-{uuid.uuid4()}"

    try:
        accumulated_content = ""
        media_urls = []
        finish_reason = None

        # Buffer for streaming chunks (minimum 50 chars)
        streaming_buffer = ""
        MIN_CHUNK_SIZE = 40
        last_chunk_time = time.time()
        MAX_BUFFER_TIME = 0.5  # Max 500ms before forcing flush

        while True:
            # Try to get data with a timeout to check buffer periodically
            try:
                raw_data = await asyncio.wait_for(queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                # No new data, but check if we should flush buffer
                if is_streaming and model_type == "chat" and streaming_buffer:
                    current_time = time.time()
                    if current_time - last_chunk_time >= MAX_BUFFER_TIME:
                        chunk = {
                            "id": response_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": model,
                            "choices": [{
                                "index": 0,
                                "delta": {
                                    "role": "assistant",
                                    "content": streaming_buffer
                                },
                                "finish_reason": None
                            }],
                            "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
                        }
                        chunk_data = f"data: {json.dumps(chunk)}\n\n"
                        yield chunk_data
                        streaming_buffer = ""
                        last_chunk_time = current_time
                continue

            if raw_data == "[DONE]":
                break

            # Handle error dictionary from timeout or browser disconnect
            if isinstance(raw_data, dict) and "error" in raw_data:
                logging.error(f"STREAMER [ID: {request_id}]: Received error: {raw_data}")

                # Format error for OpenAI response
                openai_error = {
                    "error": {
                        "message": str(raw_data.get("error", "Unknown error")),
                        "type": "server_error",
                        "code": None
                    }
                }

                if is_streaming:
                    yield f"data: {json.dumps(openai_error)}\n\ndata: [DONE]\n\n"
                else:
                    yield json.dumps(openai_error)
                return

            # First, try to detect if this is a JSON error response from the server
            if isinstance(raw_data, str) and raw_data.strip().startswith('{'):
                try:
                    error_data = json.loads(raw_data.strip())
                    if "error" in error_data:
                        logging.error(f"STREAMER [ID: {request_id}]: Server returned error: {error_data}")

                        # Parse the actual error structure from the server
                        server_error = error_data["error"]

                        # If the server error is already in OpenAI format, use it directly
                        if isinstance(server_error, dict) and "message" in server_error:
                            openai_error = {"error": server_error}
                        else:
                            # If it's just a string, wrap it in OpenAI format
                            openai_error = {
                                "error": {
                                    "message": str(server_error),
                                    "type": "server_error",
                                    "code": None
                                }
                            }

                        if is_streaming:
                            yield f"data: {json.dumps(openai_error)}\n\ndata: [DONE]\n\n"
                        else:
                            yield json.dumps(openai_error)
                        return
                except json.JSONDecodeError:
                    pass  # Not a JSON error, continue with normal parsing

            # Skip processing if raw_data is not a string (e.g., error dict)
            if not isinstance(raw_data, str):
                logging.warning(f"STREAMER [ID: {request_id}]: Skipping non-string data: {type(raw_data)}")
                continue

            try:
                prefix, content = raw_data.split(":", 1)

                if model_type in ["image", "video"] and prefix == "a2":
                    media_data_list = json.loads(content)
                    for item in media_data_list:
                        url = item.get("image") if model_type == "image" else item.get("url")
                        if url:
                            logging.info(f"MEDIA [ID: {request_id}]: Found {model_type} URL: {url}")
                            media_urls.append(url)

                elif model_type == "chat" and prefix == "a0":
                    delta = json.loads(content)
                    if is_streaming:
                        # Add to buffer instead of sending immediately
                        streaming_buffer += delta

                        # Check if we should send: either buffer is full or timeout reached
                        current_time = time.time()
                        time_since_last = current_time - last_chunk_time

                        if len(streaming_buffer) >= MIN_CHUNK_SIZE or (
                                streaming_buffer and time_since_last >= MAX_BUFFER_TIME):
                            chunk = {
                                "id": response_id,
                                "object": "chat.completion.chunk",
                                "created": int(time.time()),
                                "model": model,
                                "choices": [{
                                    "index": 0,
                                    "delta": {
                                        "role": "assistant",
                                        "content": streaming_buffer
                                    },
                                    "finish_reason": None
                                }],
                                "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
                            }
                            chunk_data = f"data: {json.dumps(chunk)}\n\n"
                            yield chunk_data
                            
                            # Accumulate content for request details
                            accumulated_content += streaming_buffer
                            
                            # Clear buffer and update time after sending
                            streaming_buffer = ""
                            last_chunk_time = current_time
                    else:
                        accumulated_content += delta

                elif prefix == "ad":
                    finish_data = json.loads(content)
                    finish_reason = finish_data.get("finishReason", "stop")

            except (ValueError, json.JSONDecodeError):
                logging.warning(f"STREAMER [ID: {request_id}]: Could not parse data: {raw_data}")
                continue

            # Yield control to event loop after processing
            if is_streaming and model_type == "chat":
                await asyncio.sleep(0.001)  # Small delay to help with network flush
            else:
                await asyncio.sleep(0)

        # --- Final Response Generation ---

        # Flush any remaining buffer content for streaming chat
        if is_streaming and model_type == "chat" and streaming_buffer:
            chunk = {
                "id": response_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": streaming_buffer
                    },
                    "finish_reason": None
                }],
                "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
            }
            yield f"data: {json.dumps(chunk)}\n\n"
            accumulated_content += streaming_buffer
            streaming_buffer = ""

        if model_type in ["image", "video"]:
            logging.info(f"MEDIA [ID: {request_id}]: Found {len(media_urls)} media file(s). Returning URLs directly.")
            # Format the URLs based on their type
            if model_type == "video":
                accumulated_content = "\n".join(media_urls)  # Return raw URLs for videos
            else:  # Default to image handling
                accumulated_content = "\n".join([f"![Generated Image]({url})" for url in media_urls])

        if is_streaming:
            if model_type in ["image", "video"]:
                chunk = {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": accumulated_content
                        },
                        "finish_reason": finish_reason or "stop"
                    }],
                    "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
                }
                yield f"data: {json.dumps(chunk)}\n\n"

            # Send final chunk with finish_reason for chat models
            if model_type == "chat":
                final_chunk = {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": finish_reason or "stop"
                    }],
                    "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
                }
                yield f"data: {json.dumps(final_chunk)}\n\n"

            # Send [DONE] immediately
            yield "data: [DONE]\n\n"
        else:
            # For non-streaming, send the complete JSON object with the URL content
            complete_response = {
                "id": response_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": accumulated_content
                    },
                    "finish_reason": finish_reason or "stop"
                }],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                },
                "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}"
            }
            yield json.dumps(complete_response)

        # Record successful request
        input_tokens = estimateTokens(str(persistent_req.openai_request if persistent_req else {}))
        output_tokens = estimateTokens(accumulated_content)
        log_request_end(request_id, True, input_tokens, output_tokens, response_content=accumulated_content)
        
        # Broadcast to monitor clients
        await broadcast_to_monitors({
            "type": "request_end",
            "request_id": request_id,
            "success": True,
            "duration": time.time() - start_time
        })

    except asyncio.CancelledError:
        logging.warning(f"GENERATOR [ID: {request_id}]: Client disconnected.")

        # Send abort message to browser if WebSocket is connected
        if browser_ws:
            try:
                await browser_ws.send_text(json.dumps({
                    "type": "abort_request",
                    "request_id": request_id
                }))
                logging.info(f"GENERATOR [ID: {request_id}]: Sent abort message to browser")
            except Exception as e:
                logging.error(f"GENERATOR [ID: {request_id}]: Failed to send abort message: {e}")

        # Re-raise to properly handle the cancellation
        raise

    except KeyboardInterrupt:
        logging.info(f"GENERATOR [ID: {request_id}]: Keyboard interrupt received, cleaning up...")
        raise
    except Exception as e:
        logging.error(f"GENERATOR [ID: {request_id}]: Error: {e}", exc_info=True)
        
        # Record error
        log_error(request_id, type(e).__name__, str(e), traceback.format_exc())
        log_request_end(request_id, False, 0, 0, str(e))
        
        # Broadcast error to monitor clients
        await broadcast_to_monitors({
            "type": "request_error",
            "request_id": request_id,
            "error": str(e),
            "timestamp": time.time()
        })
        
    finally:
        # Clean up both tracking systems
        if request_id in response_channels:
            del response_channels[request_id]
            logging.info(f"GENERATOR [ID: {request_id}]: Cleaned up response channel.")

        # Mark request as completed in persistent manager
        request_manager.complete_request(request_id)


def create_lmarena_request_body(openai_req: dict) -> (dict, list):
    model_name = openai_req["model"]

    if model_name not in MODEL_REGISTRY:
        raise ValueError(f"Model '{model_name}' not found in registry. Available models: {list(MODEL_REGISTRY.keys())}")

    model_info = MODEL_REGISTRY[model_name]
    model_id = model_info.get("id", model_name)
    modality = model_info.get("type", "chat")
    evaluation_id = str(uuid.uuid4())

    files_to_upload = []
    processed_messages = []

    # Process messages to extract files and clean content
    for msg in openai_req['messages']:
        content = msg.get("content", "")
        new_msg = msg.copy()

        if isinstance(content, list):
            # Handle official multimodal content array
            text_parts = []
            for part in content:
                if part.get("type") == "text":
                    text_parts.append(part.get("text", ""))
                elif part.get("type") == "image_url":
                    image_url = part.get("image_url", {}).get("url", "")
                    match = re.match(r"data:(image/\w+);base64,(.*)", image_url)
                    if match:
                        mime_type, base64_data = match.groups()
                        file_ext = mime_type.split('/')
                        filename = f"upload-{uuid.uuid4()}.{file_ext}"
                        files_to_upload.append({"fileName": filename, "contentType": mime_type, "data": base64_data})
            new_msg["content"] = "\n".join(text_parts)
            processed_messages.append(new_msg)

        elif isinstance(content, str):
            # Handle simple string content that might contain data URLs
            text_content = content
            matches = re.findall(r"data:(image/\w+);base64,([a-zA-Z0-9+/=]+)", content)
            if matches:
                logging.info(f"Found {len(matches)} data URL(s) in string content.")
                for mime_type, base64_data in matches:
                    file_ext = mime_type.split('/')
                    filename = f"upload-{uuid.uuid4()}.{file_ext}"
                    files_to_upload.append({"fileName": filename, "contentType": mime_type, "data": base64_data})
                # Remove all found data URLs from the text to be sent
                text_content = re.sub(r"data:image/\w+;base64,[a-zA-Z0-9+/=]+", "", text_content).strip()

            new_msg["content"] = text_content
            processed_messages.append(new_msg)

        else:
            # If content is not a list or string, just pass it through
            processed_messages.append(msg)

    # Find the last user message index
    last_user_message_index = -1
    for i in range(len(processed_messages) - 1, -1, -1):
        if processed_messages[i].get("role") == "user":
            last_user_message_index = i
            break

    # Insert empty user message after the last user message (only for chat models)
    if modality == "chat" and last_user_message_index != -1:
        # Insert empty user message after the last user message
        insert_index = last_user_message_index + 1
        empty_user_message = {"role": "user", "content": " "}
        processed_messages.insert(insert_index, empty_user_message)
        logging.info(
            f"Added empty user message after last user message at index {last_user_message_index} for chat model")

    # Build Arena-formatted messages
    arena_messages = []
    message_ids = [str(uuid.uuid4()) for _ in processed_messages]
    for i, msg in enumerate(processed_messages):
        parent_message_ids = [message_ids[i - 1]] if i > 0 else []

        original_role = msg.get("role")
        role = "user" if original_role not in ["user", "assistant", "data"] else original_role

        arena_messages.append({
            "id": message_ids[i], "role": role, "content": msg['content'],
            "experimental_attachments": [], "parentMessageIds": parent_message_ids,
            "participantPosition": "a", "modelId": model_id if role == 'assistant' else None,
            "evaluationSessionId": evaluation_id, "status": "pending", "failureReason": None,
        })

    user_message_id = message_ids[-1] if message_ids else str(uuid.uuid4())
    model_a_message_id = str(uuid.uuid4())
    arena_messages.append({
        "id": model_a_message_id, "role": "assistant", "content": "",
        "experimental_attachments": [], "parentMessageIds": [user_message_id],
        "participantPosition": "a", "modelId": model_id,
        "evaluationSessionId": evaluation_id, "status": "pending", "failureReason": None,
    })

    payload = {
        "id": evaluation_id, "mode": "direct", "modelAId": model_id,
        "userMessageId": user_message_id, "modelAMessageId": model_a_message_id,
        "messages": arena_messages, "modality": modality,
    }

    return payload, files_to_upload


@app.get("/v1/models")
async def get_models():
    """Lists all available models in an OpenAI-compatible format."""
    return {
        "object": "list",
        "data": [
            {
                "id": model_name,
                "object": "model",
                "created": int(asyncio.get_event_loop().time()),
                "owned_by": "lmarena",
                "type": model_info.get("type", "chat")
            }
            for model_name, model_info in MODEL_REGISTRY.items()
        ],
    }


@app.post("/v1/refresh-models")
async def refresh_models():
    """Request model registry refresh from browser script."""
    if browser_ws:
        try:
            # Send refresh request to browser
            await browser_ws.send_text(json.dumps({
                "type": "refresh_models"
            }))

            return {
                "success": True,
                "message": "Model refresh request sent to browser",
                "models": list(MODEL_REGISTRY.keys())
            }
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.error(f"Failed to send refresh request: {e}")
            return {
                "success": False,
                "message": "Failed to send refresh request to browser",
                "models": list(MODEL_REGISTRY.keys())
            }
    else:
        return {
            "success": False,
            "message": "No browser connection available",
            "models": list(MODEL_REGISTRY.keys())
        }

# --- Prometheus Metrics Endpoint ---
@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# --- Monitoring-related API Endpoints ---
@app.get("/api/stats/summary")
async def get_stats_summary():
    """Get statistics summary"""
    # Read recent logs
    recent_logs = log_manager.read_request_logs(limit=10000)  # Read recent logs
    
    # Calculate statistics for the last 24 hours
    current_time = time.time()
    day_ago = current_time - 86400
    
    recent_24h_logs = [log for log in recent_logs if log.get('timestamp', 0) > day_ago]
    
    total_requests = len(recent_24h_logs)
    successful = sum(1 for log in recent_24h_logs if log.get('status') == 'success')
    failed = total_requests - successful
    
    total_input_tokens = sum(log.get('input_tokens', 0) for log in recent_24h_logs)
    total_output_tokens = sum(log.get('output_tokens', 0) for log in recent_24h_logs)
    
    durations = [log.get('duration', 0) for log in recent_24h_logs if log.get('duration', 0) > 0]
    avg_duration = sum(durations) / len(durations) if durations else 0
    
    # Get performance statistics
    perf_stats = performance_monitor.get_stats()
    model_perf = performance_monitor.get_model_stats()
    
    # Build model statistics
    model_stats = []
    for model_name, usage in realtime_stats.model_usage.items():
        perf = model_perf.get(model_name, {})
        model_stats.append({
            "model": model_name,
            "total_requests": usage['requests'],
            "successful_requests": usage['requests'] - usage['errors'],
            "failed_requests": usage['errors'],
            "total_input_tokens": usage.get('tokens', 0) // 2,  # Rough estimate
            "total_output_tokens": usage.get('tokens', 0) // 2,
            "avg_duration": perf.get('avg_response_time', 0),
            "qps": perf.get('qps', 0),
            "error_rate": perf.get('error_rate', 0)
        })
    
    return {
        "summary": {
            "total_requests": total_requests,
            "successful": successful,
            "failed": failed,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "avg_duration": avg_duration,
            "success_rate": (successful / total_requests * 100) if total_requests > 0 else 0
        },
        "performance": perf_stats,
        "model_stats": sorted(model_stats, key=lambda x: x['total_requests'], reverse=True),
        "active_requests": len(realtime_stats.active_requests),
        "browser_connected": browser_ws is not None,
        "monitor_clients": len(monitor_clients),
        "uptime": time.time() - startup_time
    }

@app.get("/api/logs/requests")
async def get_request_logs(limit: int = 100, offset: int = 0, model: str = None):
    """Get request logs"""
    logs = log_manager.read_request_logs(limit, offset, model)
    return logs

@app.get("/api/logs/errors")
async def get_error_logs(limit: int = 50):
    """Get error logs"""
    logs = log_manager.read_error_logs(limit)
    return logs

@app.get("/api/logs/download")
async def download_logs(log_type: str = "requests"):
    """Download log files"""
    if log_type == "requests":
        file_path = log_manager.request_log_path
        filename = "requests.jsonl"
    elif log_type == "errors":
        file_path = log_manager.error_log_path
        filename = "errors.jsonl"
    else:
        raise HTTPException(status_code=400, detail="Invalid log type")
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Log file not found")
    
    return StreamingResponse(
        open(file_path, 'rb'),
        media_type="application/x-jsonlines",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/api/request/{request_id}")
async def get_request_details(request_id: str):
    """Get request details"""
    details = request_details_storage.get(request_id)
    if not details:
        raise HTTPException(status_code=404, detail="Request details not found")
    
    return {
        "request_id": details.request_id,
        "timestamp": details.timestamp,
        "model": details.model,
        "status": details.status,
        "duration": details.duration,
        "input_tokens": details.input_tokens,
        "output_tokens": details.output_tokens,
        "error": details.error,
        "request_params": details.request_params,
        "request_messages": details.request_messages,
        "response_content": details.response_content,
        "headers": details.headers
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "browser_connected": browser_ws is not None,
        "active_requests": len(request_manager.active_requests),
        "uptime": time.time() - startup_time,
        "models_loaded": len(MODEL_REGISTRY),
        "monitor_clients": len(monitor_clients),
        "log_files": {
            "requests": str(log_manager.request_log_path),
            "errors": str(log_manager.error_log_path)
        }
    }


# --- New Alert-related API ---
@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    """Get recent alerts"""
    return list(monitoring_alerts.alert_history)[-limit:]


# --- Configuration Management API ---
@app.get("/api/config")
async def get_config():
    """Get current configuration"""
    return config_manager.dynamic_config


@app.post("/api/config")
async def update_config(request: Request):
    """Update configuration"""
    try:
        config_data = await request.json()

        # Update configuration
        config_manager._deep_merge(config_manager.dynamic_config, config_data)
        config_manager.save_config()

        # Apply immediate changes for some configurations
        if 'request' in config_data:
            if 'timeout_seconds' in config_data['request']:
                Config.REQUEST_TIMEOUT_SECONDS = config_data['request']['timeout_seconds']
            if 'max_concurrent_requests' in config_data['request']:
                Config.MAX_CONCURRENT_REQUESTS = config_data['request']['max_concurrent_requests']

        if 'monitoring' in config_data:
            if 'error_rate_threshold' in config_data['monitoring']:
                monitoring_alerts.alert_thresholds["error_rate"] = config_data['monitoring']['error_rate_threshold']
            if 'response_time_threshold' in config_data['monitoring']:
                monitoring_alerts.alert_thresholds["response_time_p95"] = config_data['monitoring'][
                    'response_time_threshold']

        return {"status": "success", "message": "Configuration updated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/config/quick-links")
async def update_quick_links(request: Request):
    """Update quick links"""
    try:
        links = await request.json()
        config_manager.set('quick_links', links)
        return {"status": "success", "message": "Quick links updated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/system/info")
async def get_system_info():
    """Get system information"""
    display_ip = config_manager.get_display_ip()
    port = config_manager.get('network.port', Config.PORT)

    return {
        "server_urls": {
            "local": f"http://localhost:{port}",
            "network": f"http://{display_ip}:{port}",
            "monitor": f"http://{display_ip}:{port}/monitor",
            "metrics": f"http://{display_ip}:{port}/metrics",
            "health": f"http://{display_ip}:{port}/api/health/detailed"
        },
        "detected_ips": get_all_local_ips(),
        "current_ip": display_ip,
        "auto_detect": config_manager.get('network.auto_detect_ip', True)
    }


def get_all_local_ips():
    """Get all local IP addresses"""
    import socket
    ips = []
    try:
        hostname = socket.gethostname()
        all_ips = socket.gethostbyname_ex(hostname)[2]
        for ip in all_ips:
            if not ip.startswith('127.') and not ip.startswith('198.18.'):
                ips.append(ip)
    except:
        pass
    return ips


@app.get("/api/health/detailed")
async def get_detailed_health():
    """Get detailed health status"""
    error_rate = monitoring_alerts.calculate_error_rate()
    perf_stats = performance_monitor.get_stats()

    # === Add more health metrics ===
    health_score = 100.0
    issues = []
    # Check error rate (deduct 20 points if error rate exceeds 10%)
    if error_rate > 0.1:
        health_score -= 20
        issues.append(f"High error rate: {error_rate:.1%}")
    elif error_rate > 0.05:  # Deduct 10 points for 5-10%
        health_score -= 10
        issues.append(f"Moderate error rate: {error_rate:.1%}")
    # Check response time (deduct 15 points if P95 exceeds 30 seconds)
    p95_time = perf_stats.get("p95_response_time", 0)
    if p95_time > 30:
        health_score -= 15
        issues.append(f"Slow P95 response time: {p95_time:.1f}s")
    elif p95_time > 15:  # Deduct 7 points for 15-30s
        health_score -= 7
        issues.append(f"Moderate P95 response time: {p95_time:.1f}s")
    # Check browser connection (deduct 30 points if disconnected)
    if not browser_ws:
        health_score -= 30
        issues.append("Browser WebSocket disconnected")
    # Check active request count (deduct 10 points if over 80% capacity)
    active_count = len(realtime_stats.active_requests)
    capacity_usage = active_count / Config.MAX_CONCURRENT_REQUESTS
    if capacity_usage > 0.8:
        health_score -= 10
        issues.append(f"High active requests: {active_count}/{Config.MAX_CONCURRENT_REQUESTS} ({capacity_usage:.0%})")
    elif capacity_usage > 0.6:  # Deduct 5 points for 60-80%
        health_score -= 5
        issues.append(
            f"Moderate active requests: {active_count}/{Config.MAX_CONCURRENT_REQUESTS} ({capacity_usage:.0%})")
    # Check monitor clients (deduct 5 points if none connected)
    if len(monitor_clients) == 0:
        health_score -= 5
        issues.append("No monitoring clients connected")
    # Ensure score is between 0 and 100
    health_score = max(0, min(100, health_score))
    # Determine health status
    if health_score >= 70:
        status = "healthy"
        status_emoji = "✅"
    elif health_score >= 40:
        status = "degraded"
        status_emoji = "⚠️"
    else:
        status = "unhealthy"
        status_emoji = "❌"
    # === End of scoring system ===

    return {
        "status": status,
        "status_emoji": status_emoji,
        "health_score": health_score,
        "issues": issues,
        "recommendations": get_health_recommendations(issues),  # Add recommendations
        "metrics": {
            "error_rate": error_rate,
            "error_rate_percent": f"{error_rate:.1%}",
            "response_time_p50": perf_stats.get("p50_response_time", 0),
            "response_time_p95": perf_stats.get("p95_response_time", 0),
            "response_time_p99": perf_stats.get("p99_response_time", 0),
            "qps": perf_stats.get("qps", 0),
            "active_requests": active_count,
            "capacity_usage": f"{capacity_usage:.0%}",
            "browser_connected": browser_ws is not None,
            "monitor_clients": len(monitor_clients),
            "uptime": time.time() - startup_time,
            "uptime_hours": (time.time() - startup_time) / 3600
        },
        "thresholds": monitoring_alerts.alert_thresholds
    }


def get_health_recommendations(issues):
    """Provide recommendations based on issues"""
    recommendations = []

    for issue in issues:
        if "error rate" in issue.lower():
            recommendations.append("Check server logs for error patterns")
        elif "response time" in issue.lower():
            recommendations.append("Consider reducing concurrent requests or optimizing model selection")
        elif "browser" in issue.lower():
            recommendations.append("Ensure browser extension is running and connected")
        elif "active requests" in issue.lower():
            recommendations.append("Consider increasing MAX_CONCURRENT_REQUESTS if server can handle it")
        elif "monitoring" in issue.lower():
            recommendations.append("Open /monitor in a browser to track system health")

    return recommendations


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_dashboard():
    """Monitor dashboard"""
    with open("templates\monitor.html", "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(content=html)

print("\n" + "="*60)
print("🚀 LMArena Reverse Proxy Server")
print("="*60)
print(f"📍 Local access: http://localhost:{Config.PORT}")
print(f"📍 LAN access: http://{get_local_ip()}:{Config.PORT}")
print(f"📊 Monitor dashboard: http://{get_local_ip()}:{Config.PORT}/monitor")
print("="*60)
print("💡 Tip: Please make sure the browser extension is installed and enabled")
print("💡 If you use proxy software, the LAN IP may be inaccurate")
print("💡 If the LAN IP is inaccurate, you can modify it in this file: change MANUAL_IP = None to MANUAL_IP = your specified IP address, e.g., 192.168.0.1")
print("="*60 + "\n")

if __name__ == "__main__":
    uvicorn.run(app, host=Config.HOST, port=Config.PORT)

