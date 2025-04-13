import asyncio
import websockets
import json
import logging
from datetime import datetime
import sqlite3  # For data storage
import pandas as pd  # For data analysis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sensor_server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sensor_server")


# Database setup
def setup_database():
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()

    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS location_data (
        timestamp INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        altitude REAL,
        speed REAL,
        heading REAL,
        accuracy REAL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_data (
        timestamp INTEGER PRIMARY KEY,
        accel_x REAL, accel_y REAL, accel_z REAL,
        gyro_x REAL, gyro_y REAL, gyro_z REAL,
        mag_x REAL, mag_y REAL, mag_z REAL,
        pressure REAL,
        pitch REAL, roll REAL, yaw REAL
    )
    ''')

    conn.commit()
    return conn


# Data processing functions
class DataProcessor:
    def __init__(self):
        self.conn = setup_database()
        self.data_buffer = []
        self.buffer_size = 100  # Save to DB after this many records

    def process_data(self, data):
        """Process incoming sensor data"""
        try:
            # Extract timestamp
            timestamp = data["timestamp"]

            # Process location data if available
            if data.get("location"):
                self.save_location_data(timestamp, data["location"])

            # Process sensor data
            self.save_sensor_data(timestamp, data)

            # Example of real-time analysis (you can add your custom processing here)
            self.analyze_data(data)

            # Buffer and periodically save to database
            self.data_buffer.append(data)
            if len(self.data_buffer) >= self.buffer_size:
                self.flush_buffer()

            return True
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return False

    def save_location_data(self, timestamp, location_data):
        """Save location data to database"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO location_data VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    location_data.get("latitude"),
                    location_data.get("longitude"),
                    location_data.get("altitude"),
                    location_data.get("speed"),
                    location_data.get("heading"),
                    location_data.get("accuracy")
                )
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Database error saving location: {e}")

    def save_sensor_data(self, timestamp, data):
        """Save sensor data to database"""
        cursor = self.conn.cursor()
        try:
            accel = data.get("accelerometer", {})
            gyro = data.get("gyroscope", {})
            mag = data.get("magnetometer", {})
            orientation = data.get("orientation", {})

            cursor.execute(
                "INSERT OR REPLACE INTO sensor_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    accel.get("x"), accel.get("y"), accel.get("z"),
                    gyro.get("x"), gyro.get("y"), gyro.get("z"),
                    mag.get("x"), mag.get("y"), mag.get("z"),
                    data.get("pressure"),
                    orientation.get("pitch"), orientation.get("roll"), orientation.get("yaw")
                )
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Database error saving sensor data: {e}")

    def analyze_data(self, data):
        """Perform real-time analysis on the data"""
        # Example: Detect significant motion
        accel = data.get("accelerometer", {})
        if accel:
            magnitude = (accel.get("x", 0) ** 2 + accel.get("y", 0) ** 2 + accel.get("z", 0) ** 2) ** 0.5
            if magnitude > 15:  # Threshold for significant movement
                logger.info(f"Significant motion detected! Magnitude: {magnitude}")

        # Example: Monitor device orientation changes
        orientation = data.get("orientation", {})
        if orientation:
            pitch_deg = orientation.get("pitch", 0) * 180 / 3.14159
            roll_deg = orientation.get("roll", 0) * 180 / 3.14159
            if abs(pitch_deg) > 45 or abs(roll_deg) > 45:
                logger.info(f"Device significantly tilted! Pitch: {pitch_deg:.1f}°, Roll: {roll_deg:.1f}°")

    def flush_buffer(self):
        """Save buffered data and clear buffer"""
        logger.info(f"Flushing buffer with {len(self.data_buffer)} records")
        self.data_buffer = []

    def close(self):
        """Close database connection"""
        self.conn.close()


# Create data processor instance
data_processor = DataProcessor()


# WebSocket server handler
async def sensor_data_handler(websocket, path):
    client_ip = websocket.remote_address[0]
    logger.info(f"Client connected from {client_ip}")

    try:
        # Send welcome message
        await websocket.send(json.dumps({"status": "connected", "message": "Welcome to Sensor Data Server"}))

        # Process incoming messages
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"Received data: {data}")

                # Process the data
                success = data_processor.process_data(data)

                # Acknowledge receipt
                await websocket.send(json.dumps({"status": "received", "timestamp": data.get("timestamp")}))

            except json.JSONDecodeError:
                logger.error(f"Received invalid JSON: {message}")
                await websocket.send(json.dumps({"status": "error", "message": "Invalid JSON format"}))

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed from {client_ip}")
    except Exception as e:
        logger.error(f"Error handling connection: {e}")
    finally:
        logger.info(f"Client disconnected: {client_ip}")


# Start the server
async def main():
    # Start WebSocket server
    server_address = "0.0.0.0"  # Listen on all interfaces
    server_port = 8765  # Same port as in the Flutter app

    logger.info(f"Starting WebSocket server on {server_address}:{server_port}")

    async with websockets.serve(sensor_data_handler, server_address, server_port):
        logger.info("Server started. Waiting for connections...")
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down")
    finally:
        data_processor.close()
        logger.info("Database connection closed")