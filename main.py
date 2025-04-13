import asyncio
import websockets
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
import traceback
from typing import Dict, List, Optional, Any
import csv
import argparse
from colorama import Fore, Back, Style, init as colorama_init

# Optional dependencies for visualization
try:
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation

    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

# Initialize colorama for cross-platform colored terminal output
colorama_init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sensor_server.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SensorServer")

# Global variables
connected_clients = set()
data_buffer = []  # Buffer to store recent data for plotting
MAX_BUFFER_SIZE = 300  # Keep last 5 minutes of data at 1 second intervals
SAVE_DIRECTORY = "sensor_data"
active_recording = False
recording_start_time = None
current_recording_file = None
recording_csv_writer = None


class SensorServer:
    def __init__(self, host="0.0.0.0", port=8765, save_data=False, visualize=False):
        self.host = host
        self.port = port
        self.save_data = save_data
        self.visualize = visualize and VISUALIZATION_AVAILABLE
        self.server = None
        self.animation = None
        self.fig = None
        self.data_lock = asyncio.Lock()

        # Ensure the data directory exists
        if self.save_data:
            os.makedirs(SAVE_DIRECTORY, exist_ok=True)

    async def handle_connection(self, websocket, path):
        """Handle an incoming WebSocket connection."""
        client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        logger.info(f"Client connected from {client_info}")
        print(f"{Fore.GREEN}➕ Client connected from {client_info}{Style.RESET_ALL}")

        connected_clients.add(websocket)

        try:
            # Send welcome message
            await websocket.send(json.dumps({
                "type": "server_info",
                "message": "Connected to Sensor Data Server",
                "timestamp": datetime.now().isoformat(),
                "client_count": len(connected_clients)
            }))

            # Process incoming messages
            async for message in websocket:
                try:
                    await self.process_message(websocket, message)
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON from {client_info}")
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": "Invalid JSON format"
                    }))
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    logger.debug(traceback.format_exc())
        except websockets.ConnectionClosed:
            logger.info(f"Client disconnected from {client_info}")
            print(f"{Fore.RED}➖ Client disconnected from {client_info}{Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"Unexpected error with client {client_info}: {str(e)}")
            logger.debug(traceback.format_exc())
        finally:
            connected_clients.remove(websocket)

    async def process_message(self, websocket, message):
        """Process an incoming message from a client."""
        data = json.loads(message)

        # Add timestamp if not present
        if "timestamp" not in data:
            data["timestamp"] = int(time.time() * 1000)

        # Handle based on the updated data structure from Flutter app
        if "location" in data:
            await self.handle_sensor_data(data)
        else:
            # For backward compatibility with the original data format
            await self.handle_legacy_data(data)

        # Acknowledge receipt
        await websocket.send(json.dumps({
            "type": "ack",
            "timestamp": datetime.now().isoformat()
        }))

    async def handle_sensor_data(self, data):
        """Handle new format sensor data."""
        # Print nicely formatted data to console
        self.print_sensor_data(data)

        # Save data if recording is active
        if active_recording and recording_csv_writer:
            self.save_data_to_csv(data)

        # Update data buffer for visualization
        async with self.data_lock:
            global data_buffer
            data_buffer.append(data)
            # Keep buffer size limited
            if len(data_buffer) > MAX_BUFFER_SIZE:
                data_buffer = data_buffer[-MAX_BUFFER_SIZE:]

    async def handle_legacy_data(self, data):
        """Handle the original data format for backward compatibility."""
        # Print basic information
        print(f"\n{Fore.CYAN}📦 Received legacy format data:{Style.RESET_ALL}")

        try:
            print(f"Latitude: {data['latitude']}")
            print(f"Longitude: {data['longitude']}")
            print(f"Altitude: {data['altitude']} m")
            print(f"Pressure: {data['pressure']} hPa")
            print(f"Accelerometer: x={data['accel_x']} y={data['accel_y']} z={data['accel_z']}")
            print(f"Gyroscope:     x={data['gyro_x']} y={data['gyro_y']} z={data['gyro_z']}")
            print(f"Magnetometer:  x={data['mag_x']} y={data['mag_y']} z={data['mag_z']}")
            print(f"Orientation:   Pitch={data['pitch']} Roll={data['roll']} Yaw={data['yaw']}")
        except KeyError as e:
            logger.warning(f"Missing key in legacy data: {e}")

        # Save data if recording is active
        if active_recording and recording_csv_writer:
            self.save_legacy_data_to_csv(data)

        # Transform to new format for visualization
        transformed_data = self.transform_legacy_data(data)

        # Update data buffer for visualization
        async with self.data_lock:
            global data_buffer
            data_buffer.append(transformed_data)
            # Keep buffer size limited
            if len(data_buffer) > MAX_BUFFER_SIZE:
                data_buffer = data_buffer[-MAX_BUFFER_SIZE:]

    def transform_legacy_data(self, data):
        """Transform legacy data format to new format for consistency."""
        return {
            "timestamp": int(time.time() * 1000),
            "location": {
                "latitude": data.get("latitude", 0),
                "longitude": data.get("longitude", 0),
                "altitude": data.get("altitude", 0),
                "speed": 0,
                "heading": 0,
                "accuracy": 0
            },
            "pressure": data.get("pressure", 0),
            "accelerometer": {
                "x": data.get("accel_x", 0),
                "y": data.get("accel_y", 0),
                "z": data.get("accel_z", 0)
            },
            "gyroscope": {
                "x": data.get("gyro_x", 0),
                "y": data.get("gyro_y", 0),
                "z": data.get("gyro_z", 0)
            },
            "magnetometer": {
                "x": data.get("mag_x", 0),
                "y": data.get("mag_y", 0),
                "z": data.get("mag_z", 0)
            },
            "orientation": {
                "pitch": data.get("pitch", 0),
                "roll": data.get("roll", 0),
                "yaw": data.get("yaw", 0)
            }
        }

    def print_sensor_data(self, data):
        """Print the sensor data in a nicely formatted way."""
        timestamp = datetime.fromtimestamp(data["timestamp"] / 1000).strftime('%H:%M:%S')

        print(f"\n{Fore.CYAN}📦 Received data at {timestamp}:{Style.RESET_ALL}")

        if "location" in data and data["location"]:
            loc = data["location"]
            print(f"{Fore.YELLOW}📍 Location:{Style.RESET_ALL}")
            print(f"  Latitude:  {loc.get('latitude', 'N/A')}")
            print(f"  Longitude: {loc.get('longitude', 'N/A')}")
            print(f"  Altitude:  {loc.get('altitude', 'N/A')} m")
            if 'speed' in loc:
                print(f"  Speed:     {loc.get('speed', 0) * 3.6:.2f} km/h")
            if 'accuracy' in loc:
                print(f"  Accuracy:  ±{loc.get('accuracy', 0):.2f} m")

        if "pressure" in data:
            print(f"{Fore.MAGENTA}🌡️ Barometer:{Style.RESET_ALL} {data['pressure']:.2f} hPa")

        if "accelerometer" in data:
            accel = data["accelerometer"]
            print(f"{Fore.GREEN}⚡ Accelerometer:{Style.RESET_ALL} "
                  f"x={accel.get('x', 0):.3f} y={accel.get('y', 0):.3f} z={accel.get('z', 0):.3f} m/s²")

        if "gyroscope" in data:
            gyro = data["gyroscope"]
            print(f"{Fore.BLUE}🔄 Gyroscope:{Style.RESET_ALL}     "
                  f"x={gyro.get('x', 0):.3f} y={gyro.get('y', 0):.3f} z={gyro.get('z', 0):.3f} rad/s")

        if "magnetometer" in data:
            mag = data["magnetometer"]
            print(f"{Fore.RED}🧲 Magnetometer:{Style.RESET_ALL}  "
                  f"x={mag.get('x', 0):.3f} y={mag.get('y', 0):.3f} z={mag.get('z', 0):.3f} µT")

        if "orientation" in data:
            orient = data["orientation"]
            # Convert radians to degrees
            pitch_deg = orient.get('pitch', 0) * 180 / 3.14159 if isinstance(orient.get('pitch', 0),
                                                                             float) else orient.get('pitch', 0)
            roll_deg = orient.get('roll', 0) * 180 / 3.14159 if isinstance(orient.get('roll', 0),
                                                                           float) else orient.get('roll', 0)
            yaw_deg = orient.get('yaw', 0) * 180 / 3.14159 if isinstance(orient.get('yaw', 0), float) else orient.get(
                'yaw', 0)

            print(f"{Fore.CYAN}🧭 Orientation:{Style.RESET_ALL}   "
                  f"Pitch={pitch_deg:.1f}° Roll={roll_deg:.1f}° Yaw={yaw_deg:.1f}°")

    def save_data_to_csv(self, data):
        """Save the data to a CSV file."""
        global recording_csv_writer

        if not recording_csv_writer:
            return

        try:
            # Flatten the nested dictionary
            flat_data = {
                'timestamp': data['timestamp'],
                'datetime': datetime.fromtimestamp(data['timestamp'] / 1000).isoformat(),
            }

            # Add location data if available
            if 'location' in data and data['location']:
                for key, value in data['location'].items():
                    flat_data[f'location_{key}'] = value

            # Add other sensor data
            for sensor in ['accelerometer', 'gyroscope', 'magnetometer', 'orientation']:
                if sensor in data and data[sensor]:
                    for axis, value in data[sensor].items():
                        flat_data[f'{sensor}_{axis}'] = value

            # Add pressure as a top-level field
            if 'pressure' in data:
                flat_data['pressure'] = data['pressure']

            # Write to CSV
            recording_csv_writer.writerow(flat_data)
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")

    def save_legacy_data_to_csv(self, data):
        """Save legacy format data to CSV."""
        global recording_csv_writer

        if not recording_csv_writer:
            return

        try:
            # Create a flat dictionary with timestamp
            flat_data = {
                'timestamp': int(time.time() * 1000),
                'datetime': datetime.now().isoformat(),
            }

            # Add all keys from the original data
            for key, value in data.items():
                flat_data[key] = value

            # Write to CSV
            recording_csv_writer.writerow(flat_data)
        except Exception as e:
            logger.error(f"Error saving legacy data to CSV: {e}")

    async def start(self):
        """Start the WebSocket server."""
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Start server
        self.server = await websockets.serve(
            self.handle_connection,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=30
        )

        server_url = f"ws://{self.host if self.host != '0.0.0.0' else 'localhost'}:{self.port}"
        print(f"{Fore.GREEN}🚀 WebSocket server started at {server_url}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}ℹ️ Press Ctrl+C to stop the server{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}⌨️ Commands:{Style.RESET_ALL}")
        print("  record - Start recording data to CSV")
        print("  stop   - Stop recording")
        print("  plot   - Show real-time visualization (if matplotlib is installed)")
        print("  exit   - Stop the server and exit")

        # Start command handling in background
        asyncio.create_task(self.handle_commands())

        # Start visualization if requested
        if self.visualize:
            self.start_visualization()

        await self.server.wait_closed()

    async def shutdown(self):
        """Gracefully shut down the server."""
        print(f"{Fore.YELLOW}Shutting down server...{Style.RESET_ALL}")

        # Stop recording if active
        global active_recording
        if active_recording:
            self.stop_recording()

        # Close all client connections
        if connected_clients:
            close_tasks = [client.close() for client in connected_clients]
            await asyncio.gather(*close_tasks, return_exceptions=True)

        # Close the server
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        print(f"{Fore.GREEN}Server has been shut down.{Style.RESET_ALL}")

    async def handle_commands(self):
        """Handle commands from the console."""
        while True:
            cmd = await asyncio.get_event_loop().run_in_executor(None, input, "")
            cmd = cmd.strip().lower()

            if cmd == "exit":
                await self.shutdown()
                sys.exit(0)
            elif cmd == "record":
                self.start_recording()
            elif cmd == "stop":
                self.stop_recording()
            elif cmd == "plot" and VISUALIZATION_AVAILABLE:
                self.start_visualization()
            elif cmd == "help":
                print(f"{Fore.YELLOW}Available commands:{Style.RESET_ALL}")
                print("  record - Start recording data to CSV")
                print("  stop   - Stop recording")
                print("  plot   - Show real-time visualization (if matplotlib is installed)")
                print("  exit   - Stop the server and exit")
                print("  help   - Show this help message")
            else:
                print(f"{Fore.RED}Unknown command: {cmd}{Style.RESET_ALL}")

    def start_recording(self):
        """Start recording data to a CSV file."""
        global active_recording, recording_start_time, current_recording_file, recording_csv_writer

        if active_recording:
            print(f"{Fore.YELLOW}⚠️ Recording is already active!{Style.RESET_ALL}")
            return

        try:
            # Create a new file name with timestamp
            recording_start_time = datetime.now()
            filename = f"sensor_data_{recording_start_time.strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(SAVE_DIRECTORY, filename)

            # Open file and create CSV writer
            file = open(filepath, 'w', newline='')
            recording_csv_writer = csv.DictWriter(
                file,
                fieldnames=[
                    'timestamp', 'datetime',
                    'location_latitude', 'location_longitude', 'location_altitude',
                    'location_speed', 'location_heading', 'location_accuracy',
                    'pressure',
                    'accelerometer_x', 'accelerometer_y', 'accelerometer_z',
                    'gyroscope_x', 'gyroscope_y', 'gyroscope_z',
                    'magnetometer_x', 'magnetometer_y', 'magnetometer_z',
                    'orientation_pitch', 'orientation_roll', 'orientation_yaw'
                ]
            )
            recording_csv_writer.writeheader()

            current_recording_file = file
            active_recording = True

            print(f"{Fore.GREEN}🔴 Recording started! Saving to {filename}{Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"Failed to start recording: {e}")
            print(f"{Fore.RED}❌ Failed to start recording: {e}{Style.RESET_ALL}")

    def stop_recording(self):
        """Stop the active recording."""
        global active_recording, recording_start_time, current_recording_file, recording_csv_writer

        if not active_recording:
            print(f"{Fore.YELLOW}⚠️ No active recording to stop!{Style.RESET_ALL}")
            return

        try:
            if current_recording_file:
                current_recording_file.close()

            duration = datetime.now() - recording_start_time
            print(f"{Fore.GREEN}⏹ Recording stopped! Duration: {duration}{Style.RESET_ALL}")

            active_recording = False
            recording_start_time = None
            current_recording_file = None
            recording_csv_writer = None
        except Exception as e:
            logger.error(f"Error stopping recording: {e}")
            print(f"{Fore.RED}❌ Error stopping recording: {e}{Style.RESET_ALL}")

    def start_visualization(self):
        """Start the real-time data visualization if matplotlib is available."""
        if not VISUALIZATION_AVAILABLE:
            print(f"{Fore.RED}❌ Visualization requires matplotlib, numpy. Install with:")
            print(f"{Fore.YELLOW}pip install matplotlib numpy{Style.RESET_ALL}")
            return

        try:
            # Create a new thread for the visualization to avoid blocking the server
            import threading
            vis_thread = threading.Thread(target=self._visualization_thread)
            vis_thread.daemon = True
            vis_thread.start()
            print(f"{Fore.GREEN}📊 Starting visualization in a new window...{Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"Failed to start visualization: {e}")
            print(f"{Fore.RED}❌ Failed to start visualization: {e}{Style.RESET_ALL}")

    def _visualization_thread(self):
        """Thread function to handle the visualization."""
        try:
            # Create figure with subplots
            plt.style.use('ggplot')
            fig, axs = plt.subplots(3, 1, figsize=(10, 12), sharex=True)
            fig.canvas.manager.set_window_title('Sensor Data Visualization')

            # Initialize empty plots
            times = np.array([])

            # Accelerometer data
            accel_x, accel_y, accel_z = np.array([]), np.array([]), np.array([])
            accel_lines = []
            accel_lines.append(axs[0].plot([], [], 'r-', label='X-axis')[0])
            accel_lines.append(axs[0].plot([], [], 'g-', label='Y-axis')[0])
            accel_lines.append(axs[0].plot([], [], 'b-', label='Z-axis')[0])
            axs[0].set_title('Accelerometer (m/s²)')
            axs[0].set_ylabel('Acceleration (m/s²)')
            axs[0].legend()

            # Gyroscope data
            gyro_x, gyro_y, gyro_z = np.array([]), np.array([]), np.array([])
            gyro_lines = []
            gyro_lines.append(axs[1].plot([], [], 'r-', label='X-axis')[0])
            gyro_lines.append(axs[1].plot([], [], 'g-', label='Y-axis')[0])
            gyro_lines.append(axs[1].plot([], [], 'b-', label='Z-axis')[0])
            axs[1].set_title('Gyroscope (rad/s)')
            axs[1].set_ylabel('Angular velocity (rad/s)')
            axs[1].legend()

            # Orientation data
            pitch, roll, yaw = np.array([]), np.array([]), np.array([])
            orient_lines = []
            orient_lines.append(axs[2].plot([], [], 'r-', label='Pitch')[0])
            orient_lines.append(axs[2].plot([], [], 'g-', label='Roll')[0])
            orient_lines.append(axs[2].plot([], [], 'b-', label='Yaw')[0])
            axs[2].set_title('Orientation (degrees)')
            axs[2].set_ylabel('Angle (degrees)')
            axs[2].set_xlabel('Time (s)')
            axs[2].legend()

            # Setup animation function
            def update_plot(frame):
                nonlocal times, accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z, pitch, roll, yaw

                # Get data from buffer (copy to avoid race conditions)
                buffer_copy = list(data_buffer)

                if not buffer_copy:
                    return accel_lines + gyro_lines + orient_lines

                # Extract timestamps and convert to seconds from start
                start_time = buffer_copy[0]['timestamp'] / 1000 if buffer_copy else 0
                times = np.array([(d['timestamp'] / 1000 - start_time) for d in buffer_copy])

                # Extract accelerometer data
                accel_x = np.array([d.get('accelerometer', {}).get('x', 0) for d in buffer_copy])
                accel_y = np.array([d.get('accelerometer', {}).get('y', 0) for d in buffer_copy])
                accel_z = np.array([d.get('accelerometer', {}).get('z', 0) for d in buffer_copy])

                # Extract gyroscope data
                gyro_x = np.array([d.get('gyroscope', {}).get('x', 0) for d in buffer_copy])
                gyro_y = np.array([d.get('gyroscope', {}).get('y', 0) for d in buffer_copy])
                gyro_z = np.array([d.get('gyroscope', {}).get('z', 0) for d in buffer_copy])

                # Extract orientation data and convert to degrees
                pitch = np.array([d.get('orientation', {}).get('pitch', 0) * 180 / np.pi
                                  for d in buffer_copy])
                roll = np.array([d.get('orientation', {}).get('roll', 0) * 180 / np.pi
                                 for d in buffer_copy])
                yaw = np.array([d.get('orientation', {}).get('yaw', 0) * 180 / np.pi
                                for d in buffer_copy])

                # Update line data
                accel_lines[0].set_data(times, accel_x)
                accel_lines[1].set_data(times, accel_y)
                accel_lines[2].set_data(times, accel_z)

                gyro_lines[0].set_data(times, gyro_x)
                gyro_lines[1].set_data(times, gyro_y)
                gyro_lines[2].set_data(times, gyro_z)

                orient_lines[0].set_data(times, pitch)
                orient_lines[1].set_data(times, roll)
                orient_lines[2].set_data(times, yaw)

                # Adjust y-axis limits if needed
                for i, ax in enumerate(axs):
                    ax.relim()
                    ax.autoscale_view()

                # Set x-axis limits
                if len(times) > 0:
                    for ax in axs:
                        ax.set_xlim(max(0, times[-1] - 30), max(30, times[-1]))

                return accel_lines + gyro_lines + orient_lines

            # Create animation
            ani = FuncAnimation(fig, update_plot, interval=100, blit=True)
            plt.tight_layout()
            plt.show()
        except Exception as e:
            logger.error(f"Visualization error: {e}")
            print(f"{Fore.RED}Visualization error: {e}{Style.RESET_ALL}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='WebSocket Server for Sensor Data')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8765, help='Port to bind the server to')
    parser.add_argument('--record', action='store_true', help='Start recording data immediately')
    parser.add_argument('--viz', action='store_true', help='Show real-time data visualization')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    return parser.parse_args()


async def main():
    """Main function to start the server."""
    # Parse command line arguments
    args = parse_arguments()

    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Print banner
    print("\n" + "=" * 50)
    print(f"{Fore.CYAN}Sensor Data WebSocket Server{Style.RESET_ALL}")
    print("=" * 50)

    # Check dependencies
    if args.viz and not VISUALIZATION_AVAILABLE:
        print(f"{Fore.YELLOW}⚠️ Visualization requested but matplotlib/numpy not found.")
        print(f"Install with: pip install matplotlib numpy{Style.RESET_ALL}")

    # Create and start the server
    server = SensorServer(
        host=args.host,
        port=args.port,
        save_data=True,
        visualize=args.viz
    )

    # Start recording if requested
    if args.record:
        server.start_recording()

    # Start the server
    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{Fore.YELLOW}Server stopped by user.{Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        logger.debug(traceback.format_exc())
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
    finally:
        # Ensure we clean up any recording in progress
        if active_recording and current_recording_file:
            try:
                current_recording_file.close()
                print(f"{Fore.GREEN}Recording saved.{Style.RESET_ALL}")
            except:
                pass