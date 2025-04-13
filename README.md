# 🛰️ Sensor Data WebSocket Server(SocketCap)

This is a Python WebSocket server designed to receive and process real-time sensor and GPS data from a Mobile app(SocketSense). It supports saving data to CSV, live visualization, and multiple client connections.

---

## 📦 Features

- Receives JSON data from a WebSocket client (e.g., Flutter app)
- Supports new and legacy data formats
- Saves sensor data to CSV files
- Visualizes data in real-time (optional)
- Console command interface for managing recording and visualization
- Built-in logging with timestamps
- Works with any client that sends JSON sensor payloads

---

## ⚙️ Requirements

- Python 3.8+
- Install dependencies:

```bash
pip install websockets colorama
```

### Optional (for visualization):

```bash
pip install matplotlib numpy
```

---

## 🚀 Usage

### 1. Clone the repository and run the server

```bash
python main.py --host 0.0.0.0 --port 8765 --record --viz
```

### 2. Command-line Arguments

| Argument      | Description                             | Default   |
|---------------|-----------------------------------------|-----------|
| `--host`      | Host to bind the server to              | 0.0.0.0   |
| `--port`      | Port for WebSocket server               | 8765      |
| `--record`    | Start recording data to CSV immediately | False     |
| `--viz`       | Enable real-time data visualization     | False     |
| `--debug`     | Enable debug logs                       | False     |

### 3. Console Commands (at runtime)

- `record` – Start recording to CSV
- `stop` – Stop recording
- `plot` – Start live data plotting (if installed)
- `exit` – Gracefully stop server
- `help` – Show help commands

---

## 📡 Data Structure

### ✅ Expected Format from Flutter app:

```json
{
  "timestamp": 1713003315000,
  "location": {
    "latitude": -1.95,
    "longitude": 30.06,
    "altitude": 1540,
    "speed": 0.0,
    "accuracy": 5.0
  },
  "pressure": 1012.58,
  "accelerometer": {"x": 0.1, "y": -0.2, "z": 9.8},
  "gyroscope": {"x": 0.01, "y": 0.02, "z": 0.03},
  "magnetometer": {"x": 35.6, "y": -47.1, "z": 12.5},
  "orientation": {"pitch": 0.2, "roll": 0.1, "yaw": -0.05}
}
```

### 🧱 Legacy Format Support:

The server also accepts older field-based formats using flat fields like `accel_x`, `gyro_y`, `latitude`, etc.

---

## 🧪 Output

### ✅ CSV Logging

Logged files are saved to `sensor_data/` directory with timestamped filenames like:

```
sensor_data_20250413_123045.csv
```

### 📊 Visualization

If `--viz` is enabled or `plot` is typed at runtime, real-time plots will show:

- Accelerometer data (X, Y, Z)
- Gyroscope data (X, Y, Z)
- Orientation (Pitch, Roll, Yaw)

---

## 🔧 Customizing Flutter WebSocket Endpoint

In the SockeSense app, simply input the WebSocket address from the **Settings tab** (e.g., `192.168.4.1:8765`) to connect to this server.

---
## Other Examples
[Simple Websocket](/simple_main.py)
[NodeJs WebSocket](/index.js)

## 🧑‍💻 Author

Developed by HIRWA Rukundo Hope
