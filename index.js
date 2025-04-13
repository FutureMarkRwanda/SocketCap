const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

// Create logs directory if it doesn't exist
const logsDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
}

// Setup logging
const logFile = path.join(logsDir, `server_${new Date().toISOString().slice(0, 10)}.log`);
const logger = {
    info: (message) => {
        const logEntry = `[INFO] [${new Date().toISOString()}] ${message}`;
        console.log(logEntry);
        fs.appendFileSync(logFile, logEntry + '\n');
    },
    error: (message) => {
        const logEntry = `[ERROR] [${new Date().toISOString()}] ${message}`;
        console.error(logEntry);
        fs.appendFileSync(logFile, logEntry + '\n');
    },
    debug: (message) => {
        const logEntry = `[DEBUG] [${new Date().toISOString()}] ${message}`;
        console.debug(logEntry);
        fs.appendFileSync(logFile, logEntry + '\n');
    }
};

// Database setup
class Database {
    constructor() {
        this.db = new sqlite3.Database('sensor_data.db', (err) => {
            if (err) {
                logger.error(`Failed to connect to database: ${err.message}`);
            } else {
                logger.info('Connected to the SQLite database');
                this.initTables();
            }
        });
    }

    initTables() {
        this.db.serialize(() => {
            // Location data table
            this.db.run(`CREATE TABLE IF NOT EXISTS location_data (
                timestamp INTEGER PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                altitude REAL, 
                speed REAL,
                heading REAL,
                accuracy REAL
            )`);

            // Sensor data table
            this.db.run(`CREATE TABLE IF NOT EXISTS sensor_data (
                timestamp INTEGER PRIMARY KEY,
                accel_x REAL, accel_y REAL, accel_z REAL,
                gyro_x REAL, gyro_y REAL, gyro_z REAL,
                mag_x REAL, mag_y REAL, mag_z REAL,
                pressure REAL,
                pitch REAL, roll REAL, yaw REAL
            )`);

            logger.info('Database tables initialized');
        });
    }

    saveLocationData(timestamp, location) {
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO location_data
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `);

        stmt.run(
            timestamp,
            location.latitude,
            location.longitude,
            location.altitude,
            location.speed,
            location.heading,
            location.accuracy
        );

        stmt.finalize();
    }

    saveSensorData(timestamp, data) {
        const accel = data.accelerometer || {};
        const gyro = data.gyroscope || {};
        const mag = data.magnetometer || {};
        const orientation = data.orientation || {};

        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO sensor_data
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);

        stmt.run(
            timestamp,
            accel.x, accel.y, accel.z,
            gyro.x, gyro.y, gyro.z,
            mag.x, mag.y, mag.z,
            data.pressure,
            orientation.pitch, orientation.roll, orientation.yaw
        );

        stmt.finalize();
    }

    close() {
        this.db.close((err) => {
            if (err) {
                logger.error(`Error closing database: ${err.message}`);
            } else {
                logger.info('Database connection closed');
            }
        });
    }
}

// Data processor class
class DataProcessor {
    constructor() {
        this.db = new Database();
        this.dataBuffer = [];
        this.bufferSize = 100; // Save to DB after this many records
    }

    processData(data) {
        try {
            // Extract timestamp
            const timestamp = data.timestamp;

            // Process location data if available
            if (data.location) {
                this.db.saveLocationData(timestamp, data.location);
            }

            // Process sensor data
            this.db.saveSensorData(timestamp, data);

            // Example of real-time analysis (you can add your custom processing here)
            this.analyzeData(data);

            // Buffer and periodically save to database
            this.dataBuffer.push(data);
            if (this.dataBuffer.length >= this.bufferSize) {
                this.flushBuffer();
            }

            return { success: true };
        } catch (error) {
            logger.error(`Error processing data: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    analyzeData(data) {
        // Example: Detect significant motion
        const accel = data.accelerometer || {};
        if (accel.x !== undefined) {
            const magnitude = Math.sqrt(accel.x ** 2 + accel.y ** 2 + accel.z ** 2);
            if (magnitude > 15) { // Threshold for significant movement
                logger.info(`Significant motion detected! Magnitude: ${magnitude.toFixed(2)}`);
            }
        }

        // Example: Monitor device orientation changes
        const orientation = data.orientation || {};
        if (orientation.pitch !== undefined) {
            const pitchDeg = orientation.pitch * 180 / Math.PI;
            const rollDeg = orientation.roll * 180 / Math.PI;
            if (Math.abs(pitchDeg) > 45 || Math.abs(rollDeg) > 45) {
                logger.info(`Device significantly tilted! Pitch: ${pitchDeg.toFixed(1)}°, Roll: ${rollDeg.toFixed(1)}°`);
            }
        }

        // Add your custom data processing logic here
        // For example, you might want to:
        // - Detect patterns in movement
        // - Calculate velocity or acceleration trends
        // - Perform feature extraction for ML purposes
        // - Trigger alerts based on certain conditions
    }

    flushBuffer() {
        logger.info(`Flushing buffer with ${this.dataBuffer.length} records`);

        // Here you could do batch processing with the buffered data
        // Example: Calculate average values, generate summaries, etc.

        this.dataBuffer = [];
    }

    close() {
        this.db.close();
    }
}

// Create data processor instance
const dataProcessor = new DataProcessor();

// Set up WebSocket server
const PORT = 8765;
const wss = new WebSocket.Server({ port: PORT });

logger.info(`WebSocket server starting on port ${PORT}`);

// Handle WebSocket connections
wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    logger.info(`Client connected from ${clientIp}`);

    // Send welcome message
    ws.send(JSON.stringify({
        status: 'connected',
        message: 'Welcome to Sensor Data Server'
    }));

    // Handle incoming messages
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            logger.debug(`Received data from ${clientIp}`);

            // Process the data
            const result = dataProcessor.processData(data);

            // Acknowledge receipt
            ws.send(JSON.stringify({
                status: result.success ? 'received' : 'error',
                timestamp: data.timestamp,
                message: result.error || 'Data processed successfully'
            }));

        } catch (error) {
            logger.error(`Error handling message: ${error.message}`);
            ws.send(JSON.stringify({
                status: 'error',
                message: 'Invalid JSON format'
            }));
        }
    });

    // Handle disconnections
    ws.on('close', () => {
        logger.info(`Client disconnected: ${clientIp}`);
    });

    // Handle errors
    ws.on('error', (error) => {
        logger.error(`WebSocket error: ${error.message}`);
    });
});

// Handle server shutdown
process.on('SIGINT', () => {
    logger.info('Server shutting down');
    wss.clients.forEach((client) => {
        client.close();
    });
    dataProcessor.close();
    process.exit(0);
});

logger.info('WebSocket server is running and waiting for connections');