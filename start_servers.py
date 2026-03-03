#!/usr/bin/env python3
"""
Startup script for running both FastAPI and MCP servers
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path

class ServerManager:
    def __init__(self):
        self.processes = []
        self.running = True

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down servers...")
        self.running = False
        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception:
                pass  # Process might already be dead
        sys.exit(0)

    def start_fastapi_server(self):
        """Start the FastAPI server"""
        port = int(os.environ.get("PORT", 8000))
        print(f"Starting FastAPI server on port {port}...")
        process = subprocess.Popen([
            sys.executable, "-m", "uvicorn", "server:app",
            "--host", "0.0.0.0",
            "--port", str(port),
            "--reload"
        ])
        self.processes.append(process)
        return process

    def start_websocket_server(self):
        """Start the WebSocket server"""
        raise RuntimeError("WebSocket server is hosted by FastAPI in /ws")

    def start_mcp_server(self):
        """Start the MCP server"""
        mcp_port = int(os.environ.get("MCP_PORT", 9000))
        print(f"Starting MCP server on port {mcp_port}...")
        process = subprocess.Popen([
            sys.executable, "mcp_server.py"
        ])
        self.processes.append(process)
        return process

    def run(self):
        """Run all servers"""
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        try:
            # Start FastAPI server
            fastapi_process = self.start_fastapi_server()

            # Wait a moment for FastAPI to start
            time.sleep(2)

            # Check if FastAPI started successfully
            if fastapi_process.poll() is not None:
                print("Failed to start FastAPI server")
                return

            print("FastAPI server started successfully!")

            # Start MCP server
            mcp_process = self.start_mcp_server()

            # Wait a moment for MCP server to start
            time.sleep(2)

            print("\nAvailable endpoints:")
            fastapi_port = int(os.environ.get("PORT", 8000))
            print(f"  - Health check: http://localhost:{fastapi_port}/health")
            print(f"  - API docs: http://localhost:{fastapi_port}/docs")
            print(f"  - Main page: http://localhost:{fastapi_port}/")
            print(f"  - WebSocket: ws://localhost:{fastapi_port}/ws")
            mcp_port = int(os.environ.get("MCP_PORT", 9000))
            print(f"  - MCP server: http://0.0.0.0:{mcp_port}/mcp")

            print("\nServers are running. Press Ctrl+C to stop.")

            # Keep the main process alive
            while self.running:
                time.sleep(1)

                # Check if processes are still running
                if fastapi_process.poll() is not None:
                    print("FastAPI server stopped unexpectedly")
                    break

                if mcp_process.poll() is not None:
                    print("MCP server stopped unexpectedly")
                    break

        except KeyboardInterrupt:
            pass
        finally:
            self.signal_handler(None, None)

if __name__ == "__main__":
    manager = ServerManager()
    manager.run()
