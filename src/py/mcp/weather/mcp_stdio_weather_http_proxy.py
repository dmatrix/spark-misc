import subprocess
import threading
import queue
from flask import Flask, request, Response

# === CONFIGURATION ===
MCP_COMMAND = ["python3", "weather.py"]  # <-- Change this to your MCP server command

# =====================

app = Flask(__name__)

# Start MCP server as subprocess
mcp_proc = subprocess.Popen(
    MCP_COMMAND,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    bufsize=0
)

# Thread-safe queue for responses
response_queue = queue.Queue()

def read_stdout():
    while True:
        line = mcp_proc.stdout.readline()
        if not line:
            break
        response_queue.put(line)

# Start thread to read MCP server stdout
threading.Thread(target=read_stdout, daemon=True).start()

@app.route("/", methods=["POST"])
def handle_request():
    # Forward request body to MCP server stdin
    mcp_proc.stdin.write(request.data)
    mcp_proc.stdin.flush()
    # Read response from MCP server stdout
    response = response_queue.get()
    return Response(response, mimetype="application/json")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)