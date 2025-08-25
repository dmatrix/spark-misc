import importlib.util
from pathlib import Path

if __name__ == "__main__":
    # Get the script's directory and construct path to utils module
    script_dir = Path(__file__).parent
    utils_dir = script_dir / "utils"
    web_utils_path = utils_dir / "web_utils.py"
    
    spec = importlib.util.spec_from_file_location("web_utils", web_utils_path)
    web_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(web_utils)

    print("=== Testing HTML retrieval ===")
    html = web_utils.get_html("https://httpbin.org/html")
    print(f"HTML length: {len(html)} characters")
    print(f"First 200 chars: {html[:200]}...")
    
    print("\n=== Testing JSON retrieval ===")
    try:
        json_data = web_utils.get_json("https://httpbin.org/json")
        print(f"JSON data: {json_data}")
    except Exception as e:
        print(f"JSON error: {e}")
    
    print("\n=== Testing text retrieval ===")
    text = web_utils.get_text("https://httpbin.org/robots.txt")
    print(f"Text content: {text}")
    
    print("\n=== Testing binary retrieval ===")
    binary = web_utils.get_binary("https://httpbin.org/bytes/50")
    print(f"Binary data length: {len(binary)} bytes")
    print(f"First 20 bytes: {binary[:20]}")
    
    print("\n=== Testing file download ===")
    file_data = web_utils.get_file("https://httpbin.org/bytes/100")
    print(f"File data length: {len(file_data)} bytes")