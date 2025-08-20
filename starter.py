import os
import sys
import subprocess
import time
from pathlib import Path

script_dir = Path(__file__).parent
os.chdir(script_dir)

browser_exe = script_dir / "browser" / "ungoogled-chromium-portable.exe"

proxy_script = script_dir / "server" / "proxy_server.py"
if proxy_script.exists():
    # subprocess.Popen([sys.executable, str(proxy_script)], 
    #             creationflags=subprocess.CREATE_NEW_CONSOLE)

    subprocess.Popen([sys.executable, str(proxy_script)])


if browser_exe.exists():
    print("Ready For Main Run..")
    
    subprocess.Popen([str(browser_exe), "https://lmarena.ai"], 
                creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    time.sleep(1)
    
    subprocess.Popen([str(browser_exe), "http://localhost:9080/monitor"], 
                    creationflags=subprocess.CREATE_NEW_CONSOLE)
    print("Running on Browser!")
else:
    print(f"Error: {browser_exe} file not found")
    sys.exit(1)
