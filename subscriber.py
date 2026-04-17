#!/usr/bin/env python3
"""Multi-user OwnTracks → GitHub location subscriber."""
import json
import csv
import os
import subprocess
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
import paho.mqtt.client as mqtt

CONFIG_FILE = Path(__file__).parent / "users.json"
TZ_BKK = timezone(timedelta(hours=7))
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USER = "server"
MQTT_PASS = os.environ.get("MQTT_SERVER_PASS", "")

MQTT_TOPIC = "owntracks/#"


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return json.load(f).get("users", {})


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    import math
    R = 6371000
    p = math.pi / 180
    a = 0.5 - math.cos((lat2 - lat1) * p) / 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * (1 - math.cos((lon2 - lon1) * p)) / 2
    return 2 * R * math.asin(math.sqrt(a))


def resolve_address(lat, lon, named_places: list) -> str:
    for place in named_places:
        if haversine_m(lat, lon, place["lat"], place["lon"]) <= place["radius_m"]:
            return place["name"]
    return reverse_geocode(lat, lon)


def reverse_geocode(lat, lon) -> str:
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "Paji-LocationServer/1.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read()).get("display_name", "")
    except Exception:
        return ""


def write_csv(path: str, lat, lon, address, ts, batt, acc, device=""):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["lat", "lon", "address", "timestamp", "battery", "accuracy", "device"])
        writer.writerow([lat, lon, address, ts, batt, acc, device])


def append_history(path: str, lat, lon, address, ts, batt, acc, device=""):
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["lat", "lon", "address", "timestamp", "battery", "accuracy", "device"])
        writer.writerow([lat, lon, address, ts, batt, acc, device])


def git_push(repo_dir: str, lat, lon):
    ts = datetime.now(TZ_BKK).strftime("%Y-%m-%dT%H:%M:%S+07:00")
    subprocess.run(["git", "-C", repo_dir, "add", "current.csv", "history.csv"], check=True)
    result = subprocess.run(["git", "-C", repo_dir, "commit", "-m", f"loc: {lat},{lon} @ {ts}"])
    if result.returncode != 0:
        return
    subprocess.run(["git", "-C", repo_dir, "push"], check=True)
    print(f"[pushed] {lat},{lon} @ {ts}")


def on_message(client, userdata, msg):
    try:
        # topic: owntracks/{username}/{device}
        parts = msg.topic.split("/")
        if len(parts) < 3:
            return
        username = parts[1]
        device = parts[2] if len(parts) > 2 else "phone"

        config = load_config()
        if username not in config:
            print(f"[skip] unknown user: {username}")
            return

        user_cfg = config[username]
        data = json.loads(msg.payload)
        if data.get("_type") != "location":
            return

        lat = data.get("lat")
        lon = data.get("lon")
        batt = data.get("batt", "")
        acc = data.get("acc", "")
        ts = datetime.fromtimestamp(data.get("tst", 0), tz=TZ_BKK).strftime("%Y-%m-%dT%H:%M:%S+07:00")

        named_places = user_cfg.get("named_places", [])
        address = resolve_address(lat, lon, named_places)

        repo_dir = user_cfg["repo_dir"]
        write_csv(f"{repo_dir}/current.csv", lat, lon, address, ts, batt, acc, device)
        append_history(f"{repo_dir}/history.csv", lat, lon, address, ts, batt, acc, device)

        print(f"[{username}] {lat},{lon} acc={acc}m — {address[:50] if address else 'unknown'}")
        git_push(repo_dir, lat, lon)

    except Exception as e:
        print(f"[error] {e}")


client = mqtt.Client()
if MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT)
client.subscribe(MQTT_TOPIC)
print(f"[listening] {MQTT_TOPIC}")
client.loop_forever()
