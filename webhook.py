#!/usr/bin/env python3
"""OwnTracks HTTP webhook — receives location POST, writes CSV, git pushes."""
import json
import csv
import os
import subprocess
import urllib.request
import math
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Depends, Header
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

CONFIG_FILE = Path(__file__).parent / "users.json"
TZ_BKK = timezone(timedelta(hours=7))
REGISTER_SECRET = os.environ.get("LOCATION_REGISTER_SECRET", "")

app = FastAPI()
security = HTTPBasic()


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return json.load(f).get("users", {})


def save_config(users: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump({"users": users}, f, indent=2)


def haversine_m(lat1, lon1, lat2, lon2) -> float:
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


def git_push(repo_dir: str, github_repo: str, github_token: str, lat, lon):
    ts = datetime.now(TZ_BKK).strftime("%Y-%m-%dT%H:%M:%S+07:00")
    subprocess.run(["git", "-C", repo_dir, "add", "current.csv", "history.csv"], check=True)
    result = subprocess.run(["git", "-C", repo_dir, "commit", "-m", f"loc: {lat},{lon} @ {ts}"])
    if result.returncode != 0:
        return
    remote_url = f"https://{github_token}@github.com/{github_repo}.git"
    subprocess.run(["git", "-C", repo_dir, "push", remote_url, "HEAD:main"], check=True)
    print(f"[pushed] {lat},{lon} @ {ts}")


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    config = load_config()
    username = credentials.username
    if username not in config:
        raise HTTPException(status_code=401, detail="Unknown user")
    user_cfg = config[username]
    expected_password = user_cfg.get("http_password", "")
    if not secrets.compare_digest(credentials.password, expected_password):
        raise HTTPException(status_code=401, detail="Invalid password")
    return username


class RegisterRequest(BaseModel):
    username: str
    password: str
    github_repo: str   # format: "owner/repo-name"
    github_token: str  # PAT with repo scope


@app.post("/register")
async def register_user(body: RegisterRequest, x_register_secret: str = Header(default="")):
    if REGISTER_SECRET and not secrets.compare_digest(x_register_secret, REGISTER_SECRET):
        raise HTTPException(status_code=403, detail="Invalid register secret")

    config = load_config()
    if body.username in config:
        raise HTTPException(status_code=409, detail=f"Username '{body.username}' already exists")

    repo_dir = f"/home/paji/Project/{body.username.capitalize()}-Location"

    # Clone repo using token
    remote_url = f"https://{body.github_token}@github.com/{body.github_repo}.git"
    if os.path.exists(repo_dir):
        raise HTTPException(status_code=409, detail=f"Directory {repo_dir} already exists")

    result = subprocess.run(
        ["git", "clone", remote_url, repo_dir],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise HTTPException(status_code=400, detail=f"Git clone failed: {result.stderr}")

    # Initialize CSV if empty
    current_csv = f"{repo_dir}/current.csv"
    if not os.path.exists(current_csv):
        write_csv(current_csv, 0, 0, "initializing",
                  datetime.now(TZ_BKK).strftime("%Y-%m-%dT%H:%M:%S+07:00"), "", "")
        subprocess.run(["git", "-C", repo_dir, "add", "current.csv"])
        subprocess.run(["git", "-C", repo_dir, "commit", "-m", "init: location tracking"])
        subprocess.run(["git", "-C", repo_dir, "push", remote_url, "HEAD:main"])

    # Add to users.json
    config[body.username] = {
        "repo_dir": repo_dir,
        "github_repo": body.github_repo,
        "github_token": body.github_token,
        "http_password": body.password,
        "named_places": []
    }
    save_config(config)

    print(f"[registered] {body.username} → {body.github_repo}")
    return {
        "status": "ok",
        "username": body.username,
        "github_repo": f"https://github.com/{body.github_repo}",
        "owntracks": {
            "mode": "HTTP",
            "url": "https://location.athena-oracle.site/pub",
            "username": body.username,
            "password": body.password,
            "tls": True
        }
    }


@app.post("/pub")
async def receive_location(request: Request, username: str = Depends(verify_credentials)):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if data.get("_type") != "location":
        return {}

    config = load_config()
    user_cfg = config[username]

    lat = data.get("lat")
    lon = data.get("lon")
    batt = data.get("batt", "")
    acc = data.get("acc", "")
    # Extract device from topic field: owntracks/{username}/{deviceid}
    topic = data.get("topic", "")
    topic_parts = topic.split("/")
    device = topic_parts[2] if len(topic_parts) >= 3 else "phone"
    ts = datetime.fromtimestamp(data.get("tst", 0), tz=TZ_BKK).strftime("%Y-%m-%dT%H:%M:%S+07:00")

    named_places = user_cfg.get("named_places", [])
    address = resolve_address(lat, lon, named_places)

    repo_dir = user_cfg["repo_dir"]
    github_repo = user_cfg["github_repo"]
    github_token = user_cfg.get("github_token", "")

    write_csv(f"{repo_dir}/current.csv", lat, lon, address, ts, batt, acc, device)
    append_history(f"{repo_dir}/history.csv", lat, lon, address, ts, batt, acc, device)

    print(f"[{username}] {lat},{lon} acc={acc}m — {address[:50] if address else 'unknown'}")
    git_push(repo_dir, github_repo, github_token, lat, lon)

    return {}


@app.get("/health")
def health():
    return {"status": "ok"}
