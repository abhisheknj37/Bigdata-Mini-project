#!/usr/bin/env python3
"""
================================================================================
Script Name : generate_sample_data.py
Description : Production-grade Synthetic Data Generator for Media & Entertainment
              Data Warehousing on Hadoop (HDFS / Hive / Sqoop / Pig / MapReduce).
              
Entities Generated (CSV staging files):
  1. staging_subscription_plans.csv - Subscription tier master metadata
  2. staging_devices.csv            - Device hardware & client app catalog
  3. staging_content.csv            - Media catalog (Movies, TV Series, Episodes)
  4. staging_users.csv              - Subscriber demographic & profile records
  5. staging_viewing_activity.csv   - Video player playback telemetry & QoS logs
  6. staging_user_interactions.csv  - User ratings, likes, watchlists & shares
  7. staging_subscription_billing.csv - Monthly recurring billing transactions

Features:
  - Strong referential integrity and realistic cross-entity correlations
  - Realistic viewing behavior (sessionization, completion %, buffer events, bitrates)
  - Temporal distribution with peak-hour viewing simulation
  - Reproducible with fixed random seeds
  - Fallback logic: works cleanly using standard library random, while supporting
    Faker if available.
================================================================================
"""

import argparse
import csv
import datetime
import math
import os
import random
import sys
from typing import Dict, List, Optional, Tuple

# Optional Faker import with graceful fallback
try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False


# ==============================================================================
# Domain Catalogs & Reference Data
# ==============================================================================

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen", "Stephen", "Anna",
    "Larry", "Brenda", "Justin", "Pamela", "Scott", "Nicole", "Brandon", "Emma",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Gregory", "Christine", "Frank", "Debra",
    "Alexander", "Rachel", "Raymond", "Catherine", "Patrick", "Carolyn", "Jack", "Janet",
    "Dennis", "Ruth", "Jerry", "Maria", "Tyler", "Heather", "Aaron", "Diane",
    "Jose", "Virginia", "Adam", "Julie", "Henry", "Joyce", "Nathan", "Victoria",
    "Douglas", "Olivia", "Zachary", "Kelly", "Peter", "Christina", "Kyle", "Lauren"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales",
    "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
    "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
    "Long", "Ross", "Foster", "Jimenez", "Powell", "Jenkins", "Perry", "Russell"
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "icloud.com", "protonmail.com",
    "comcast.net", "verizon.net", "aol.com", "mail.com", "zoho.com"
]

COUNTRIES = [
    ("US", "United States", 0.40),
    ("GB", "United Kingdom", 0.12),
    ("CA", "Canada", 0.08),
    ("DE", "Germany", 0.07),
    ("FR", "France", 0.06),
    ("AU", "Australia", 0.05),
    ("JP", "Japan", 0.05),
    ("BR", "Brazil", 0.05),
    ("IN", "India", 0.07),
    ("MX", "Mexico", 0.05),
]

AGE_GROUPS = [
    ("18-24", 0.18),
    ("25-34", 0.35),
    ("35-44", 0.24),
    ("45-54", 0.14),
    ("55+", 0.09)
]

MEDIA_TITLES = [
    # Movies (Title, Genre, Release Year, Duration Sec, Rating, Studio, Language)
    ("Interstellar Odyssey", "Sci-Fi", 2021, 8820, "PG-13", "Paramount Pictures", "English"),
    ("The Crimson Shadow", "Thriller", 2020, 7200, "R", "Warner Bros", "English"),
    ("Quantum Paradox", "Sci-Fi", 2022, 7920, "PG-13", "Universal Studios", "English"),
    ("Midnight in Kyoto", "Drama", 2019, 6480, "PG", "Sony Pictures", "Japanese"),
    ("Laugh Out Loud: Reloaded", "Comedy", 2023, 5880, "PG-13", "Netflix Studios", "English"),
    ("Echoes of the Serengeti", "Documentary", 2020, 5400, "G", "National Geographic", "English"),
    ("Apex Velocity", "Action", 2022, 7560, "PG-13", "Universal Studios", "English"),
    ("Whispers in the Mist", "Horror", 2021, 6120, "R", "Blumhouse", "English"),
    ("The Clockwork Kingdom", "Animation", 2023, 6180, "PG", "Pixar Animation", "English"),
    ("Secret Agent: Protocol Zero", "Action", 2021, 7680, "PG-13", "Metro-Goldwyn-Mayer", "English"),
    ("Parisian Romance", "Romance", 2018, 6600, "PG-13", "Canal+", "French"),
    ("The Deep Blue Abyss", "Documentary", 2022, 5700, "G", "BBC Earth", "English"),
    ("Cyberpunk 2099: Neon City", "Sci-Fi", 2023, 8100, "R", "HBO Films", "English"),
    ("Dragonheart's Legacy", "Fantasy", 2019, 8400, "PG-13", "Warner Bros", "English"),
    ("The Wall Street Heist", "Crime", 2022, 7380, "R", "A24", "English"),
    ("Under the Northern Lights", "Drama", 2021, 6720, "PG", "Nordic Film", "Swedish"),
    ("Formula Velocity: Championship", "Documentary", 2023, 6300, "PG-13", "Box to Box Films", "English"),
    ("Ghost of the Samurai", "Action", 2020, 7860, "R", "Toho Pictures", "Japanese"),
    ("The Bavarian Alps Mystery", "Mystery", 2019, 6960, "PG-13", "Constantin Film", "German"),
    ("Eternal Horizon", "Sci-Fi", 2024, 8520, "PG-13", "Paramount Pictures", "English"),
    ("Culinary Masters: Tokyo", "Documentary", 2021, 5100, "G", "NHK", "Japanese"),
    ("The Jazz Cellar", "Drama", 2017, 7080, "PG-13", "Lionsgate", "English"),
    ("The Last Gladiator", "Action", 2018, 9300, "R", "DreamWorks", "English"),
    ("Valley of the Pharaohs", "Documentary", 2022, 5580, "PG", "Discovery", "English"),
    ("Silicon Valley Hustle", "Comedy", 2023, 6360, "R", "Sony Pictures", "English")
]

TV_SERIES_CATALOG = [
    # (Series Title, Genre, Start Year, Rating, Studio, Seasons, EpisodesPerSeason, EpDurationSec)
    ("Crown of Thorns", "Fantasy", 2019, "TV-MA", "HBO Entertainment", 4, 10, 3600),
    ("Stranger Realities", "Sci-Fi", 2020, "TV-14", "Netflix Studios", 3, 8, 3300),
    ("The Code Matrix", "Thriller", 2021, "TV-MA", "Amazon Studios", 2, 10, 3100),
    ("Chicago Detectives", "Crime", 2018, "TV-14", "NBCUniversal", 5, 12, 2700),
    ("Corporate Ladder", "Comedy", 2020, "TV-14", "Paramount Television", 3, 10, 1800),
    ("Planet Cosmos", "Documentary", 2022, "TV-G", "BBC Natural History", 2, 6, 3000),
    ("Dark Nebula", "Sci-Fi", 2021, "TV-MA", "Apple Studios", 2, 8, 3400),
    ("The Royal Dynasty", "Drama", 2017, "TV-MA", "Left Bank Pictures", 4, 10, 3600),
    ("Cyber Hackers", "Action", 2022, "TV-MA", "Sony Television", 2, 8, 3200),
    ("Hospital Central", "Drama", 2016, "TV-14", "ABC Signature", 6, 16, 2600)
]

DEVICE_TEMPLATES = [
    {"type": "Smart TV", "brand": "Samsung", "os": "Tizen OS", "os_ver": "7.0", "app_ver": "5.4.1"},
    {"type": "Smart TV", "brand": "LG", "os": "webOS", "os_ver": "23.1", "app_ver": "5.3.8"},
    {"type": "Smart TV", "brand": "Sony", "os": "Android TV", "os_ver": "12.0", "app_ver": "5.4.0"},
    {"type": "Streaming Stick", "brand": "Apple", "os": "tvOS", "os_ver": "17.4", "app_ver": "5.4.2"},
    {"type": "Streaming Stick", "brand": "Roku", "os": "Roku OS", "os_ver": "12.5", "app_ver": "5.2.9"},
    {"type": "Streaming Stick", "brand": "Amazon", "os": "Fire OS", "os_ver": "8.1", "app_ver": "5.3.4"},
    {"type": "Mobile", "brand": "Apple", "os": "iOS", "os_ver": "17.4", "app_ver": "5.4.2"},
    {"type": "Mobile", "brand": "Samsung", "os": "Android", "os_ver": "14.0", "app_ver": "5.4.1"},
    {"type": "Mobile", "brand": "Google", "os": "Android", "os_ver": "14.0", "app_ver": "5.4.1"},
    {"type": "Tablet", "brand": "Apple", "os": "iPadOS", "os_ver": "17.4", "app_ver": "5.4.2"},
    {"type": "Tablet", "brand": "Samsung", "os": "Android", "os_ver": "13.0", "app_ver": "5.3.7"},
    {"type": "Web Browser", "brand": "Google Chrome", "os": "Windows 11", "os_ver": "123.0", "app_ver": "Web-3.2"},
    {"type": "Web Browser", "brand": "Apple Safari", "os": "macOS Sonoma", "os_ver": "17.2", "app_ver": "Web-3.2"},
    {"type": "Web Browser", "brand": "Mozilla Firefox", "os": "Ubuntu 22.04", "os_ver": "124.0", "app_ver": "Web-3.1"},
    {"type": "Gaming Console", "brand": "Sony PlayStation 5", "os": "PS5 OS", "os_ver": "8.2", "app_ver": "5.1.0"},
    {"type": "Gaming Console", "brand": "Microsoft Xbox Series X", "os": "Xbox OS", "os_ver": "10.0", "app_ver": "5.1.0"}
]

SUBSCRIPTION_PLANS = [
    {
        "plan_id": 1,
        "plan_name": "Basic Ad-Supported",
        "monthly_price_usd": 6.99,
        "max_streams": 1,
        "video_quality": "HD (720p)",
        "is_ad_supported": "Y"
    },
    {
        "plan_id": 2,
        "plan_name": "Standard Full HD",
        "monthly_price_usd": 15.49,
        "max_streams": 2,
        "video_quality": "Full HD (1080p)",
        "is_ad_supported": "N"
    },
    {
        "plan_id": 3,
        "plan_name": "Premium 4K Ultra HD",
        "monthly_price_usd": 22.99,
        "max_streams": 4,
        "video_quality": "4K Ultra HD",
        "is_ad_supported": "N"
    },
    {
        "plan_id": 4,
        "plan_name": "Family Premium Bundle",
        "monthly_price_usd": 27.99,
        "max_streams": 6,
        "video_quality": "4K Ultra HD + Atmos",
        "is_ad_supported": "N"
    }
]

RESOLUTION_PROFILES = {
    "4K Ultra HD": {"min_bitrate": 15000, "max_bitrate": 25000, "res_str": "3840x2160"},
    "4K Ultra HD + Atmos": {"min_bitrate": 18000, "max_bitrate": 28000, "res_str": "3840x2160"},
    "Full HD (1080p)": {"min_bitrate": 4500, "max_bitrate": 8000, "res_str": "1920x1080"},
    "HD (720p)": {"min_bitrate": 2000, "max_bitrate": 4000, "res_str": "1280x720"}
}


# ==============================================================================
# Generator Engine Class
# ==============================================================================

class MediaDataGenerator:
    """Enterprise synthetic data generator for Media DW."""

    def __init__(self, output_dir: str, seed: int = 42, base_records: int = 10000, days: int = 7):
        self.output_dir = output_dir
        self.seed = seed
        self.base_records = base_records
        self.days = days
        self.end_date = datetime.datetime.now(datetime.timezone.utc).date()
        self.start_date = self.end_date - datetime.timedelta(days=self.days)
        
        # Seed random generators
        random.seed(self.seed)
        self.faker = None
        if FAKER_AVAILABLE:
            self.faker = Faker()
            Faker.seed(self.seed)

        # In-memory storage for relational referential integrity
        self.plans: List[Dict] = []
        self.devices: List[Dict] = []
        self.content_catalog: List[Dict] = []
        self.users: List[Dict] = []
        
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"[INFO] Initialized MediaDataGenerator: Seed={seed}, OutputDir={output_dir}, Days={days}")

    def _weighted_choice(self, choices_with_weights):
        items = [x[0] for x in choices_with_weights]
        weights = [x[-1] for x in choices_with_weights]
        return random.choices(items, weights=weights, k=1)[0]

    # --------------------------------------------------------------------------
    # 1. Generate Subscription Plans (Master Dimension)
    # --------------------------------------------------------------------------
    def generate_subscription_plans(self) -> str:
        filepath = os.path.join(self.output_dir, "staging_subscription_plans.csv")
        headers = [
            "plan_id", "plan_name", "monthly_price_usd", "max_streams",
            "video_quality", "is_ad_supported"
        ]
        self.plans = SUBSCRIPTION_PLANS

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for plan in self.plans:
                writer.writerow(plan)

        print(f"[SUCCESS] Generated {len(self.plans)} subscription plans -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 2. Generate Devices (Master Dimension)
    # --------------------------------------------------------------------------
    def generate_devices(self) -> str:
        filepath = os.path.join(self.output_dir, "staging_devices.csv")
        headers = [
            "device_id", "device_type", "brand", "os_name", "os_version", "app_version"
        ]
        self.devices = []

        device_id = 1
        for template in DEVICE_TEMPLATES:
            device = {
                "device_id": device_id,
                "device_type": template["type"],
                "brand": template["brand"],
                "os_name": template["os"],
                "os_version": template["os_ver"],
                "app_version": template["app_ver"]
            }
            self.devices.append(device)
            device_id += 1

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for dev in self.devices:
                writer.writerow(dev)

        print(f"[SUCCESS] Generated {len(self.devices)} devices -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 3. Generate Media Content Catalog (Movies & TV Episodes)
    # --------------------------------------------------------------------------
    def generate_content_catalog(self, target_count: Optional[int] = None) -> str:
        filepath = os.path.join(self.output_dir, "staging_content.csv")
        headers = [
            "content_id", "title", "content_type", "genre", "release_year",
            "duration_sec", "content_rating", "studio", "language",
            "series_title", "season_num", "episode_num"
        ]
        self.content_catalog = []
        content_id = 10001

        # A. Insert Standalone Movies
        for movie in MEDIA_TITLES:
            title, genre, year, dur, rating, studio, lang = movie
            self.content_catalog.append({
                "content_id": content_id,
                "title": title,
                "content_type": "Movie",
                "genre": genre,
                "release_year": year,
                "duration_sec": dur,
                "content_rating": rating,
                "studio": studio,
                "language": lang,
                "series_title": "\\N",
                "season_num": "\\N",
                "episode_num": "\\N"
            })
            content_id += 1

        # B. Insert TV Series Episodes
        for tv in TV_SERIES_CATALOG:
            s_title, genre, start_year, rating, studio, seasons, eps_per_season, ep_dur = tv
            for s in range(1, seasons + 1):
                for ep in range(1, eps_per_season + 1):
                    ep_title = f"{s_title} S{s:02d}E{ep:02d}"
                    year = start_year + (s - 1)
                    # Add small variance in episode duration (±5 mins)
                    actual_dur = ep_dur + random.randint(-300, 300)
                    self.content_catalog.append({
                        "content_id": content_id,
                        "title": ep_title,
                        "content_type": "Episode",
                        "genre": genre,
                        "release_year": year,
                        "duration_sec": actual_dur,
                        "content_rating": rating,
                        "studio": studio,
                        "language": "English",
                        "series_title": s_title,
                        "season_num": s,
                        "episode_num": ep
                    })
                    content_id += 1

        # C. Expand if target count requires more titles
        needed = (target_count or max(1000, self.base_records // 5)) - len(self.content_catalog)
        if needed > 0:
            genres = ["Action", "Sci-Fi", "Drama", "Comedy", "Thriller", "Horror", "Documentary", "Animation"]
            studios = ["Warner Bros", "Universal", "Paramount", "Sony", "Netflix", "A24", "MGM", "Lionsgate"]
            ratings = ["G", "PG", "PG-13", "R", "TV-14", "TV-MA"]
            for i in range(needed):
                genre = random.choice(genres)
                c_type = random.choice(["Movie", "Episode"])
                dur = random.randint(4800, 9600) if c_type == "Movie" else random.randint(1800, 3900)
                year = random.randint(2010, 2024)
                title = f"Media Title Alpha {content_id}"
                self.content_catalog.append({
                    "content_id": content_id,
                    "title": title,
                    "content_type": c_type,
                    "genre": genre,
                    "release_year": year,
                    "duration_sec": dur,
                    "content_rating": random.choice(ratings),
                    "studio": random.choice(studios),
                    "language": "English",
                    "series_title": f"Series {content_id % 50}" if c_type == "Episode" else "\\N",
                    "season_num": random.randint(1, 5) if c_type == "Episode" else "\\N",
                    "episode_num": random.randint(1, 12) if c_type == "Episode" else "\\N"
                })
                content_id += 1

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for c in self.content_catalog:
                writer.writerow(c)

        print(f"[SUCCESS] Generated {len(self.content_catalog)} content items -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 4. Generate Users / Subscribers
    # --------------------------------------------------------------------------
    def generate_users(self, count: Optional[int] = None) -> str:
        filepath = os.path.join(self.output_dir, "staging_users.csv")
        headers = [
            "user_id", "username", "email", "first_name", "last_name",
            "country", "age_group", "subscription_plan_id", "registration_date",
            "status", "updated_at"
        ]
        user_count = count or self.base_records
        self.users = []

        plan_ids = [p["plan_id"] for p in self.plans]
        plan_weights = [0.25, 0.45, 0.20, 0.10] # Standard Full HD is most popular

        for uid in range(1, user_count + 1):
            fn = random.choice(FIRST_NAMES)
            ln = random.choice(LAST_NAMES)
            domain = random.choice(EMAIL_DOMAINS)
            username = f"{fn.lower()}_{ln.lower()}_{uid % 997}"
            email = f"{username}@{domain}"
            
            country_code = self._weighted_choice(COUNTRIES)
            age = self._weighted_choice(AGE_GROUPS)
            plan_id = random.choices(plan_ids, weights=plan_weights, k=1)[0]
            
            # Registration within past 4 years
            days_ago = random.randint(10, 1460)
            reg_date = self.end_date - datetime.timedelta(days=days_ago)
            status = random.choices(["ACTIVE", "SUSPENDED", "CANCELLED"], weights=[0.92, 0.03, 0.05], k=1)[0]
            
            user = {
                "user_id": uid,
                "username": username,
                "email": email,
                "first_name": fn,
                "last_name": ln,
                "country": country_code,
                "age_group": age,
                "subscription_plan_id": plan_id,
                "registration_date": reg_date.strftime("%Y-%m-%d"),
                "status": status,
                "updated_at": f"{reg_date.strftime('%Y-%m-%d')} 12:00:00"
            }
            self.users.append(user)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for u in self.users:
                writer.writerow(u)

        print(f"[SUCCESS] Generated {len(self.users)} subscribers -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 5. Generate Viewing Activity (Telemetry / QoS Fact Data)
    # --------------------------------------------------------------------------
    def generate_viewing_activity(self, count: Optional[int] = None) -> str:
        filepath = os.path.join(self.output_dir, "staging_viewing_activity.csv")
        headers = [
            "viewing_id", "session_id", "user_id", "content_id", "device_id",
            "plan_id", "stream_start_time", "stream_end_time", "watch_duration_sec",
            "completion_pct", "bitrate_kbps", "buffer_count", "buffer_duration_sec",
            "pause_count", "seek_count", "error_code", "resolution", "is_completed"
        ]
        viewing_count = count or self.base_records

        # Map plan metadata for quick lookup
        plan_dict = {p["plan_id"]: p for p in self.plans}
        active_users = [u for u in self.users if u["status"] == "ACTIVE"] or self.users
        device_ids = [d["device_id"] for d in self.devices]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for vid in range(1, viewing_count + 1):
                session_id = f"sess_{vid:08d}"
                user = random.choice(active_users)
                content = random.choice(self.content_catalog)
                device_id = random.choice(device_ids)
                plan = plan_dict[user["subscription_plan_id"]]

                # Temporal distribution: Day within the window, skewed towards evening
                day_offset = random.randint(0, self.days - 1)
                event_date = self.start_date + datetime.timedelta(days=day_offset)
                
                # Peak hours: 19:00 - 23:00 (70% probability)
                if random.random() < 0.70:
                    start_hour = random.randint(19, 23)
                else:
                    start_hour = random.randint(6, 18)
                
                start_min = random.randint(0, 59)
                start_sec = random.randint(0, 59)
                start_dt = datetime.datetime.combine(event_date, datetime.time(start_hour, start_min, start_sec))

                # Playback duration behavior:
                content_len = int(content["duration_sec"])
                behavior = random.choices(["BOUNCED", "PARTIAL", "COMPLETED"], weights=[0.12, 0.28, 0.60], k=1)[0]

                if behavior == "BOUNCED":
                    # Dropped off in first 2-10 minutes
                    watch_sec = min(random.randint(120, 600), content_len)
                    is_completed = "N"
                elif behavior == "PARTIAL":
                    # Watched 30% to 80%
                    pct = random.uniform(0.30, 0.85)
                    watch_sec = int(content_len * pct)
                    is_completed = "N"
                else: # COMPLETED
                    watch_sec = content_len
                    is_completed = "Y"

                completion_pct = round((watch_sec / content_len) * 100.0, 2)
                end_dt = start_dt + datetime.timedelta(seconds=watch_sec)

                # Quality of Service (QoS) & Telemetry
                quality_tier = plan["video_quality"]
                profile = RESOLUTION_PROFILES.get(quality_tier, RESOLUTION_PROFILES["Full HD (1080p)"])
                bitrate = random.randint(profile["min_bitrate"], profile["max_bitrate"])
                res_str = profile["res_str"]

                # Network buffering: 90% smooth, 10% buffering
                if random.random() < 0.90:
                    buf_count = 0
                    buf_sec = 0
                else:
                    buf_count = random.randint(1, 5)
                    buf_sec = random.randint(3, 45)

                pause_count = random.randint(0, 4) if watch_sec > 1800 else random.randint(0, 1)
                seek_count = random.randint(0, 3)

                # Error Code: 98% NONE, 2% error
                if random.random() < 0.98:
                    err_code = "NONE"
                else:
                    err_code = random.choice(["ERR_NET_TIMEOUT", "ERR_DRM_FAIL", "ERR_BUFFER_STALL", "ERR_CDN_503"])

                writer.writerow({
                    "viewing_id": vid,
                    "session_id": session_id,
                    "user_id": user["user_id"],
                    "content_id": content["content_id"],
                    "device_id": device_id,
                    "plan_id": plan["plan_id"],
                    "stream_start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "stream_end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "watch_duration_sec": watch_sec,
                    "completion_pct": completion_pct,
                    "bitrate_kbps": bitrate,
                    "buffer_count": buf_count,
                    "buffer_duration_sec": buf_sec,
                    "pause_count": pause_count,
                    "seek_count": seek_count,
                    "error_code": err_code,
                    "resolution": res_str,
                    "is_completed": is_completed
                })

        print(f"[SUCCESS] Generated {viewing_count} viewing telemetry logs -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 6. Generate User Interactions (Likes, Ratings, Watchlists)
    # --------------------------------------------------------------------------
    def generate_user_interactions(self, count: Optional[int] = None) -> str:
        filepath = os.path.join(self.output_dir, "staging_user_interactions.csv")
        headers = [
            "interaction_id", "user_id", "content_id", "device_id",
            "interaction_type", "rating_value", "interaction_time"
        ]
        interaction_count = count or self.base_records

        active_users = [u for u in self.users if u["status"] == "ACTIVE"] or self.users
        device_ids = [d["device_id"] for d in self.devices]
        interaction_types = ["RATING", "LIKE", "DISLIKE", "WATCHLIST_ADD", "WATCHLIST_REMOVE", "SHARE"]
        type_weights = [0.35, 0.25, 0.05, 0.20, 0.05, 0.10]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for iid in range(1, interaction_count + 1):
                user = random.choice(active_users)
                content = random.choice(self.content_catalog)
                dev_id = random.choice(device_ids)
                itype = random.choices(interaction_types, weights=type_weights, k=1)[0]

                rating_val = "\\N"
                if itype == "RATING":
                    # Rating 1 to 5 stars, skewed towards 4 and 5
                    rating_val = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.17, 0.38, 0.32], k=1)[0]
                elif itype == "LIKE":
                    rating_val = 5
                elif itype == "DISLIKE":
                    rating_val = 1

                day_offset = random.randint(0, self.days - 1)
                event_date = self.start_date + datetime.timedelta(days=day_offset)
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                sec = random.randint(0, 59)
                event_dt = datetime.datetime.combine(event_date, datetime.time(hour, minute, sec))

                writer.writerow({
                    "interaction_id": iid,
                    "user_id": user["user_id"],
                    "content_id": content["content_id"],
                    "device_id": dev_id,
                    "interaction_type": itype,
                    "rating_value": rating_val,
                    "interaction_time": event_dt.strftime("%Y-%m-%d %H:%M:%S")
                })

        print(f"[SUCCESS] Generated {interaction_count} user interactions -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # 7. Generate Subscription Billing Transactions
    # --------------------------------------------------------------------------
    def generate_subscription_billing(self, count: Optional[int] = None) -> str:
        filepath = os.path.join(self.output_dir, "staging_subscription_billing.csv")
        headers = [
            "transaction_id", "user_id", "plan_id", "billing_date",
            "amount_billed_usd", "discount_usd", "tax_usd", "payment_method",
            "payment_status"
        ]
        tx_count = count or self.base_records
        plan_dict = {p["plan_id"]: p for p in self.plans}
        payment_methods = ["CREDIT_CARD", "PAYPAL", "DEBIT_CARD", "APPLE_PAY", "GOOGLE_PAY"]
        method_weights = [0.55, 0.20, 0.10, 0.10, 0.05]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for tid in range(1, tx_count + 1):
                user = random.choice(self.users)
                plan = plan_dict[user["subscription_plan_id"]]
                base_price = float(plan["monthly_price_usd"])

                # Occasional promo discount
                discount = round(random.choice([0.0, 0.0, 0.0, 2.0, 5.0]), 2)
                net = max(0.0, base_price - discount)
                tax = round(net * 0.0825, 2) # ~8.25% state/local tax
                total_billed = round(net + tax, 2)

                day_offset = random.randint(0, self.days - 1)
                bdate = self.start_date + datetime.timedelta(days=day_offset)

                method = random.choices(payment_methods, weights=method_weights, k=1)[0]
                status = random.choices(["PAID", "FAILED", "REFUNDED"], weights=[0.96, 0.03, 0.01], k=1)[0]

                writer.writerow({
                    "transaction_id": tid,
                    "user_id": user["user_id"],
                    "plan_id": plan["plan_id"],
                    "billing_date": bdate.strftime("%Y-%m-%d"),
                    "amount_billed_usd": total_billed,
                    "discount_usd": discount,
                    "tax_usd": tax,
                    "payment_method": method,
                    "payment_status": status
                })

        print(f"[SUCCESS] Generated {tx_count} billing transactions -> {filepath}")
        return filepath

    # --------------------------------------------------------------------------
    # Orchestrate All Generations
    # --------------------------------------------------------------------------
    def generate_all(self):
        print("==================================================================")
        print("Starting Full Media & Entertainment Synthetic Data Generation")
        print("==================================================================")
        self.generate_subscription_plans()
        self.generate_devices()
        self.generate_content_catalog()
        self.generate_users()
        self.generate_viewing_activity()
        self.generate_user_interactions()
        self.generate_subscription_billing()
        print("==================================================================")
        print(f"Data Generation Finished! All files available at: {self.output_dir}")
        print("==================================================================")


# ==============================================================================
# CLI Entry Point
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Enterprise Synthetic Data Generator for Media & Entertainment DW on Hadoop."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/staging",
        help="Target directory to save generated CSV files (default: ./data/staging)"
    )
    parser.add_argument(
        "--records",
        type=int,
        default=10000,
        help="Default volume of records per entity (default: 10000)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic generation (default: 42)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of historical days to simulate for viewing activity (default: 7)"
    )

    args = parser.parse_args()

    # Convert relative path to absolute
    abs_output_dir = os.path.abspath(args.output_dir)

    generator = MediaDataGenerator(
        output_dir=abs_output_dir,
        seed=args.seed,
        base_records=args.records,
        days=args.days
    )
    generator.generate_all()


if __name__ == "__main__":
    main()
