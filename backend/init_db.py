"""Utility to initialize the SQL schema (Supabase MySQL)."""
import asyncio
import os
import sys

from dotenv import load_dotenv

ROOT = os.path.dirname(__file__)
load_dotenv(os.path.join(ROOT, ".env"))

from .db import init_db, USE_SQL


def main():
    if not USE_SQL:
        print("No MYSQL_URL or SUPABASE_MYSQL_URL set in env; aborting.")
        sys.exit(1)
    asyncio.run(init_db())
    print("DB init complete")


if __name__ == "__main__":
    main()
