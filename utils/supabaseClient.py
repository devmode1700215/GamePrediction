from supabase import create_client, Client
import os
import sys
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Missing required environment variables SUPABASE_URL or SUPABASE_KEY")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    # Test connection
    supabase.table("matches").select("*").limit(1).execute()
    print("✅ Supabase connection successful")
except Exception as e:
    print(f"❌ ERROR: Failed to connect to Supabase: {e}")
    sys.exit(1)