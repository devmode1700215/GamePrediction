import json
import os
import logging
from openai import OpenAI
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
api_key = os.getenv("OPENAI_KEY")

# Validate API key
if not api_key:
    logger.error("❌ ERROR: Missing OPENAI_KEY environment variable")
    raise ValueError("OPENAI_KEY environment variable is required")

client = OpenAI(api_key=api_key)

# Load and validate prompt
try:
    with open("prompt.txt", "r", encoding="utf-8") as f:
        prompt = f.read()
    if not prompt.strip():
        raise ValueError("Prompt file is empty")
    logger.info("✅ Prompt loaded successfully")
except FileNotFoundError:
    logger.error("❌ ERROR: prompt.txt file not found")
    raise
except Exception as e:
    logger.error(f"❌ ERROR: Failed to load prompt.txt: {e}")
    raise

def validate_prediction_response(prediction_data):
    """Validate the structure of prediction response"""
    if not isinstance(prediction_data, dict):
        return False, "Response is not a valid JSON object"
    
    if "fixture_id" not in prediction_data:
        return False, "Missing fixture_id in response"
    
    predictions = prediction_data.get("predictions", {})
    if not predictions:
        return False, "Missing predictions object"
    
    over_2_5 = predictions.get("over_2_5", {})
    if not over_2_5:
        return False, "Missing over_2_5 prediction"
    
    required_prediction_fields = ["prediction", "confidence", "po_value", "odds"]
    missing_fields = [field for field in required_prediction_fields if field not in over_2_5]
    
    if missing_fields:
        return False, f"Missing prediction fields: {missing_fields}"
    
    return True, None

def get_prediction(match_data):
    """Get prediction from OpenAI with comprehensive error handling"""
    try:
        fixture_id = match_data.get("fixture_id")
        logger.info(f"🤖 Requesting prediction for fixture {fixture_id}")
        
        # Make API call with timeout and retry logic
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(match_data)}
            ],
            temperature=0.2,
            max_tokens=1000,
            timeout=30.0
        )
        
        if not response or not response.choices:
            logger.error(f"❌ Empty response from OpenAI API for fixture {fixture_id}")
            return None
        
        content = response.choices[0].message.content
        if not content:
            logger.error(f"❌ Empty content in API response for fixture {fixture_id}")
            return None
        
        logger.info(f"📝 Raw API response for {fixture_id}: {content[:200]}...")
        
        # Parse JSON with better error handling
        try:
            prediction_data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error for fixture {fixture_id}: {e}")
            logger.error(f"❌ Raw content: {content}")
            return None
        
        # Validate response structure
        is_valid, error_msg = validate_prediction_response(prediction_data)
        if not is_valid:
            logger.error(f"❌ Invalid prediction response for fixture {fixture_id}: {error_msg}")
            logger.error(f"❌ Response data: {prediction_data}")
            return None
        
        logger.info(f"✅ Successfully generated prediction for fixture {fixture_id}")
        return prediction_data
        
    except Exception as e:
        logger.error(f"❌ Unexpected error getting prediction for fixture {fixture_id if 'fixture_id' in locals() else 'unknown'}: {e}")
        return None
