#!/usr/bin/env python3
"""
Test suite for the football prediction system
Run this before executing main.py to ensure everything is working correctly
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

def test_environment_variables():
    """Test that all required environment variables are set"""
    print("🧪 Testing environment variables...")
    
    load_dotenv()
    required_vars = {
        'OPENAI_KEY': 'OpenAI API key',
        'FOOTBALL_API_KEY': 'Football API key',
        'SUPABASE_URL': 'Supabase URL',
        'SUPABASE_KEY': 'Supabase key'
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        if not os.getenv(var):
            missing_vars.append(f"{var} ({description})")
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ All environment variables are set")
    return True


def test_file_structure():
    """Test that all required files exist"""
    print("🧪 Testing file structure...")
    
    required_files = [
        'main.py',
        'prompt.txt',
        'requirements.txt',
        'utils/supabaseClient.py',
        'utils/get_prediction.py',
        'utils/get_football_data.py',
        'utils/safe_get.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        return False
    
    print("✅ All required files exist")
    return True

def test_prompt_file():
    """Test that prompt.txt is valid"""
    print("🧪 Testing prompt file...")
    
    try:
        with open('prompt.txt', 'r', encoding='utf-8') as f:
            prompt = f.read()
        
        if not prompt.strip():
            print("❌ prompt.txt is empty")
            return False
        
        # Check for essential prompt components
        required_components = [
            'over_2_5',
            'confidence',
            'fixture_id',
            'predictions'
        ]
        
        missing_components = []
        for component in required_components:
            if component not in prompt.lower():
                missing_components.append(component)
        
        if missing_components:
            print(f"❌ Prompt missing components: {', '.join(missing_components)}")
            return False
        
        print("✅ Prompt file is valid")
        return True
        
    except Exception as e:
        print(f"❌ Error reading prompt.txt: {e}")
        return False

def test_database_connection():
    """Test Supabase database connection"""
    print("🧪 Testing database connection...")
    
    try:
        from utils.supabaseClient import supabase
        
        # Test connection with a simple query
        result = supabase.table("matches").select("*").limit(1).execute()
        print("✅ Database connection successful")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_api_connection():
    """Test Football API connection"""
    print("🧪 Testing Football API connection...")
    
    try:
        from utils.safe_get import safe_get
        
        load_dotenv()
        api_key = os.getenv("FOOTBALL_API_KEY")
        
        if not api_key:
            print("❌ FOOTBALL_API_KEY not found")
            return False
        
        headers = {'x-apisports-key': api_key}
        url = "https://v3.football.api-sports.io/timezone"
        
        response = safe_get(url, headers=headers)
        
        if response and response.status_code == 200:
            print("✅ Football API connection successful")
            return True
        else:
            print("❌ Football API connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Football API: {e}")
        return False

def test_openai_connection():
    """Test OpenAI API connection"""
    print("🧪 Testing OpenAI API connection...")
    
    try:
        from openai import OpenAI
        
        load_dotenv()
        api_key = os.getenv("OPENAI_KEY")
        
        if not api_key:
            print("❌ OPENAI_KEY not found")
            return False
        
        client = OpenAI(api_key=api_key)
        
        # Test with a simple completion
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[{"role": "user", "content": "Hello, respond with 'OK'"}],
            max_tokens=5,
            temperature=0
        )
        
        if response and response.choices:
            print("✅ OpenAI API connection successful")
            return True
        else:
            print("❌ OpenAI API connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing OpenAI API: {e}")
        return False

def test_data_validation():
    """Test data validation functions"""
    print("🧪 Testing data validation...")
    
    try:
        # Test with sample data
        sample_match_data = {
            "fixture_id": 12345,
            "home_team": {"id": 1, "name": "Team A"},
            "away_team": {"id": 2, "name": "Team B"},
            "odds": {"over_2_5": 1.8, "under_2_5": 2.0}
        }
        
        from utils.get_prediction import validate_match_data
        is_valid, error = validate_match_data(sample_match_data)
        
        if not is_valid:
            print(f"❌ Data validation failed: {error}")
            return False
        
        print("✅ Data validation working correctly")
        return True
        
    except Exception as e:
        print(f"❌ Error testing data validation: {e}")
        return False

def run_all_tests():
    """Run all tests and return overall status"""
    print("🚀 Starting system tests...\n")
    
    tests = [
        test_environment_variables,
        test_file_structure,
        test_prompt_file,
        test_database_connection,
        test_api_connection,
        test_openai_connection,
        test_data_validation
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            print()
        except Exception as e:
            print(f"❌ Test failed with exception: {e}\n")
            results.append(False)
    
    passed = sum(results)
    total = len(results)
    
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! System is ready to run.")
        return True
    else:
        print("⚠️ Some tests failed. Please fix the issues before running the system.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1) 