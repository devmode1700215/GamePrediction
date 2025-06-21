# prompt.txt

- Refactor betting prediction structure in prompt.txt to focus solely on the Over/Under 2.5 Goals market, removing references to Full-Time Result and Both Teams To Score. Simplified prediction format and adjusted related fields accordingly.

# supabaseClient.py

- Adding validation for required environment variables and error handling for connection attempts.

# main.py

- Add logging, try catch to the main flow
- Add function: safe_extract_match_data to safely extract and validate match data with comprehensive error handling for nested structures (fixture, teams, league, venue)
- Add try catch logic to function update_results_for_finished_matches
- Add try catch to save_match_json, save_prediction_json

# test_system.py

- Run this file before main.py to test the system connection first, the logic will test: environment variables, file structure, prompt.txt validation, database connection, Football API connection, OpenAI API connection, and data validation functions

# get_football_data.py

- Add validation logic

# get_prediction.py

- Add validation logic

# verify_predictions_for_fixture.py

- Change insert function to upsert function

# fetch_and_store_result.py

- Update fetch_and_store_result.py to use upsert for result storage and add main execution block for testing.
