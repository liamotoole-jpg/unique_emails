import os
import requests
import pandas as pd
from io import StringIO
import logging
from flask import Flask, render_template, request
from dotenv import load_dotenv

# Set up logging for Render visibility
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

# CLIENT_MAP: Groups multiple project lists under a single client ID
CLIENT_MAP = {
    'johnson': [
        {'name': 'Mike Johnson for Louisiana', 'api_key': os.getenv('MJ_LA_API_KEY'), 'list_id': os.getenv('MJ_LA_LIST_ID')},
        {'name': 'Mike Johnson for Louisiana NY', 'api_key': os.getenv('MJ_NY_API_KEY'), 'list_id': os.getenv('MJ_NY_LIST_ID')}
    ],
    'whatley': [
        {'name': 'Whatley for Senate', 'api_key': os.getenv('WHATLEY_API_KEY'), 'list_id': os.getenv('WHATLEY_LIST_ID')}
    ],
    'britt': [
        {'name': 'Britt for Alabama', 'api_key': os.getenv('BRITT_API_KEY'), 'list_id': os.getenv('BRITT_LIST_ID')}
    ],
    'rogers': [
        {'name': 'Rogers for Senate', 'api_key': os.getenv('ROGERS_API_KEY'), 'list_id': os.getenv('ROGERS_LIST_ID')}
    ],
    'hilton': [
        {'name': 'Steve Hilton for Governor 2026', 'api_key': os.getenv('HILTON_API_KEY'), 'list_id': os.getenv('HILTON_LIST_ID')}
    ]
}

@app.route('/', methods=['GET', 'POST'])
def index():
    result = None
    if request.method == 'POST':
        client_key = request.form.get('client')
        projects = CLIENT_MAP.get(client_key, [])
        file = request.files.get('file')

        if not file or not projects:
            return "Error: Missing file or client selection.", 400

        try:
            # MEMORY OPTIMIZATION 1: Load ONLY required columns with specific types
            uploaded_df = pd.read_csv(
                file, 
                usecols=['email', 'unsubscribed', 'active_subscriber'],
                dtype={'unsubscribed': 'category', 'active_subscriber': 'category'}
            )
            
            # Filter based on your specific logic
            uploaded_df = uploaded_df[
                (uploaded_df['unsubscribed'] == 'no') & 
                (uploaded_df['active_subscriber'] == 'yes')
            ]
            
            # Remove empty emails and keep only the email column
            uploaded_emails = uploaded_df[['email']].dropna().drop_duplicates()
            all_dfs = [uploaded_emails]
            
            logger.info(f"Filtered Upload: {len(uploaded_emails)} valid emails.")

            # 2. API Fetching for all projects in the selected group
            total_api_count = 0
            url_base = 'https://api.iterable.com/api/lists/getUsers?listId='
            
            for p in projects:
                if p['api_key'] and p['list_id']:
                    try:
                        headers = {'Api-Key': p['api_key']}
                        # Set timeout to prevent one slow list from hanging the whole app
                        resp = requests.get(url_base + p['list_id'], headers=headers, timeout=60)
                        
                        if resp.status_code == 200 and resp.text.strip():
                            # API data only has 1 column (email), no header
                            temp_df = pd.read_csv(StringIO(resp.text), header=None, names=['email'])
                            temp_df = temp_df.dropna().drop_duplicates()
                            total_api_count += len(temp_df)
                            all_dfs.append(temp_df)
                    except Exception as e:
                        logger.error(f"Failed API call for {p['name']}: {e}")

            # 3. Final Consolidation and Deduplication
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df.drop_duplicates(subset='email', inplace=True)
            
            result = {
                'client_name': projects[0]['name'].split(" for ")[0],
                'total_unique': len(final_df),
                'uploaded_count': len(uploaded_emails),
                'api_count': total_api_count
            }

        except Exception as e:
            logger.error(f"Processing Error: {str(e)}")
            return f"An internal error occurred: {str(e)}", 500

    return render_template('index.html', result=result)

if __name__ == "__main__":
    app.run()
