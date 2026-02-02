import os
import requests
import pandas as pd
from io import StringIO
import logging
from flask import Flask, render_template, request
from dotenv import load_dotenv

# Set up logging to see errors in Render "Logs" tab
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

CLIENT_MAP = {
    'johnson': [
        {'project': 'Mike Johnson for Louisiana', 'api_key': os.getenv('MJ_LA_API_KEY'), 'list_id': os.getenv('MJ_LA_LIST_ID')},
        {'project': 'Mike Johnson for Louisiana NY', 'api_key': os.getenv('MJ_NY_API_KEY'), 'list_id':os.getenv('MJ_NY_LIST_ID')}
    ],
    'whatley': [
        {'project': 'Whatley for Senate', 'api_key': os.getenv('WHATLEY_API_KEY'), 'list_id': os.getenv('WHATLEY_LIST_ID')}
    ],
    'britt': [
        {'project': 'Britt for Alabama', 'api_key': os.getenv('BRITT_API_KEY'), 'list_id': os.getenv('BRITT_LIST_ID')}
    ],
    'rogers': [
        {'project': 'Rogers for Senate', 'api_key': os.getenv('ROGERS_API_KEY'), 'list_id': os.getenv('ROGERS_LIST_ID')}
    ],
    'hilton': [
        {'project': 'Steve Hilton for Governor 2026', 'api_key': os.getenv('HILTON_API_KEY'), 'list_id': os.getenv('HILTON_LIST_ID')}
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
            return "Error: Please select a client and upload a CSV.", 400

        try:
            # MEMORY FIX: Read file stream directly instead of loading into string
            uploaded_df = pd.read_csv(file)
            
            # 1. Clean and filter uploaded data
            # Ensure columns match your file: 'email', 'unsubscribed', 'active_subscriber'
            uploaded_df = uploaded_df[(uploaded_df['unsubscribed'] == 'no') & 
                                      (uploaded_df['active_subscriber'] == 'yes')]
            
            # Remove rows where email is empty
            uploaded_emails = uploaded_df[['email']].dropna()
            all_dfs = [uploaded_emails]
            
            logger.info(f"Processed upload: {len(uploaded_emails)} valid emails found.")

            # 2. Process API Projects
            total_api_emails = 0
            url_base = 'https://api.iterable.com/api/lists/getUsers?listId='
            
            for p in projects:
                if p['api_key'] and p['list_id']:
                    headers = {'Api-Key': p['api_key']}
                    resp = requests.get(url_base + p['list_id'], headers=headers)
                    if resp.status_code == 200:
                        # Handle potential empty responses from API
                        if resp.text.strip():
                            temp = pd.read_csv(StringIO(resp.text), header=None)
                            temp.rename({0: 'email'}, inplace=True, axis=1)
                            temp = temp[['email']].dropna()
                            total_api_emails += len(temp)
                            all_dfs.append(temp)
                        else:
                            logger.warning(f"API returned empty list for {p['project']}")

            # 3. Consolidate and De-duplicate
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df.drop_duplicates(subset='email', inplace=True, ignore_index=True)
            
            result = {
                'client_name': projects[0]['project'].split(" for ")[0],
                'total_unique': len(final_df),
                'uploaded_count': len(uploaded_emails),
                'api_count': total_api_emails
            }
            logger.info(f"Success: {result['total_unique']} unique emails found.")

        except Exception as e:
            logger.error(f"Critical Error: {str(e)}")
            return f"An error occurred: {str(e)}", 500

    return render_template('index.html', result=result)

if __name__ == "__main__":
    app.run(debug=True)
