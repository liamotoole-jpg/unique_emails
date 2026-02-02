import os
import requests
import pandas as pd
from io import StringIO
from flask import Flask, render_template, request
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# Mapped from your original list
# Clients with multiple projects are stored as a list of dictionaries
CLIENT_MAP = {
    'johnson': [
        {'project': 'Mike Johnson for Louisiana', 'api_key': os.getenv('MJ_LA_API_KEY'), 'list_id': os.getenv('MJ_LA_LIST_ID')},
        {'project': 'Mike Johnson for Louisiana NY', 'api_key': os.getenv('MJ_NY_API_KEY'), os.getenv('MJ_NY_LIST_ID')}
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

        # 1. Process Uploaded CSV
        csv_data = file.read().decode('utf-8')
        uploaded_df = pd.read_csv(StringIO(csv_data))
        # Match your logic: unsubscribed == no AND active_subscriber == yes
        uploaded_df = uploaded_df[(uploaded_df['unsubscribed'] == 'no') & 
                                  (uploaded_df['active_subscriber'] == 'yes')]
        all_dfs = [uploaded_df[['email']]]

        # 2. Process All Associated Projects for that Client
        total_api_emails = 0
        url_base = 'https://api.iterable.com/api/lists/getUsers?listId='
        
        for p in projects:
            if p['api_key'] and p['list_id']:
                headers = {'Api-Key': p['api_key']}
                resp = requests.get(url_base + p['list_id'], headers=headers)
                if resp.status_code == 200:
                    temp = pd.read_csv(StringIO(resp.content.decode('utf-8')), header=None)
                    temp.rename({0: 'email'}, inplace=True, axis=1)
                    total_api_emails += len(temp)
                    all_dfs.append(temp)

        # 3. Consolidate and De-duplicate
        final_df = pd.concat(all_dfs)
        final_df.drop_duplicates(subset='email', inplace=True, ignore_index=True)
        
        result = {
            'client_name': projects[0]['project'].split(" for ")[0], # Pretty name
            'total_unique': len(final_df),
            'uploaded_count': len(uploaded_df),
            'api_count': total_api_emails
        }

    return render_template('index.html', result=result)

if __name__ == "__main__":
    app.run(debug=True)
