import os
import requests

# Get API address and port from environment variables or use defaults
api_address = os.environ.get('API_ADDRESS', 'api')
api_port = os.environ.get('API_PORT', '8000')

sentences = ['life is beautiful', 'that sucks']
# Make the request to the API

for s in sentences:
    for version in ['v1', 'v2']:
        endpoint = f'/{version}/sentiment'
        url = f'http://{api_address}:{api_port}{endpoint}'
        params = {'username':'alice', 'password':'wonderland', 'sentence':s}

        # Make the request to the API
        r = requests.get(url, params=params)

        score_result = r.json().get('score', 'N/A')

        # Determine test status based on the HTTP status code
        if score_result > 0:
            score = 'POSITIVE'
        else:
            score = 'NEGATIVE'

        # Output results
        output = f'''
        ============================
            Content test
        ============================
        request done at "{endpoint}"
        {r.json()}
        ==>  sentiment score {score}
        '''

        print(output)

    # Print to a file if LOG environment variable is set to '1'
        if os.environ.get('LOG') == '1':
            with open('/logs/api_test.log', 'a') as file:
                file.write(output)

