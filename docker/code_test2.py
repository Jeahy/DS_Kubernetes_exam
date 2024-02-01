import os
import requests

# Get API address and port from environment variables or use defaults
api_address = os.environ.get('API_ADDRESS', 'api')
api_port = os.environ.get('API_PORT', '8000')


user_data = [
    {'username': 'alice',
    'password': 'wonderland'},
    {'username': 'bob',
    'password': 'builder'},
    {'username': 'clementine',
    'password': 'mandarine'}
    ]
sentence = 'this is a test sentence'
# Make the request to the API

for user in user_data:
    for version in ['v1', 'v2']:
        endpoint = f'/{version}/sentiment'
        url = f'http://{api_address}:{api_port}{endpoint}'
        params = {'username': user['username'], 'password': user['password'], 'sentence': sentence}

        # Make the request to the API
        r = requests.get(url, params=params)

        status_code = r.status_code
        # Determine test status based on the HTTP status code
        if status_code == 200:
            test_status = 'SUCCESS'
        else:
            test_status = 'FAILURE'

        # Output results
        output = f'''
        ============================
            Authorization test
        ============================
        request done at "{endpoint}"
        {user['username']}
        expected result = 200
        actual result = {status_code}
        ==>  {test_status}
        '''

        print(output)

    # Print to a file if LOG environment variable is set to '1'
        if os.environ.get('LOG') == '1':
            with open('/logs/api_test.log', 'a') as file:
                file.write(output)

