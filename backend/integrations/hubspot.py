# hubspot.py

import os
import json
import secrets
import urllib.parse
from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse
import httpx
import asyncio
import base64
import requests
from dotenv import load_dotenv

from integrations.integration_item import IntegrationItem
from redis_client import add_key_value_redis, get_value_redis, delete_key_redis

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv('HUBSPOT_CLIENT_ID')
CLIENT_SECRET = os.getenv('HUBSPOT_CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/integrations/hubspot/oauth2callback'

authorization_url = 'https://app.hubspot.com/oauth/authorize'
token_url = 'https://api.hubapi.com/oauth/v1/token'

scopes = (
    'contacts crm.objects.contacts.read crm.objects.companies.read '
    'crm.objects.deals.read crm.objects.line_items.read '
    'crm.objects.quotes.read crm.objects.tasks.read crm.objects.calls.read '
    'crm.objects.meetings.read crm.objects.notes.read crm.objects.emails.read'
)

async def authorize_hubspot(user_id, org_id):
    state_data = {
        'state': secrets.token_urlsafe(32),
        'user_id': user_id,
        'org_id': org_id
    }
    encoded_state = json.dumps(state_data)
    await add_key_value_redis(f'hubspot_state:{org_id}:{user_id}', encoded_state, expire=600)

    # Properly URL encode parameters
    params = {
        'client_id': CLIENT_ID,
        'redirect_uri': REDIRECT_URI,
        'scope': scopes,
        'state': encoded_state
    }
    
    auth_url = f"{authorization_url}?{urllib.parse.urlencode(params)}"
    return auth_url

async def oauth2callback_hubspot(request: Request):
    if request.query_params.get('error'):
        raise HTTPException(status_code=400, detail=request.query_params.get('error'))
    
    code = request.query_params.get('code')
    encoded_state = request.query_params.get('state')
    state_data = json.loads(encoded_state)

    original_state = state_data.get('state')
    user_id = state_data.get('user_id')
    org_id = state_data.get('org_id')

    saved_state = await get_value_redis(f'hubspot_state:{org_id}:{user_id}')

    if not saved_state or original_state != json.loads(saved_state).get('state'):
        raise HTTPException(status_code=400, detail='State does not match.')

    async with httpx.AsyncClient() as client:
        response, _ = await asyncio.gather(
            client.post(
                token_url,
                data={
                    'grant_type': 'authorization_code',
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'redirect_uri': REDIRECT_URI,
                    'code': code
                },
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            ),
            delete_key_redis(f'hubspot_state:{org_id}:{user_id}'),
        )

    if response.status_code != 200:
        raise HTTPException(status_code=400, detail='Failed to exchange code for token')

    await add_key_value_redis(f'hubspot_credentials:{org_id}:{user_id}', json.dumps(response.json()), expire=600)
    
    close_window_script = """
    <html>
        <script>
            window.close();
        </script>
    </html>
    """
    return HTMLResponse(content=close_window_script)

async def get_hubspot_credentials(user_id, org_id):
    credentials = await get_value_redis(f'hubspot_credentials:{org_id}:{user_id}')
    if not credentials:
        raise HTTPException(status_code=400, detail='No credentials found.')
    credentials = json.loads(credentials)
    if not credentials:
        raise HTTPException(status_code=400, detail='No credentials found.')
    await delete_key_redis(f'hubspot_credentials:{org_id}:{user_id}')

    return credentials

def create_integration_item_metadata_object(
    response_json: dict, item_type: str, parent_id=None, parent_name=None
) -> IntegrationItem:
    """Creates an integration metadata object from the HubSpot API response"""
    
    # Extract name based on object type
    name = None
    if item_type == 'contact':
        # For contacts, use firstname + lastname or email as fallback
        properties = response_json.get('properties', {})
        firstname = properties.get('firstname', '')
        lastname = properties.get('lastname', '')
        email = properties.get('email', '')
        
        if firstname or lastname:
            name = f"{firstname} {lastname}".strip()
        elif email:
            name = email
        else:
            name = f"Contact {response_json.get('id', 'Unknown')}"
            
    elif item_type == 'company':
        properties = response_json.get('properties', {})
        name = properties.get('name') or properties.get('domain') or f"Company {response_json.get('id', 'Unknown')}"
        
    elif item_type == 'deal':
        properties = response_json.get('properties', {})
        name = properties.get('dealname') or f"Deal {response_json.get('id', 'Unknown')}"
        
    else:
        # Generic fallback for other object types
        properties = response_json.get('properties', {})
        name = properties.get('name') or properties.get('subject') or f"{item_type.title()} {response_json.get('id', 'Unknown')}"

    # Extract timestamps
    created_time = response_json.get('createdAt') or response_json.get('properties', {}).get('createdate')
    modified_time = response_json.get('updatedAt') or response_json.get('properties', {}).get('lastmodifieddate')

    integration_item_metadata = IntegrationItem(
        id=f"{response_json.get('id')}_{item_type}",
        type=item_type,
        name=name,
        creation_time=created_time,
        last_modified_time=modified_time,
        parent_id=parent_id,
        parent_path_or_name=parent_name,
    )

    return integration_item_metadata

def fetch_hubspot_objects(access_token: str, object_type: str, limit=100) -> list:
    """Fetch objects from HubSpot CRM API"""
    url = f'https://api.hubapi.com/crm/v3/objects/{object_type}'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    all_results = []
    after = None
    
    while True:
        params = {'limit': limit}
        if after:
            params['after'] = after
            
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching {object_type}: {response.status_code} - {response.text}")
            break
            
        data = response.json()
        results = data.get('results', [])
        all_results.extend(results)
        
        # Check for pagination
        paging = data.get('paging', {})
        after = paging.get('next', {}).get('after')
        
        if not after:
            break
    
    return all_results

async def get_items_hubspot(credentials) -> list[IntegrationItem]:
    """Aggregates all metadata relevant for a HubSpot integration"""
    credentials = json.loads(credentials)
    access_token = credentials.get('access_token')
    
    if not access_token:
        raise HTTPException(status_code=400, detail='No access token found in credentials')
    
    list_of_integration_item_metadata = []
    
    # Define the HubSpot object types to fetch
    object_types = ['contacts', 'companies', 'deals', 'tickets', 'tasks', 'calls', 'meetings', 'notes', 'emails']
    
    for object_type in object_types:
        try:
            objects = fetch_hubspot_objects(access_token, object_type)
            
            for obj in objects:
                integration_item = create_integration_item_metadata_object(
                    obj, 
                    object_type.rstrip('s')  # Remove plural 's' for item type
                )
                list_of_integration_item_metadata.append(integration_item)
                
        except Exception as e:
            print(f"Error processing {object_type}: {str(e)}")
            continue
    
    print(f'HubSpot integration items count: {len(list_of_integration_item_metadata)}')
    print(f'HubSpot integration items: {list_of_integration_item_metadata}')
    return list_of_integration_item_metadata