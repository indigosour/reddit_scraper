import os,json,emoji,re,logging
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.DEBUG)

## Common functions

def load_sublist():
    global sublist_value
    if os.path.exists("sublist.json"):
        try:
            with open('sublist.json', 'r') as f:
                data = json.load(f)
                return data     
        except Exception as e:
            print("Error loading sublist.json",e)
    else:
        print("sublist.json does not exist")


def cleanString(sourcestring):
    text_ascii = emoji.demojize(sourcestring) if sourcestring else ""
    pattern = r"[%:/,.\"\\[\]<>*\?]"
    text_without_emoji = re.sub(pattern, '', text_ascii) if text_ascii else ""
    return text_without_emoji


def get_az_secret(key_name):
    try:
        az_tenant_id = os.getenv('AZURE_TENANT_ID')
        az_client_id = os.getenv('AZURE_CLIENT_ID')
        az_client_secret = os.getenv('AZURE_CLIENT_SECRET')

        az_credential = ClientSecretCredential(az_tenant_id, az_client_id, az_client_secret)
        vault_url = os.getenv('AZURE_VAULT_URL')
        az_client = SecretClient(vault_url=vault_url, credential=az_credential)
        secret_value = az_client.get_secret(key_name)

        logging.info(f"get_az_secret: Retrieved secret '{key_name}' from Azure Key Vault.")
        return json.loads(secret_value.value)

    except Exception as e:
        logging.error(f"get_az_secret: Error retrieving secret '{key_name}' from Azure Key Vault: {e}")
        raise