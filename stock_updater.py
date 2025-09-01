'''
import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import json
import pandas as pd
from ftplib import FTP
import io
from dotenv import load_dotenv

load_dotenv()

# Shopify Configuration
SHOP_NAME = os.getenv("SHOP_NAME")
API_VERSION = os.getenv("API_VERSION")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
SHOPIFY_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"


# Configure logging
logging.basicConfig(
handlers=[RotatingFileHandler('app.log', maxBytes=100000, backupCount=10)],
level=logging.INFO,
format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# FTP Configuration
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FILE_PATH = os.getenv("FTP_FILE_PATH")

def download_excel_from_ftp():
    """Download Excel file from FTP and load into DataFrame"""
    try:
        ftp = FTP(FTP_HOST)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        with io.BytesIO() as file_buffer:
            ftp.retrbinary(f"RETR {FTP_FILE_PATH}", file_buffer.write)
            file_buffer.seek(0)
            return pd.read_excel(file_buffer)
            
    except Exception as e:
        print(f"FTP Error: {str(e)}")
        return None
    finally:
        ftp.quit()

def map_excel_to_shopify(df):
    """Map Excel columns to Shopify fields"""
    if df is None:
        return None
        
    # Clean and map columns - adjust these based on your actual Excel structure
    mapped_data = []
    for _, row in df.iterrows():
        # Extract SKU (first part before space if "Code & Description" is combined)
        sku = str(row['Code & Description']).split()[0] if 'Code & Description' in row else None
        
        mapped_data.append({
            'sku': sku,
            'inventory_quantity': int(row['Balance']) if 'Balance' in row else 0
        })
    
    return mapped_data

def update_shopify_inventory(items):
    """Update inventory in Shopify using GraphQL"""
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": ACCESS_TOKEN
    }
    
    for item in items:
        # First get the inventory item ID
        variant_query = """
        query getVariant($sku: String!) {
            productVariants(first: 1, query: $sku) {
                edges {
                    node {
                        id
                        inventoryItem {
                            id
                        }
                    }
                }
            }
        }
        """
        
        # Get variant and inventory item IDs
        variant_response = requests.post(
            SHOPIFY_URL,
            headers=headers,
            json={
                "query": variant_query,
                "variables": {"sku": item['sku']}
            }
        )
        
        if variant_response.status_code != 200:
            print(f"Error fetching variant for SKU {item['sku']}")
            continue
            
        variant_data = variant_response.json()
        try:
            variant_id = variant_data['data']['productVariants']['edges'][0]['node']['id']
            inventory_item_id = variant_data['data']['productVariants']['edges'][0]['node']['inventoryItem']['id']
        except (KeyError, IndexError):
            print(f"Variant not found for SKU: {item['sku']}")
            continue
            
        # Update inventory mutation
        mutation = """
        mutation adjustInventory($input: InventoryAdjustQuantityInput!) {
            inventoryAdjustQuantity(input: $input) {
                inventoryLevel {
                    quantities(names: ["available"]) {
                        quantity
                    }
                }
            }
        }
        """
        
        variables = {
            "input": {
                "inventoryItemId": inventory_item_id,
                "availableDelta": item['inventory_quantity']
            }
        }
        
        update_response = requests.post(
            SHOPIFY_URL,
            headers=headers,
            json={
                "query": mutation,
                "variables": variables
            }
        )
        
        if update_response.status_code == 200:
            print(f"Successfully updated inventory for SKU: {item['sku']}")
        else:
            print(f"Failed to update SKU {item['sku']}: {update_response.text}")

def check_environment():
    required_vars = ['SHOP_NAME', 'ACCESS_TOKEN', 'FTP_PASS']
    missing = [var for var in required_vars if not os.getenv(var)]
    print(f"Missing environment variables: {', '.join(missing)}")
    if missing:
        logger.critical(f"Missing environment variables: {', '.join(missing)}")
        raise EnvironmentError("Missing required configuration")

def main():
    check_environment()
    # 1. Download Excel from FTP
    logger.info("Downloading inventory file from FTP...")
    excel_data = download_excel_from_ftp()
    
    if excel_data is None:
        logger.error("Failed to download Excel file")
        return
    
    # 2. Process and map data
    logger.info("Processing inventory data...")
    shopify_items = map_excel_to_shopify(excel_data)
    
    if not shopify_items:
        logger.warning("No valid inventory items found")
        return
    
    # 3. Update Shopify
    logger.info(f"Updating {len(shopify_items)} items in Shopify...")
    update_shopify_inventory(shopify_items)
    logger.info("Inventory update complete!")

if __name__ == "__main__":
    main()
'''
import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import json
import pandas as pd
from ftplib import FTP
import io
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    handlers=[RotatingFileHandler('app.log', maxBytes=100000, backupCount=10)],
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# FTP Configuration (shared across all stores)
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FILE_PATH = os.getenv("FTP_FILE_PATH")

# Store configurations - load from environment
STORES = []
store_index = 1
while True:
    shop_name = os.getenv(f"SHOP_NAME_{store_index}")
    access_token = os.getenv(f"ACCESS_TOKEN_{store_index}")
    api_version = os.getenv(f"API_VERSION_{store_index}", "2025-07")  # Default API version
    
    if not shop_name or not access_token:
        break
        
    STORES.append({
        'shop_name': shop_name,
        'access_token': access_token,
        'api_version': api_version,
        'url': f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    })
    store_index += 1

class ShopifyInventoryUpdater:
    def __init__(self, store_config):
        self.store_config = store_config
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": store_config['access_token']
        }
        
    def download_excel_from_ftp(self):
        """Download Excel file from FTP and load into DataFrame"""
        try:
            ftp = FTP(FTP_HOST)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            with io.BytesIO() as file_buffer:
                ftp.retrbinary(f"RETR {FTP_FILE_PATH}", file_buffer.write)
                file_buffer.seek(0)
                return pd.read_excel(file_buffer)
                
        except Exception as e:
            logger.error(f"FTP Error for store {self.store_config['shop_name']}: {str(e)}")
            return None
        finally:
            try:
                ftp.quit()
            except:
                pass

    def map_excel_to_shopify(self, df):
        """Map Excel columns to Shopify fields"""
        if df is None:
            return None
            
        mapped_data = []
        for _, row in df.iterrows():
            # Extract SKU (first part before space if "Code & Description" is combined)
            sku = str(row['Code & Description']).split()[0] if 'Code & Description' in row else None
            
            if sku and str(sku).lower() != 'nan':
                mapped_data.append({
                    'sku': sku,
                    'inventory_quantity': int(row['Balance']) if 'Balance' in row and pd.notna(row['Balance']) else 0
                })
        
        return mapped_data

    def get_variant_and_inventory_id(self, sku):
        """Get variant and inventory item IDs for a given SKU"""
        variant_query = """
        query getVariant($sku: String!) {
            productVariants(first: 1, query: $sku) {
                edges {
                    node {
                        id
                        inventoryItem {
                            id
                        }
                    }
                }
            }
        }
        """
        
        response = requests.post(
            self.store_config['url'],
            headers=self.headers,
            json={
                "query": variant_query,
                "variables": {"sku": sku}
            }
        )
        
        if response.status_code != 200:
            logger.warning(f"Error fetching variant for SKU {sku} in store {self.store_config['shop_name']}")
            return None, None
            
        try:
            variant_data = response.json()
            edge = variant_data['data']['productVariants']['edges'][0]
            variant_id = edge['node']['id']
            inventory_item_id = edge['node']['inventoryItem']['id']
            return variant_id, inventory_item_id
        except (KeyError, IndexError):
            logger.debug(f"Variant not found for SKU: {sku} in store {self.store_config['shop_name']}")
            return None, None

    def update_inventory_item(self, inventory_item_id, quantity_delta):
        """Update inventory for a specific inventory item"""
        mutation = """
        mutation adjustInventory($input: InventoryAdjustQuantityInput!) {
            inventoryAdjustQuantity(input: $input) {
                inventoryLevel {
                    quantities(names: ["available"]) {
                        quantity
                    }
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        variables = {
            "input": {
                "inventoryItemId": inventory_item_id,
                "availableDelta": quantity_delta
            }
        }
        
        response = requests.post(
            self.store_config['url'],
            headers=self.headers,
            json={
                "query": mutation,
                "variables": variables
            }
        )
        
        return response

    def update_shopify_inventory(self, items):
        """Update inventory in Shopify using GraphQL"""
        updated_count = 0
        skipped_count = 0
        
        logger.info(f"Processing {len(items)} items for store: {self.store_config['shop_name']}")
        
        for item in items:
            variant_id, inventory_item_id = self.get_variant_and_inventory_id(item['sku'])
            
            if not inventory_item_id:
                skipped_count += 1
                continue
            
            response = self.update_inventory_item(inventory_item_id, item['inventory_quantity'])
            
            if response.status_code == 200:
                result = response.json()
                if result.get('data', {}).get('inventoryAdjustQuantity', {}).get('userErrors'):
                    errors = result['data']['inventoryAdjustQuantity']['userErrors']
                    logger.warning(f"Errors updating SKU {item['sku']} in {self.store_config['shop_name']}: {errors}")
                    skipped_count += 1
                else:
                    logger.debug(f"Successfully updated inventory for SKU: {item['sku']} in {self.store_config['shop_name']}")
                    updated_count += 1
            else:
                logger.error(f"Failed to update SKU {item['sku']} in {self.store_config['shop_name']}: {response.text}")
                skipped_count += 1
        
        logger.info(f"Store {self.store_config['shop_name']}: Updated {updated_count} items, Skipped {skipped_count} items")
        return updated_count, skipped_count

def check_environment():
    """Check if required environment variables are set"""
    required_vars = ['FTP_HOST', 'FTP_USER', 'FTP_PASS', 'FTP_FILE_PATH']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logger.critical(f"Missing FTP environment variables: {', '.join(missing)}")
        raise EnvironmentError("Missing required FTP configuration")
    
    if not STORES:
        logger.critical("No Shopify stores configured. Please set SHOP_NAME_1, ACCESS_TOKEN_1, etc.")
        raise EnvironmentError("No Shopify stores configured")
    
    logger.info(f"Found {len(STORES)} store(s) configured")

def main():
    """Main function to process inventory updates for all stores"""
    try:
        check_environment()
        
        # Download inventory data once (shared across all stores)
        logger.info("Downloading inventory file from FTP...")
        
        # Use first store's updater just to download the file
        temp_updater = ShopifyInventoryUpdater(STORES[0])
        excel_data = temp_updater.download_excel_from_ftp()
        
        if excel_data is None:
            logger.error("Failed to download Excel file")
            return
        
        logger.info(f"Downloaded inventory file with {len(excel_data)} rows")
        
        # Process each store
        total_updated = 0
        total_skipped = 0
        
        for store_config in STORES:
            logger.info(f"\n--- Processing store: {store_config['shop_name']} ---")
            
            updater = ShopifyInventoryUpdater(store_config)
            
            # Process and map data for this store
            shopify_items = updater.map_excel_to_shopify(excel_data)
            
            if not shopify_items:
                logger.warning(f"No valid inventory items found for store {store_config['shop_name']}")
                continue
            
            # Update Shopify inventory for this store
            updated, skipped = updater.update_shopify_inventory(shopify_items)
            total_updated += updated
            total_skipped += skipped
        
        logger.info(f"\n=== SUMMARY ===")
        logger.info(f"Total items updated across all stores: {total_updated}")
        logger.info(f"Total items skipped across all stores: {total_skipped}")
        logger.info("Multi-store inventory update complete!")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
