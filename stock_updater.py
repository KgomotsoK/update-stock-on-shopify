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