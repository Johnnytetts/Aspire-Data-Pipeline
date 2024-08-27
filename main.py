import requests
import pandas as pd
import os
import time
import winreg

def get_registry_env_variable(variable_name):
    """Retrieve an environment variable from the Windows registry."""
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment") as key:
            value, _ = winreg.QueryValueEx(key, variable_name)
            return value
    except FileNotFoundError:
        return None

base_url = get_registry_env_variable('BASE_URL')
auth_token = get_registry_env_variable('AUTH_TOKEN')

headers = {
    'Authorization': f'Bearer {auth_token}'
}

endpoints_with_ids = {
    "Activities": "ActivityID",
    "ActivityCategories": "ActivityCategoryID",
    "Addresses": "AddressID",
    "Attachments": "AttachmentID",
    "BankDeposits": "BankDepositID",
    "Branches": "BranchID",
    "Budget": "BudgetID",
    "CatalogItemCategories": "CatalogItemCategoryID",
    "CatalogItems": "CatalogItemID",
    "ClockTimes": "ClockTimeID",
    "Companies": "CompanyID",
    "Contacts": "ContactID",
    "ContactTypes": "ContactTypeID",
    "Divisions": "DivisionID",
    "EquipmentClasses": "EquipmentClassID",
    "EquipmentManufacturers": "EquipmentManufacturerID",
    "EquipmentModels": "EquipmentModelID",
    "InventoryLocations": "InventoryLocationID",
    "InvoiceBatches": "InvoiceBatchID",
    "InvoiceRevenues": "InvoiceRevenueID",
    "Invoices": "InvoiceID",
    "InvoiceTaxes": "InvoiceTaxID",
    "ItemAllocations": "ItemAllocationID",
    "Opportunities": "OpportunityID",
    "OpportunityServiceItems": "OpportunityServiceItemID",
    "OpportunityServices": "OpportunityServiceID",
    "PayCodes": "PayCodeID",
    "PaymentCategories": "PaymentCategoryID",
    "Payments": "PaymentID",
    "PaymentTerms": "PaymentTermID",
    "PayRateOverridePayCodes": "PayRateOverridePayCodeID",
    "PayRates": "PayRateID",
    "PaySchedules": "PayScheduleID",
    "Properties": "PropertyID",
    "PropertyContacts": "PropertyContactID",
    "PropertyCustomFieldDefinitions": "PropertyCustomFieldDefinitionID",
    "PropertyCustomFields": "PropertyCustomFieldID",
    "PropertyGroups": "PropertyGroupID",
    "PropertyStatuses": "PropertyStatusID",
    "ProspectRatings": "ProspectRatingID",
    "Receipts": "ReceiptID",
    "ReceiptStatuses": "ReceiptStatusID",
    "RevenueVariances": "RevenueVarianceID",
    "Routes": "RouteID",
    "SalesTypes": "SalesTypeID",
    "Services": "ServiceID",
    "ServiceTypes": "ServiceTypeID",
    "Tags": "TagID",
    "TakeoffGroups": "TakeoffGroupID",
    "TakeoffItems": "TakeoffItemID",
    "TaxEntities": "TaxEntityID",
    "TaxJurisdictions": "TaxJurisdictionID",
    "UnitTypes": "UnitTypeID",
    "Vendors": "VendorID",
    "WorkTicketItems": "WorkTicketItemID",
    "WorkTicketRevenues": "WorkTicketRevenueID",
    "WorkTickets": "WorkTicketID",
    "WorkTicketTimes": "WorkTicketTimeID",
    "WorkTicketVisitNotes": "WorkTicketVisitNoteID",
    "WorkTicketVisits": "WorkTicketVisitID",
    "OpportunityServiceGroups": "OpportunityServiceGroupID",
    "OpportunityServiceKitItems": "OpportunityServiceKitItemID",
    "Users": "UserID"
}

data_stage_dir = "E:\\data stage"
os.makedirs(data_stage_dir, exist_ok=True)

def fetch_data(endpoint):
    skip = 0
    top = 1000
    all_data = []

    while True:
        url = f"{base_url}{endpoint}?$top={top}&$skip={skip}"
        print(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} for endpoint {endpoint}")
            print(f"Response content: {response.text}")  # Debugging line
            break

        data = response.json()

        if isinstance(data, list):
            records = data
        else:
            records = data.get('value', [])

        if not records:
            break

        all_data.extend(records)
        skip += top

        time.sleep(5)  # Adjust if needed

    return all_data

def save_data_to_csv(data, endpoint):
    if not data:
        print(f"No data to save for {endpoint}.")
        return
    
    df = pd.DataFrame(data)
    
    filename = f"{data_stage_dir}\\{endpoint.lower()}.csv"
    df.to_csv(filename, index=False)
    print(f"Captured {len(df)} rows for {endpoint}, saved to {filename}")

start_time = time.time()

for endpoint in endpoints_with_ids:
    data = fetch_data(endpoint)
    if data:
        save_data_to_csv(data, endpoint)
    else:
        print(f"No data found for {endpoint}.")

    print(f"Moving to next endpoint...")
    time.sleep(2)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total time elapsed: {elapsed_time / 60: .2f} minutes ({elapsed_time: .2f} seconds)")
