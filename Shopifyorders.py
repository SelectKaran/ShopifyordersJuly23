import requests
import datetime
import time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
def extract_date(date_str):
    if date_str is None:
        return ""
    try:
        datetime_obj = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
        formatted_date = datetime_obj.strftime("%d/%m/%Y")
    except ValueError:
        formatted_date = ""
    return formatted_date
def extract_payment_type(payment_str):
    if 'Cash on Delivery (COD)' in payment_str:
        return 'COD'
    elif 'Razorpay Secure' in payment_str:
        return 'prepaid'
    else:
        return ''
def get_shopify(day=30):
    api_key = "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"
    store_name = "kyari-co"
    api_version = "2023-04"
    orders = []
    last_order_id = 2500
    is_last_page = False
    created_at_min = (datetime.datetime.now() - datetime.timedelta(days=day)).strftime("%Y-%m-%d")
    while not is_last_page:
        endpoint = f"/admin/api/{api_version}/orders.json?status=any"
        api_url = f"https://{store_name}.myshopify.com{endpoint}&created_at_min={created_at_min}&limit=250&since_id={last_order_id}"

        headers = {
            "X-Shopify-Access-Token": api_key,
            "Accept": "application/json",
        }

        try:
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            orders_on_page = json_data["orders"]

            if len(orders_on_page) == 0:
                is_last_page = True
            else:
                orders.extend(orders_on_page)
                last_order_id = orders_on_page[-1]["id"]

        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"An error occurred: {e}")
        time.sleep(0.5)

    return orders
print("API Calling Started.......")
orders = get_shopify(60)
print("Data is Prepared")
name = []
sku = []
quantity = []
rn = []
id_list=[]
w=[]
pr=[]
for i in orders:
    id=i["id"]
    order_id = i["name"]  # Get the Order-ID for the current order
    for x in i["line_items"]:
        id_list.append(id)
        rn.append(order_id)    # Append the Order-ID for each line item
        name.append(x.get("name", None))
        sku.append(x.get("sku", None))
        quantity.append(x.get("quantity", None))
        w.append(x.get("grams",None))
        pr.append(x.get("price",None))
data_dict = {"ID":id_list,"Order-ID": rn, "Name": name, "Child SKU": sku, "quantity": quantity,"Weight":w,"Item-Price":pr}
df2 = pd.DataFrame(data_dict)
new_list = []
for dictionary in orders:
    discount_codes = dictionary.get("discount_codes", [])
    if discount_codes:
        code = discount_codes[0].get("code", "")
        amount = discount_codes[0].get("amount", "")
        discount_type = discount_codes[0].get("type", "")
    else:
        code = ""
        amount = ""
        discount_type = ""
    cust_info=dictionary.get("shipping_address", [])
    if cust_info:
        first_name = cust_info.get("first_name", "")
        last_name = cust_info.get("last_name", "")
        pincode = cust_info.get("zip", "")
        contact = cust_info.get("phone", "")
        state=cust_info.get('province',"")
    else:
        first_name = ""
        last_name = ""
        pincode = ""
        contact=''
        state=''
    awb=dictionary.get("fulfillments", [])
    if awb:
        awd_dict=awb[0].get("tracking_number",'')
    else:
        awd_dict=""
    new_dict = {
        "Order-ID": dictionary["name"],"First-Name":first_name,"Last-Name":last_name,'State':state,"Pincode":pincode,"Contact Number":contact,"created_at": dictionary["created_at"],
        "financial_status": dictionary["financial_status"],"fulfillment_status": dictionary["fulfillment_status"],"Final Price": dictionary['total_price'],\
        "discount_codes": code,"Discount_Amount": amount,"Discount_Type": discount_type,"PaymentMode":dictionary['payment_gateway_names'],"AWB":awd_dict
    }
    titles = []
    for application in dictionary.get("discount_applications", []):
            if "title" in application:
                titles.append(application["title"])
    new_dict["check_replacement"] = "; ".join(titles)

    new_list.append(new_dict)
df = pd.DataFrame(new_list)
df['payment_type'] = df['PaymentMode'].apply(extract_payment_type)
df['Order Date']=df["created_at"].apply(extract_date)
df['Customer Name']=df['First-Name']+" "+df['Last-Name']
final=df[[ 'Order Date','Order-ID',"Customer Name", 'State','Pincode', 'Contact Number','financial_status', 'fulfillment_status', \
          'Final Price','discount_codes', 'Discount_Amount', 'Discount_Type','AWB', 'check_replacement', 'payment_type']].copy()
final['Contact Number'] = final['Contact Number'].str.replace(r'\+', '', regex=True).str.strip().astype(str)
final=final.merge(df2,on="Order-ID",how="left")
sort_final=final[['Order Date','ID','Order-ID', 'Customer Name', 'State', 'Pincode','Contact Number', 'financial_status', \
                  'fulfillment_status','Final Price', 'discount_codes', 'Discount_Amount', 'Discount_Type','AWB', 'check_replacement',\
                  'payment_type', 'Name', 'Child SKU','quantity', 'Weight', 'Item-Price']].copy()
print("Ready to save Data...")
sorted_final=sort_final[((final["financial_status"]=="paid")|(final["financial_status"]=="pending")|(final["financial_status"]=="partially_refunded"))&((final["fulfillment_status"]!="restocked"))].copy()
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./emerald-cab-384306-b8566336d0b0.json', scope)
client = gspread.authorize(creds)
skugs=client.open('Select._Master_Data_2023')
skuworksheet=skugs.worksheet('sku_parent')
sku_record=skuworksheet.get_all_records()
sku_df=pd.DataFrame(sku_record)
sorted_sku=sku_df[["Child SKU","Parent Category"]].copy()
final_merge=sorted_final.merge(sorted_sku,on="Child SKU",how="left").drop_duplicates(ignore_index=True)
gs= client.open('Shopify_orders_tracking_July23')
gs.timeout =120
sheetb2c=gs.worksheet('Raw')
sheetb2c.clear()
set_with_dataframe(sheetb2c,final_merge)
print("Data is exported successfully...")
