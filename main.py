from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import time
import sqlite3


# List of Gumroad access tokens
access_tokens = [
    "gumroad_access_token_1",
    "gumroad_access_token_2",
    "gumroad_access_token_3",
    "gumroad_access_token_4",
    "gumroad_access_token_5"
]

# Connect to SQLite database (or create it)
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Create a table to store processed sales
cursor.execute('''CREATE TABLE IF NOT EXISTS processed_sales (
                    sale_id TEXT PRIMARY KEY,
                    cancelled BOOLEAN
                 )''')
conn.commit()
def serviceEnd(sale, token_generator, current_token):
    subscription_id = sale.get('subscription_id')

    while True:
        # Make the API request to get the subscriber details
        url = f"https://api.gumroad.com/v2/subscribers/{subscription_id}"
        params = {
            "access_token": current_token
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            # Parse the subscriber data from the response
            subscriber_data = response.json().get('subscriber')

            # Extract the ended_at field
            ended_at = subscriber_data.get('ended_at')

            # If the ended_at field is present, return the end date as a UNIX timestamp
            if ended_at:
                end_date = datetime.strptime(ended_at, "%Y-%m-%dT%H:%M:%SZ")
                return int(time.mktime(end_date.timetuple())),current_token
            
            cancelled_at = subscriber_data.get('cancelled_at')
            if cancelled_at:
                cancelled_at = datetime.strptime(cancelled_at, "%Y-%m-%dT%H:%M:%SZ")
                return int(time.mktime(cancelled_at.timetuple())),current_token
        elif response.status_code == 400:
            print("A required parameter is missing")
            return None,current_token
        elif response.status_code == 401:
            print("An invalid gumroad access token.")
            return None,current_token
        else : # Rate limit
            print("Rate limit reached. Switching tokens...")
            try:
                current_token = next(token_generator)
            except StopIteration:
                # All tokens exhausted
                print("All tokens exhausted. Please try again later.")
                return None, current_token


def map_plan_interval(gumroad_interval):
    interval_mapping = {
        'monthly': 'month',
        'quarterly': 'month', 
        'biannually': 'month',  
        'yearly': 'year',
        'every_two_years': 'year'  
    }
    return interval_mapping.get(gumroad_interval, 'month')  # Default to 'month' if no match

# Function to check if a sale exists in the database
def sale_exists(sale_id):
    cursor.execute("SELECT cancelled FROM processed_sales WHERE sale_id = ?", (sale_id,))
    return cursor.fetchone()

# Function to mark a sale as processed and store its cancellation status
def mark_sale_processed(sale_id, cancelled):
    cursor.execute("INSERT INTO processed_sales (sale_id, cancelled) VALUES (?, ?)", (sale_id, cancelled))
    conn.commit()

# Function to update the cancellation status of a sale
def update_sale_cancelled(sale_id, cancelled):
    cursor.execute("UPDATE processed_sales SET cancelled = ? WHERE sale_id = ?", (cancelled, sale_id))
    conn.commit()
# Function to delete a sale from the database
def delete_sale_from_db(sale_id):
    cursor.execute("DELETE FROM processed_sales WHERE sale_id = ?", (sale_id,))
    conn.commit()
    print(f"Deleted sale {sale_id} from the database.")

# Function to cancel a subscription in ProfitWell
def churn_subscription(subscription_alias, effective_date, churn_type):
    url = f'https://api.profitwell.com/v2/subscriptions/{subscription_alias}/?effective_date={effective_date}&churn_type={churn_type}'
    headers = {
        'Authorization': 'profitwell_private_key'
    }

    response = requests.delete(url, headers=headers)

    if response.status_code == 200:
        print(f"Subscription {subscription_alias} successfully churned as {churn_type}.")
    else:
        print(f"Error churning subscription {subscription_alias}: {response.status_code}")
        print(response.text)

    return response

# Function to post sales data to ProfitWell
def post_to_profitwell(sale):
    profitwell_url = "https://api.profitwell.com/v2/subscriptions/"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'profitwell_private_key'
    }
    # Map the plan interval using the function
    plan_interval = map_plan_interval(sale['subscription_duration'])
    # Prepare the data for ProfitWell
    values = {
        "user_alias": sale['email'],  # or use your own user_alias
        "subscription_alias": sale['subscription_id']+"7",  # Unique subscription alias
        "email": sale['email'],
        "plan_id": sale['product_id'],
        "plan_interval": plan_interval,
        "plan_currency": "usd",  # Adjust this if needed
        "status": "active" if sale["price"] > 0 else "trialing",
        "value": sale['price'],
        "effective_date": int(time.mktime(time.strptime(sale['created_at'], "%Y-%m-%dT%H:%M:%SZ")))  # Convert to UNIX timestamp
    }
    response = requests.post(profitwell_url, json=values, headers=headers)
    return response

# Function to get sales data from Gumroad using the current token
def get_sales_data(token_generator, current_token, page_key=None):
    url = "https://api.gumroad.com/v2/sales"
    params = {
        "access_token": current_token,
    }
    if page_key:
        params["page_key"] = page_key
    
    response = requests.get(url, params=params)
    if response.status_code == 200 :
        return response, current_token 
    elif response.status_code == 400:
            print("A required parameter is missing")
            return None,current_token
    elif response.status_code == 401:
        print("An invalid gumroad access token.")
        return None,current_token
    # If rate limit or other error, rotate tokens
    else:  # Rate limit
        print(f"Error fetching sales data: {response.status_code}")
        print("Rate limit reached. Switching tokens...")
        try:
            current_token = next(token_generator)  # Switch to next token
        except StopIteration:
            # All tokens exhausted
            print("All tokens exhausted. Please try again later.")
            
            return None, None
        return get_sales_data(token_generator, current_token, page_key)  # Retry with new token

    


# Function to rotate through access tokens
def rotate_tokens():
    for token in access_tokens:
        yield token

# Main function
def process_sales():
    token_generator = rotate_tokens()
    current_token = next(token_generator)
    page_key = None
    
    while True:
        try:
            # Fetch sales data from Gumroad
            response, current_token = get_sales_data(token_generator, current_token, page_key)  # Updated to handle token rotation
            if response is None:
                break  # Stop if token switching failed or all tokens are exhausted
            
            if response.status_code == 200:
                sales_data = response.json()['sales']
                next_page_key = response.json().get('next_page_key')

                # Process each sale
                for sale in sales_data:
                    if sale.get("subscription_id"):
                        sale_id = sale['id']
                        cancelled = sale.get('cancelled', False)  # Get the 'cancelled' field from the sale
                        ended = sale.get('ended', None)  # Get the 'ended' field from the sale
                        churn_type = None

                        # Determine the churn type and effective date
                        if cancelled:
                            churn_type = "voluntary"
                        if ended:
                            churn_type = "delinquent"
                        
                        if cancelled or ended:
                            effective_date, current_token = serviceEnd(sale, token_generator, current_token)  # Handle token switching within serviceEnd
                            if effective_date is None:
                                continue  

                        # Check if the sale already exists in the database
                        existing_sale = sale_exists(sale_id)
                        
                        # If the sale exists and the subscription is either cancelled or ended, delete it
                        if existing_sale and (cancelled or ended):
                            print(f"Sale {sale_id} is cancelled or ended. Deleting from database...")
                            delete_sale_from_db(sale_id)
                            churn_subscription(sale['subscription_id']+"7", effective_date, churn_type)
                        else:
                            # If the sale is not cancelled or ended, process it normally
                            post_response = post_to_profitwell(sale)
                            if post_response.status_code == 200 or post_response.status_code == 201:
                                print(f"Posted sale {sale['id']} to ProfitWell successfully!")
                            else:
                                print(f"Error posting sale {sale['id']} to ProfitWell, status code {post_response.status_code}: {post_response.json()}")
                            # Churn the subscription if needed
                            if churn_type:
                                print(f"Churning subscription for sale {sale_id} with type {churn_type}")
                                churn_subscription(sale['subscription_id']+"7", effective_date, churn_type)
                            else:
                                mark_sale_processed(sale_id, True)

                            

                            

                # Move to the next page if available
                if next_page_key:
                    page_key = next_page_key
                else:
                    break

            else:
                print(f"Error fetching data from Gumroad: {response.status_code}")
                print(response.json())
                current_token = next(token_generator)  # Switch to the next token if an error occurs
                continue

        except StopIteration:
            print("All tokens exhausted. Please try again later.")
            break

if __name__ == "__main__":
    process_sales()

# Close the database connection when done
conn.close()
