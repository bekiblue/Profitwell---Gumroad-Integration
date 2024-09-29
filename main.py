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

# Create a table to store processed subscriptions with their cancellation status
cursor.execute('''CREATE TABLE IF NOT EXISTS processed_subscriptions (
                    subscription_id TEXT PRIMARY KEY,
                    cancelled BOOLEAN
                 )''')
conn.commit()

# Function to handle subscription service end logic
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

            # Extract the failed_at field
            failed_at = subscriber_data.get('failed_at')
            # If the failed_at field is present, return the failed_at as a UNIX timestamp
            if failed_at:
                failed_at = datetime.strptime(failed_at, "%Y-%m-%dT%H:%M:%SZ")
                return int(time.mktime(failed_at.timetuple())), current_token
            
            # Extract the cancelled_at field
            cancelled_at = subscriber_data.get('cancelled_at')
            # If the cancelled_at field is present, return the cancelled_at as a UNIX timestamp
            if cancelled_at:
                cancelled_at = datetime.strptime(cancelled_at, "%Y-%m-%dT%H:%M:%SZ")
                return int(time.mktime(cancelled_at.timetuple())), current_token
            
            # Extract the ended_at field
            ended_at = subscriber_data.get('ended_at')
            # If the ended_at field is present, return the end date as a UNIX timestamp
            if ended_at:
                end_date = datetime.strptime(ended_at, "%Y-%m-%dT%H:%M:%SZ")
                return int(time.mktime(end_date.timetuple())), current_token
        elif response.status_code == 400:
            print("A required parameter is missing")
            return None, current_token
        elif response.status_code == 401:
            print("An invalid Gumroad access token.")
            return None, current_token
        else:  # Rate limit
            print("Rate limit reached. Switching tokens...")
            try:
                current_token = next(token_generator)
            except StopIteration:
                # All tokens exhausted
                print("All tokens exhausted. Please try again later.")
                return None, current_token


# Function to map plan intervals for ProfitWell
def map_plan_interval(gumroad_interval):
    interval_mapping = {
        'monthly': 'month',
        'quarterly': 'month', 
        'biannually': 'month',  
        'yearly': 'year',
        'every_two_years': 'year'  
    }
    return interval_mapping.get(gumroad_interval, 'month')  # Default to 'month' if no match


# Function to check if a subscription exists in the database and get its cancellation status
def subscription_exists_and_cancelled(subscription_id):
    cursor.execute("SELECT cancelled FROM processed_subscriptions WHERE subscription_id = ?", (subscription_id,))
    return cursor.fetchone()  # Returns (cancelled,) or None if not found


# Function to mark a subscription as processed and store its cancellation status
def mark_subscription_processed(subscription_id, cancelled):
    cursor.execute("INSERT INTO processed_subscriptions (subscription_id, cancelled) VALUES (?, ?)", (subscription_id, cancelled))
    conn.commit()


# Function to update the cancellation status of a subscription
def update_subscription_cancelled(subscription_id, cancelled):
    cursor.execute("UPDATE processed_subscriptions SET cancelled = ? WHERE subscription_id = ?", (cancelled, subscription_id))
    conn.commit()


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
    
    #Truncate the email to 36 characters
    user_alias = sale['email'][:36]

    # Prepare the data for ProfitWell
    values = {
        "user_alias": user_alias,  # or use your own user_alias
        "subscription_alias": sale['subscription_id'],  # Unique subscription alias
        "email": user_alias,
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
    if response.status_code == 200:
        return response, current_token
    elif response.status_code == 400:
        print("A required parameter is missing")
        return None, current_token
    elif response.status_code == 401:
        print("An invalid Gumroad access token.")
        return None, current_token
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


# Main function to process sales and work with subscriptions
def process_sales():
    token_generator = rotate_tokens()
    current_token = next(token_generator)
    page_key = None
    
    while True:
        try:
            # Fetch sales data from Gumroad
            response, current_token = get_sales_data(token_generator, current_token, page_key)
            if response is None:
                break  # Stop if token switching failed or all tokens are exhausted
            
            if response.status_code == 200:
                sales_data = response.json()['sales']
                next_page_key = response.json().get('next_page_key')

                # Process each sale
                for sale in sales_data:
                    if sale.get("subscription_id"):
                        subscription_id = sale['subscription_id']
                        cancelled = sale.get('cancelled', False)  # Get the 'cancelled' field
                        ended = sale.get('ended', None) # Get the 'ended' field
                        dead = sale.get('dead', None)   
                        churn_type = None

                        # Determine the churn type and effective date
                        if cancelled:
                            churn_type = "voluntary"
                        if ended or dead:
                            churn_type = "delinquent"
                        
                        
                        if cancelled or ended or dead:
                            effective_date, current_token = serviceEnd(sale, token_generator, current_token)  # Handle token switching within serviceEnd
                            if effective_date is None:
                                continue  

                        # Check if the subscription exists in the database
                        existing_subscription = subscription_exists_and_cancelled(subscription_id)
                        
                        if existing_subscription:
                            # If the subscription exists and was previously active (not canceled) but now is canceled
                            if not existing_subscription[0] and (cancelled or ended):
                                print(f"Updating cancellation status for subscription {subscription_id}.")
                                churn_response = churn_subscription(subscription_id, effective_date, churn_type)
                                if churn_response.status_code == 200:
                                    update_subscription_cancelled(subscription_id, True)
                            else:
                                print(f"Subscription {subscription_id} already cancelled and processed before or no change needed.")
                                continue
                        else:
                            # If the subscription is new, process it normally
                            post_response = post_to_profitwell(sale)
                            if post_response.status_code == 200 or post_response.status_code == 201:
                                print(f"Posted subscription {subscription_id} to ProfitWell successfully!")
                            else:
                                print(f"Error posting subscription {subscription_id} to ProfitWell, status code {post_response.status_code}: {post_response.json()}")
                                continue
                            
                            # If it's cancelled, churn the subscription
                            if churn_type:
                                print(f"Churning subscription {subscription_id} with type {churn_type}")
                                churn_response=  churn_subscription(subscription_id, effective_date, churn_type)
                                if churn_response.status_code == 200:
                                    mark_subscription_processed(subscription_id, True)
                            else:
                                mark_subscription_processed(subscription_id, False)

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
