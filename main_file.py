import pandas as pd
import os


def load_data(file_path):
    return pd.read_csv(file_path)

#function to clean and transform the data

def transform_data(df):
    # Clean the 'price' column and remove non-numeric characters
    df['price'] = df['price'].replace({r'\$': '', r',': ''}, regex=True)

    # Convert the 'price' column to float, setting errors='coerce' to turn invalid entries into NaN
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Optionally, handle NaN values, for example, by filling them with 0 or removing rows
    df['price'] = df['price'].fillna(0)  # Or df.dropna(subset=['price'], inplace=True)

    # 2. Clean 'rating' column: Convert text rating (e.g., 'Five Stars') to numeric (e.g., 5)
    rating_map = {
        'One Star': 1,
        'Two Stars': 2,
        'Three Stars': 3,
        'Four Stars': 4,
        'Five Stars': 5
    }
    df['rating'] = df['rating'].map(rating_map).fillna(df['rating'])

    # 3. Clean 'reviewTime' column: Convert to datetime and extract year, month, and day of week
    df['reviewTime'] = pd.to_datetime(df['reviewTime'], errors='coerce')
    df['year'] = df['reviewTime'].dt.year
    df['month'] = df['reviewTime'].dt.month
    df['day_of_week'] = df['reviewTime'].dt.dayofweek

    # 4. Clean 'vote' column: Extract helpfulness score if it's in ratio format
    df['helpfulness_score'] = df['vote'].apply(lambda x: int(x.split('/')[0]) if isinstance(x, str) else 0)

    # 5. Clean 'verified' column: Convert boolean True/False to 1/0
    df['verified'] = df['verified'].astype(int)

    # 6. Clean 'description' column: Remove unnecessary quotes and square brackets
    df['description'] = df['description'].apply(lambda x: ', '.join(eval(x)) if isinstance(x, str) else x)

    # 7. Clean 'feature' column: Convert from list of features to string or None if empty
    df['feature'] = df['feature'].apply(lambda x: ', '.join(eval(x)) if isinstance(x, str) else None)

    # 8. Clean 'image' column: Convert image URLs from string to a list of URLs
    df['image'] = df['image'].apply(lambda x: eval(x) if isinstance(x, str) else [])

    # 9. Clean 'itemName' column: Keep as is, no transformation needed

    return df
    
# function to load the transformed data back into s3 in batch files

def load_to_local(df, file_name, batch_size=1000):
    # Define the path where the transformed data will be saved
    save_dir = os.path.expanduser("~/Desktop/ETL/transformed_Data")  # macOS/Linux

    # Ensure the directory exists
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Calculate the number of batches
    total_rows = len(df)
    num_batches = (total_rows // batch_size) + 1  # Add one more batch if there's a remainder

    for batch_num in range(num_batches):
        # Get the start and end indices for this batch
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        
        # Slice the DataFrame for the current batch
        batch_df = df.iloc[start_index:end_index]

        # Define the full file path for the batch file
        batch_file = f"transformed_{file_name}_batch_{batch_num + 1}.csv"
        file_path = os.path.join(save_dir, batch_file)

        # Save the batch as a CSV file
        batch_df.to_csv(file_path, index=False)
        
        print(f"Batch {batch_num + 1} saved locally at: {file_path}")


# main etl function

def etl_process(file_path):
    #step1: extract data
    df = load_data(file_path)
    print("data loaded successfully")

    #step 2 Transform data
    df_transformed = transform_data(df)
    print("data transformed successfully")

    load_to_local(df_transformed,file_path)
    print("batch wise data loading complete")

file_path = 'amazon_reviews_2018.csv'


#run the etl process
etl_process(file_path)