import pandas as pd
from faker import Faker
import random

# Initialize Faker instance
fake = Faker()

# Function to generate random data for each row
def generate_random_row(index):
    return {
        "Index": index,
        "Organization Id": fake.uuid4(),
        "Name": fake.company(),
        "Website": fake.url(),
        "Country": fake.country(),
        "Description": fake.bs(),
        "Founded": random.randint(1900, 2024),
        "Industry": fake.word(),
        "Number of employees": random.randint(10, 10000)
    }

# Number of rows per file
rows_per_file = 500000
total_rows = 10000000
num_files = 20

# Loop to create the required files
for file_index in range(1, num_files + 1):
    data = []
    start_index = (file_index - 1) * rows_per_file + 1
    end_index = file_index * rows_per_file

    # Generate data for each file
    for i in range(start_index, end_index + 1):
        data.append(generate_random_row(i))

    # Create DataFrame for the current chunk of data
    df = pd.DataFrame(data)

    # Save the data to a CSV file
    df.to_csv(f'random_companies_{file_index}.csv', index=False)

    print(f'File {file_index} of {num_files} generated.')

