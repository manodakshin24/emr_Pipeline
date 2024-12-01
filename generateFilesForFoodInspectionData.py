import pandas as pd
from faker import Faker
import random
import time

# Initialize Faker instance
fake = Faker()

# Function to generate random data for each row
def generate_random_row(index):
    return {
        "Index": index,
        "Name": fake.company(),
        "Program Identifier": fake.uuid4(),
        "Inspection Date": fake.date_between(start_date="-5y", end_date="today"),
        "Description": fake.sentence(nb_words=6),
        "Address": fake.address().replace("\n", ", "),
        "City": fake.city(),
        "Zip Code": fake.zipcode(),
        "Phone": fake.phone_number(),
        "Longitude": fake.longitude(),
        "Latitude": fake.latitude(),
        "Inspection Business Name": fake.company(),
        "Inspection Type": random.choice(["Routine", "Complaint-Based", "Follow-Up"]),
        "Inspection Score": random.randint(50, 100),
        "Inspection Result": random.choice(["Pass", "Fail", "Needs Improvement"]),
        "Inspection Closed Business": random.choice(["Yes", "No"]),
        "Violation Type": random.choice(["Hygiene", "Structural", "Documentation"]),
        "Violation Description": fake.sentence(nb_words=10),
        "Violation Points": random.randint(1, 10),
        "Business_ID": fake.uuid4(),
        "Inspection_Serial_Num": fake.uuid4(),
        "Violation_Record_ID": fake.uuid4(),
        "Grade": random.choice(["A", "B", "C", "D", "F"])
    }

# Number of rows per file
rows_per_file = 250000
total_rows = 7500000
num_files = total_rows // rows_per_file

# Loop to create the required files
for file_index in range(1, num_files + 1):
    start_time = time.time()  # Start timing
    
    data = []
    start_index = (file_index - 1) * rows_per_file + 1
    end_index = file_index * rows_per_file

    # Generate data for each file
    for i in range(start_index, end_index + 1):
        data.append(generate_random_row(i))

    # Create DataFrame for the current chunk of data
    df = pd.DataFrame(data)

    # Save the data to a CSV file
    df.to_csv(f'random_food_inspections_{file_index}.csv', index=False)

    # Calculate and log elapsed time
    elapsed_time = (time.time() - start_time) / 60  # Time in minutes
    print(f'File {file_index} of {num_files} generated in {elapsed_time:.2f} minutes.')