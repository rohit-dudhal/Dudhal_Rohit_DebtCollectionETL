from sqlalchemy import types

# Define the table schema using pandas dtype mappings
borrower_dtype = {
    'Name': types.VARCHAR(255),
    'Date of Birth': types.DATE,
    'Gender': types.VARCHAR(50),
    'Marital Status': types.VARCHAR(50),
    'Phone Number': types.VARCHAR(20),
    'Email Address': types.VARCHAR(255),
    'Mailing Address': types.TEXT,
    'Language Preference': types.VARCHAR(50),
    'Geographical Location': types.VARCHAR(255),
    'Credit Score': types.INTEGER,
    'Loan Type': types.VARCHAR(255),
    'Loan Amount': types.INTEGER,
    'Loan Term': types.INTEGER,
    'Interest Rate': types.FLOAT,
    'Loan Purpose': types.VARCHAR(255),
    'EMI': types.FLOAT,
    'IP Address': types.VARCHAR(50),
    'Repayment History': types.TEXT,
    'Days Left to Pay Current EMI': types.INTEGER,
    'Delayed Payment': types.VARCHAR(50),
    'Email Valid': types.BOOLEAN,
    'Geolocation Valid': types.BOOLEAN,
    'Latitude': types.FLOAT,
    'Longitude': types.FLOAT,
    'Total EMI Paid Count': types.INTEGER
}