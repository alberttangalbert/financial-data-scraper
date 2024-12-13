# Financal Data Scraper

## Description


## Steps to Run Locally
1. Create and populate .env file, example given
2. Export .env variables to environment
3. Install requirements
4. Run the Server
    ```
    $ gunicorn -b 0.0.0.0:5000 run:app
    or 
    $ python run.py
    or
    $ docker build -t rfp-azure-bridge .
    $ docker run --env-file .env -p 5001:5000 rfp-azure-bridge
    ```
