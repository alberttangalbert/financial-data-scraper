from typing import List, Tuple
import pandas as pd
import re 

from app.services.azure_services.openai_service import AzureOpenAIService
from app.controllers.document_processing.utils.openai_utils import retry_with_exponential_backoff

def run_step_with_retries(step_func, max_attempts=3):
    """
    Runs a step (a group of function calls) up to a maximum number of attempts.
    If any function in the step fails, the entire step is retried from the beginning.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return step_func()
        except Exception as e:
            if attempt == max_attempts:
                raise e  # Re-raise the last exception after max attempts
            print(f"Attempt {attempt} failed for step {step_func.__name__}. Retrying...")

def generate_income_statement(
    income_statement_dfs: List[pd.DataFrame],
    unit_scale: str,
    year_ended: str
) -> Tuple[List[pd.DataFrame], List[float]]:
    """
    Generates the income statement by calculating key financial metrics (Total Revenue, Gross Profit,
    Operating Income, Pre-Tax Income, Net Income) and returning associated DataFrames and values.
    
    If any functions within a step fail, that entire step is retried up to 3 times starting from
    the first function of that step.
    """

    def step_1():
        # Step 1: Get Revenue Breakdown and Total Revenue
        revenue_breakdown_response = get_revenue_breakdown(income_statement_dfs, unit_scale, year_ended)
        revenue_df = parse_revenue_table(revenue_breakdown_response)
        total_revenue = extract_total_revenue(revenue_df)
        return revenue_df, total_revenue

    def step_2(total_revenue):
        # Step 2: Calculate Gross Profit
        gross_profit_response = get_gross_profit(total_revenue, income_statement_dfs, unit_scale, year_ended)
        gross_profit_df = parse_gross_profit_table(gross_profit_response)
        gross_profit = calculate_gross_profit(gross_profit_df)
        return gross_profit_df, gross_profit

    def step_3(gross_profit):
        # Step 3: Calculate Operating Income
        operating_income_response = get_operating_income(gross_profit, income_statement_dfs, unit_scale, year_ended)
        operating_income_df = parse_operating_income_table(operating_income_response)
        operating_income = calculate_operating_income(operating_income_df)
        return operating_income_df, operating_income

    def step_4(operating_income):
        # Step 4: Calculate Pre-Tax Income
        pre_tax_income_response = get_pre_tax_income(operating_income, income_statement_dfs, unit_scale, year_ended)
        pre_tax_income_df = parse_pre_tax_income_table(pre_tax_income_response)
        pre_tax_income = calculate_pre_tax_income(pre_tax_income_df)
        return pre_tax_income_df, pre_tax_income

    def step_5(pre_tax_income):
        # Step 5: Calculate Net Income
        net_income_response = get_net_income(pre_tax_income, income_statement_dfs, unit_scale, year_ended)
        net_income_df = parse_net_income_table(net_income_response)
        net_income = calculate_net_income(net_income_df)
        return net_income_df, net_income

    # Run each step with retries
    revenue_df, total_revenue = run_step_with_retries(step_1)
    gross_profit_df, gross_profit = run_step_with_retries(lambda: step_2(total_revenue))
    operating_income_df, operating_income = run_step_with_retries(lambda: step_3(gross_profit))
    pre_tax_income_df, pre_tax_income = run_step_with_retries(lambda: step_4(operating_income))
    net_income_df, net_income = run_step_with_retries(lambda: step_5(pre_tax_income))

    # Return all DataFrames and calculated amounts as lists
    dataframes = [revenue_df, gross_profit_df, operating_income_df, pre_tax_income_df, net_income_df]
    amounts = [total_revenue, gross_profit, operating_income, pre_tax_income, net_income]

    return dataframes, amounts

def get_revenue_breakdown(
    income_statement_dfs: List[str],
    unit_scale: str,
    year_ended: str
) -> str:
    """
    Queries the Azure OpenAI Service to get the total revenue and revenue breakdown by segment
    for the given fiscal year.
    """
    system_prompt = (
        f"Based on the provided tables related to the income statement, "
        f"please provide the total revenue for the year ended {year_ended} "
        f"and the revenue breakdown by segment. "
        f"The unit scale for all values is '{unit_scale}'. "
        "Use the unit scale '{unit_scale}' for all values in your response!. "
        "If values are negative make sure to format them with parentheses (e.g., (500,000)). "
        "The output should be formatted as a table with only 2 columns: "
        "Segment and Revenue. "
        "Ensure that the sum of all segment revenues matches the total revenue exactly. "
        "Prioritize that segment revenues sum up to the total revenue and check your math! "
        "Make sure all revenues are converted to the same unit scale, which is explicitly stated as '{unit_scale}'. "
        "The table should follow this exact format:\n\n"
        "| Segment        | Revenue       |\n"
        "|----------------|---------------|\n"
        "| Segment A      | 1,000,000     |\n"
        "| Segment B      | 2,000,000     |\n"
        "| Segment C      | 3,000,000     |\n"
        "| Total Revenue  | 6,000,000     |\n\n"
        "1,000,000 + 2,000,000 + 3,000,000 = 6,000,000\n\n"
        "Please follow this example format exactly."
    )

    user_prompt = (
        "Provided tables:\n"
        "\n\n".join(income_statement_dfs)
    )

    openai_service = AzureOpenAIService()
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
    # print(response)
    return response

def parse_revenue_table(response: str) -> pd.DataFrame:
    """
    Parses the revenue breakdown table from the chatbot response and converts it into a pandas DataFrame.
    Handles markdown tables with additional explanatory text.

    Args:
        response (str): The raw response containing the revenue breakdown table.

    Returns:
        pd.DataFrame: A DataFrame containing columns 'Segment' and 'Revenue'.

    Raises:
        ValueError: If the table cannot be parsed correctly.
    """
    try:
        # Extract the markdown table portion
        table_start = response.find("| Segment")
        table_end = response.rfind("|")
        if table_start == -1 or table_end == -1:
            raise ValueError("Table format not found in the response.")

        table_text = response[table_start:table_end + 1]

        # Use regex to extract rows from the table
        rows = re.findall(r"\| ([^|]+?)\s*\| ([\d,]+)\s*\|", table_text)
        if not rows:
            raise ValueError("No valid rows found in the extracted table.")

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=["Segment", "Revenue"])

        # Clean and format the 'Revenue' column
        df["Revenue"] = df["Revenue"].str.replace(",", "").astype(float)

        return df

    except Exception as e:
        raise ValueError(f"Failed to parse revenue table: {e}")

def extract_total_revenue(df: pd.DataFrame) -> float:
    """
    Extracts the total revenue from a DataFrame containing a revenue breakdown table.

    Args:
        df (pd.DataFrame): A DataFrame containing columns 'Segment' and 'Revenue'.

    Returns:
        float: The total revenue value.

    Raises:
        ValueError: If the 'Total Revenue' row is not found in the DataFrame.
    """
    try:
        # Filter the DataFrame for the row containing "Total Revenue"
        total_revenue_row = df[df["Segment"].str.contains("Total Revenue", case=False, na=False)]

        if total_revenue_row.empty:
            raise ValueError("'Total Revenue' row not found in the DataFrame.")

        # Extract the revenue value from the filtered row
        total_revenue = total_revenue_row["Revenue"].iloc[0]

        return total_revenue
    except Exception as e:
        raise ValueError(f"Failed to extract total revenue: {e}")
    
def get_gross_profit(
    total_revenue: float,
    income_statement_dfs: List[str],
    unit_scale: str,
    year_ended: str
) -> str:
    """
    Queries the Azure OpenAI Service to get the detailed breakdown of how total revenue transitions
    to gross profit for the given fiscal year, excluding items that calculate gross profit to operating income.

    Args:
        total_revenue (float): The total revenue for the year.
        income_statement_dfs (List[str]): List of income statement tables as strings.
        unit_scale (str): The unit scale for the values (e.g., 'Millions').
        year_ended (str): The fiscal year ended date.

    Returns:
        str: The chatbot's response containing the breakdown of revenue to gross profit in table format.
    """
    # Define the system prompt
    system_prompt = (
        f"Based on the provided tables related to the income statement, "
        f"please provide the detailed breakdown of how the total revenue transitions to gross profit "
        f"for the year ended {year_ended}. "
        f"The total revenue for the year is {total_revenue:,.2f} ({unit_scale}). "
        "Use the unit scale '{unit_scale}' for all values in your response!. "
        "If values are negative make sure to format them with parentheses (e.g., (500,000)). "
        "The output should follow professional accounting standards. "
        "Do not include 'Less' or 'Add' in the item descriptions. "
        "If any value is negative, format it with parentheses (e.g., (500,000)). "
        "The first row should be 'Total Revenue', followed by specific items contributing to the calculation "
        "of gross profit, ending with the 'Gross Profit' as the last row. "
        "Exclude any expense numbers or items that are part of the calculation from gross profit to operating income, "
        "as operating income will be calculated separately. "
        "Ensure that all rows above 'Gross Profit' sum to exactly match the 'Gross Profit' value. "
        "The table should be formatted as follows:\n\n"
        "| Item                  | Value         |\n"
        "|-----------------------|---------------|\n"
        "| Total Revenue         | 10,000,000    |\n"
        "| Cost of Goods Sold    | (6,000,000)     |\n"
        "| Discounts             | (500,000)       |\n"
        "| Gross Profit          | 3,500,000     |\n\n"
        "10,000,000 - 6,000,000 - 500,000 = 3,500,000\n\n"
        "Please follow this example format exactly."
    )

    # Combine the income statement tables into the user prompt
    user_prompt = (
        "Provided tables:\n"
        "\n\n".join(income_statement_dfs)
    )

    # Initialize the Azure OpenAI Service
    openai_service = AzureOpenAIService()

    # Query the chatbot for the gross profit breakdown
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
    # print(response)
    return response

def parse_gross_profit_table(response: str) -> pd.DataFrame:
    """
    Parses the response from the get_gross_profit function and converts the table into a pandas DataFrame.

    Args:
        response (str): The raw response containing the gross profit table.

    Returns:
        pd.DataFrame: A DataFrame containing columns 'Item' and 'Value'.

    Raises:
        ValueError: If the table cannot be parsed correctly.
    """
    try:
        # Extract the table portion from the response
        table_start = response.find("| Item")
        table_end = response.rfind("|")
        if table_start == -1 or table_end == -1:
            raise ValueError("Table format not found in the response.")

        table_text = response[table_start:table_end + 1]

        # Use regex to parse table rows
        rows = re.findall(r"\| (.+?)\s+\| ([\d,().-]+)\s+\|", table_text)

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=["Item", "Value"])

        # Clean and format 'Value' column
        df["Value"] = (
            df["Value"]
            .str.replace(",", "")  # Remove commas
            .str.replace(r"\(([\d.]+)\)", r"-\1", regex=True)  # Convert parentheses to negatives
            .astype(float)
        )

        return df
    except Exception as e:
        raise ValueError(f"Failed to parse gross profit table: {e}")
    
def calculate_gross_profit(df: pd.DataFrame) -> float:
    """
    Processes the DataFrame to extract and return the gross profit, ensuring the row contains 'Gross Profit'.

    Args:
        df (pd.DataFrame): A DataFrame containing the parsed gross profit table.

    Returns:
        float: The gross profit value.

    Raises:
        ValueError: If the 'Gross Profit' row is not found in the DataFrame.
    """
    try:
        # Check if 'Gross Profit' exists in the DataFrame
        gross_profit_row = df[df["Item"].str.contains("Gross Profit", case=False, na=False)]

        if gross_profit_row.empty:
            raise ValueError("'Gross Profit' row not found in the DataFrame.")

        # Extract the gross profit value
        gross_profit = gross_profit_row["Value"].iloc[0]

        return gross_profit
    except Exception as e:
        raise ValueError(f"Failed to calculate gross profit: {e}")
    
def get_operating_income(
    gross_profit: float,
    income_statement_dfs: List[str],
    unit_scale: str,
    year_ended: str
) -> str:
    """
    Queries the Azure OpenAI Service to get the detailed breakdown of how gross profit transitions
    to operating income for the given fiscal year.

    Args:
        gross_profit (float): The gross profit for the year.
        income_statement_dfs (List[str]): List of income statement tables as strings.
        unit_scale (str): The unit scale for the values (e.g., 'Millions').
        year_ended (str): The fiscal year ended date.

    Returns:
        str: The chatbot's response containing the breakdown of gross profit to operating income in table format.
    """
    # Define the system prompt
    system_prompt = (
        f"Based on the provided tables related to the income statement, "
        f"please provide the detailed breakdown of how the gross profit transitions to operating income "
        f"for the year ended {year_ended}. "
        f"The gross profit for the year is {gross_profit:,.2f} ({unit_scale}). "
        "Use the unit scale '{unit_scale}' for all values in your response!. "
        "If values are negative make sure to format them with parentheses (e.g., (500,000)). "
        "The output should follow professional accounting standards. "
        "Do not include 'Less' or 'Add' in the item descriptions. "
        "If any value is negative, format it with parentheses (e.g., (500,000)). "
        "The first row should be 'Gross Profit', followed by specific items contributing to the calculation "
        "of operating income, ending with the 'Operating Income' as the last row. "
        "Exclude any expense or item that transitions operating income to pre-tax income, "
        "as pre-tax income will be calculated separately. "
        "Ensure that all rows above 'Operating Income' sum to exactly match the 'Operating Income' value. "
        "The table should be formatted as follows:\n\n"
        "| Item                  | Value         |\n"
        "|-----------------------|---------------|\n"
        "| Gross Profit          | 3,500,000     |\n"
        "| Selling Expenses      | (1,000,000)     |\n"
        "| Administrative Expenses| (500,000)      |\n"
        "| Restructuring Costs   | 200,000     |\n"
        "| Operating Income      | 1,800,000     |\n\n"
        "3,500,000 - 1,000,000 - 500,000 + 200,000 = 1,800,000\n\n"
        "Please follow this example format exactly."
    )

    # Combine the income statement tables into the user prompt
    user_prompt = (
        "Provided tables:\n"
        "\n\n".join(income_statement_dfs)
    )

    # Initialize the Azure OpenAI Service
    openai_service = AzureOpenAIService()

    # Query the chatbot for the operating income breakdown
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
    # print(response)
    return response

def parse_operating_income_table(response: str) -> pd.DataFrame:
    """
    Parses the response from the get_operating_income function and converts the table into a pandas DataFrame.

    Args:
        response (str): The raw response containing the operating income table.

    Returns:
        pd.DataFrame: A DataFrame containing columns 'Item' and 'Value'.

    Raises:
        ValueError: If the table cannot be parsed correctly.
    """
    try:
        # Extract the table portion from the response
        table_start = response.find("| Item")
        table_end = response.rfind("|")
        if table_start == -1 or table_end == -1:
            raise ValueError("Table format not found in the response.")

        table_text = response[table_start:table_end + 1]

        # Use regex to parse table rows
        rows = re.findall(r"\| (.+?)\s+\| ([\d,().-]+)\s+\|", table_text)

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=["Item", "Value"])

        # Clean and format 'Value' column
        df["Value"] = (
            df["Value"]
            .str.replace(",", "")  # Remove commas
            .str.replace(r"\(([\d.]+)\)", r"-\1", regex=True)  # Convert parentheses to negatives
            .astype(float)
        )

        return df
    except Exception as e:
        raise ValueError(f"Failed to parse operating income table: {e}")

def calculate_operating_income(df: pd.DataFrame) -> float:
    """
    Processes the DataFrame to extract and return the operating income, ensuring the row contains 'Operating Income'.

    Args:
        df (pd.DataFrame): A DataFrame containing the parsed operating income table.

    Returns:
        float: The operating income value.

    Raises:
        ValueError: If the 'Operating Income' row is not found in the DataFrame.
    """
    try:
        # Check if 'Operating Income' exists in the DataFrame
        operating_income_row = df[df["Item"].str.contains("Operating Income", case=False, na=False)]

        if operating_income_row.empty:
            raise ValueError("'Operating Income' row not found in the DataFrame.")

        # Extract the operating income value
        operating_income = operating_income_row["Value"].iloc[0]

        return operating_income
    except Exception as e:
        raise ValueError(f"Failed to calculate operating income: {e}")
    
def get_pre_tax_income(
    operating_income: float,
    income_statement_dfs: List[str],
    unit_scale: str,
    year_ended: str
) -> str:
    """
    Queries the Azure OpenAI Service to get the detailed breakdown of how operating income transitions
    to pre-tax income for the given fiscal year.

    Args:
        operating_income (float): The operating income for the year.
        income_statement_dfs (List[str]): List of income statement tables as strings.
        unit_scale (str): The unit scale for the values (e.g., 'Millions').
        year_ended (str): The fiscal year ended date.

    Returns:
        str: The chatbot's response containing the breakdown of operating income to pre-tax income in table format.
    """
    # Define the system prompt
    system_prompt = (
        f"Based on the provided tables related to the income statement, "
        f"please provide the detailed breakdown of how the operating income transitions to pre-tax income "
        f"for the year ended {year_ended}. "
        f"The operating income for the year is {operating_income:,.2f} ({unit_scale}). "
        "Use the unit scale '{unit_scale}' for all values in your response!. "
        "If values are negative make sure to format them with parentheses (e.g., (500,000)). "
        "The output should follow professional accounting standards. "
        "Do not include 'Less' or 'Add' in the item descriptions. "
        "If any value is negative, format it with parentheses (e.g., (500,000)). "
        "The first row should be 'Operating Income', followed by specific items contributing to the calculation "
        "of pre-tax income, ending with the 'Pre-Tax Income' as the last row. "
        "Exclude any expense or item that transitions pre-tax income to net income, "
        "as net income will be calculated separately. "
        "Ensure that all rows above 'Pre-Tax Income' sum to exactly match the 'Pre-Tax Income' value. "
        "The table should be formatted as follows:\n\n"
        "| Item                  | Value         |\n"
        "|-----------------------|---------------|\n"
        "| Operating Income      | 1,800,000     |\n"
        "| Interest Expense      | (100,000)     |\n"
        "| Interest Income       | 50,000        |\n"
        "| Other Non-Operating Items | (50,000) |\n"
        "| Pre-Tax Income        | 1,700,000     |\n\n"
        "1,800,000 - (100,000 - 50,000 + 50,000) = 1,700,000\n\n"
        "Please follow this example format exactly."
    )

    # Combine the income statement tables into the user prompt
    user_prompt = (
        "Provided tables:\n"
        "\n\n".join(income_statement_dfs)
    )

    # Initialize the Azure OpenAI Service
    openai_service = AzureOpenAIService()

    # Query the chatbot for the pre-tax income breakdown
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
    # print(response)
    return response

def parse_pre_tax_income_table(response: str) -> pd.DataFrame:
    """
    Parses the response from the get_pre_tax_income function and converts the table into a pandas DataFrame.

    Args:
        response (str): The raw response containing the pre-tax income table.

    Returns:
        pd.DataFrame: A DataFrame containing columns 'Item' and 'Value'.

    Raises:
        ValueError: If the table cannot be parsed correctly.
    """
    try:
        # Extract the table portion from the response
        table_start = response.find("| Item")
        table_end = response.rfind("|")
        if table_start == -1 or table_end == -1:
            raise ValueError("Table format not found in the response.")

        table_text = response[table_start:table_end + 1]

        # Use regex to parse table rows
        rows = re.findall(r"\| (.+?)\s+\| ([\d,().—-]+)\s+\|", table_text)

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=["Item", "Value"])

        # Clean and format 'Value' column
        df["Value"] = (
            df["Value"]
            .str.replace(",", "")  # Remove commas
            .str.replace(r"\(([\d.]+)\)", r"-\1", regex=True)  # Convert parentheses to negatives
            .str.replace("—", "0")  # Replace dashes with 0
            .astype(float)
        )

        return df
    except Exception as e:
        raise ValueError(f"Failed to parse pre-tax income table: {e}")
    
def calculate_pre_tax_income(df: pd.DataFrame) -> float:
    """
    Processes the DataFrame to extract and return the pre-tax income, ensuring the row contains 'Pre-Tax Income'.

    Args:
        df (pd.DataFrame): A DataFrame containing the parsed pre-tax income table.

    Returns:
        float: The pre-tax income value.

    Raises:
        ValueError: If the 'Pre-Tax Income' row is not found in the DataFrame.
    """
    try:
        # Check if 'Pre-Tax Income' exists in the DataFrame
        pre_tax_income_row = df[df["Item"].str.contains("Pre-Tax Income", case=False, na=False)]

        if pre_tax_income_row.empty:
            raise ValueError("'Pre-Tax Income' row not found in the DataFrame.")

        # Extract the pre-tax income value
        pre_tax_income = pre_tax_income_row["Value"].iloc[0]

        return pre_tax_income
    except Exception as e:
        raise ValueError(f"Failed to calculate pre-tax income: {e}")
    
def get_net_income(
    pre_tax_income: float,
    income_statement_dfs: List[str],
    unit_scale: str,
    year_ended: str
) -> str:
    """
    Queries the Azure OpenAI Service to get the detailed breakdown of how pre-tax income transitions
    to net income for the given fiscal year.

    Args:
        pre_tax_income (float): The pre-tax income for the year.
        income_statement_dfs (List[str]): List of income statement tables as strings.
        unit_scale (str): The unit scale for the values (e.g., 'Millions').
        year_ended (str): The fiscal year ended date.

    Returns:
        str: The chatbot's response containing the breakdown of pre-tax income to net income in table format.
    """
    # Define the system prompt
    system_prompt = (
        f"Based on the provided tables related to the income statement, "
        f"please provide the detailed breakdown of how the pre-tax income transitions to net income "
        f"for the year ended {year_ended}. "
        "Use the unit scale '{unit_scale}' for all values in your response!. "
        "If values are negative make sure to format them with parentheses (e.g., (500,000)). "
        f"The pre-tax income for the year is {pre_tax_income:,.2f} ({unit_scale}). "
        "The output should follow professional accounting standards. "
        "Do not include 'Less' or 'Add' in the item descriptions. "
        "If any value is negative, format it with parentheses (e.g., (500,000)). "
        "The first row should be 'Pre-Tax Income', followed by specific items contributing to the calculation "
        "of net income, ending with the 'Net Income' as the last row. "
        "Ensure that all rows above 'Net Income' sum to exactly match the 'Net Income' value. "
        "The table should be formatted as follows:\n\n"
        "| Item                  | Value         |\n"
        "|-----------------------|---------------|\n"
        "| Pre-Tax Income        | 1,700,000     |\n"
        "| Income Tax Expense    | (500,000)     |\n"
        "| Net Income            | 1,200,000     |\n\n"
        "1,700,000 - 500,000 = 1,200,000\n\n"
        "Please follow this example format exactly."
    )

    # Combine the income statement tables into the user prompt
    user_prompt = (
        "Provided tables:\n"
        "\n\n".join(income_statement_dfs)
    )

    # Initialize the Azure OpenAI Service
    openai_service = AzureOpenAIService()

    # Query the chatbot for the net income breakdown
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)

    return response

def parse_net_income_table(response: str) -> pd.DataFrame:
    """
    Parses the response from the get_net_income function and converts the table into a pandas DataFrame.

    Args:
        response (str): The raw response containing the net income table.

    Returns:
        pd.DataFrame: A DataFrame containing columns 'Item' and 'Value'.

    Raises:
        ValueError: If the table cannot be parsed correctly.
    """
    try:
        # Extract the table portion from the response
        table_start = response.find("| Item")
        table_end = response.rfind("|")
        if table_start == -1 or table_end == -1:
            raise ValueError("Table format not found in the response.")

        table_text = response[table_start:table_end + 1]

        # Use regex to parse table rows
        rows = re.findall(r"\| (.+?)\s+\| ([\d,().-]+)\s+\|", table_text)

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=["Item", "Value"])

        # Clean and format 'Value' column
        df["Value"] = (
            df["Value"]
            .str.replace(",", "")  # Remove commas
            .str.replace(r"\(([\d.]+)\)", r"-\1", regex=True)  # Convert parentheses to negatives
            .astype(float)
        )

        return df
    except Exception as e:
        raise ValueError(f"Failed to parse net income table: {e}")
    
def calculate_net_income(df: pd.DataFrame) -> float:
    """
    Processes the DataFrame to extract and return the net income, ensuring the row contains 'Net Income'.

    Args:
        df (pd.DataFrame): A DataFrame containing the parsed net income table.

    Returns:
        float: The net income value.

    Raises:
        ValueError: If the 'Net Income' row is not found in the DataFrame.
    """
    try:
        # Check if 'Net Income' exists in the DataFrame
        net_income_row = df[df["Item"].str.contains("Net Income", case=False, na=False)]

        if net_income_row.empty:
            raise ValueError("'Net Income' row not found in the DataFrame.")

        # Extract the net income value
        net_income = net_income_row["Value"].iloc[0]

        return net_income
    except Exception as e:
        raise ValueError(f"Failed to calculate net income: {e}")
