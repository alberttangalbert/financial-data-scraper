import pandas as pd
import time
import re 
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict
import multiprocessing

from app.services.azure_services.openai_service import AzureOpenAIService

def retry_with_exponential_backoff(max_retries: int = 3, backoff_factor: int = 2):
    """
    Decorator that retries a function with exponential backoff upon an assertion error.

    Args:
        max_retries (int): Maximum number of retries.
        backoff_factor (int): Backoff factor for exponential delay.

    Returns:
        Decorated function.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except AssertionError:
                    if attempt < max_retries - 1:
                        time.sleep(backoff_factor ** attempt)
            # Return None if all retries fail
            return None
        return wrapper
    return decorator


def process_chatbot_response(response: str) -> Tuple[str, str]:
    """
    Processes the chatbot response to extract the statement type and explanation.

    Args:
        response (str): The chatbot response string.

    Returns:
        Tuple[str, str]: A tuple containing the statement type and explanation.
    """
    # Define the list of possible responses
    valid_responses = {
        "income statement": "Income Statement",
        "balance sheet": "Balance Sheet",
        "stockholders equity statement": "Stockholder's Equity Statement",
        "stockholder's equity statement": "Stockholder's Equity Statement",
        "stockholders' equity statement": "Stockholder's Equity Statement",
        "cash flow statement": "Cash Flow Statement",
        "none": "None"
    }

    # Find the first pair of brackets and extract the text inside
    match = re.search(r"\[(.*?)\]", response)
    statement = match.group(1).strip().lower() if match else None

    if statement is None or statement not in valid_responses:
        raise ValueError(f"Invalid response: '{statement}' is not in the list of valid responses.")

    # Map the extracted statement to the correct case-sensitive version
    normalized_statement = valid_responses[statement]

    return normalized_statement

@retry_with_exponential_backoff()
def classify_table(df: str) -> Tuple[str, str]:
    """
    Classifies a markdown table into financial statement categories.

    Args:
        df (str): DataFrame representation of the table in markdown format.

    Returns:
        Tuple[str, str]: Statement type and explanation.
    """
    system_prompt = (
        "You are given a markdown table taken from the 10-K or 10-Q filing of a company. "
        "Please identify if the table belongs to the income statement, balance sheet, "
        "stockholder's equity statement, or cash flow statement, or none of them at all. "
        "If the table contains information on the revenue breakdown "
        "that counts as part of the [Income Statement]. "
        "If the table does not belong to any of the statements, return [None]. "
        "Return the name of which statement the table belongs to in brackets "
        "then provide a very brief explanation of how you determined the answer.\n"
        "Example Output:\n"
        "[Income Statement]\nThis table shows the revenues and expenses of the company.\n\n"
    )

    user_prompt = f"Given markdown table:\n{df}"

    chatbot = AzureOpenAIService()

    response = chatbot.query(
        system_prompt=system_prompt,
        user_prompt=user_prompt
    )

    return process_chatbot_response(response)


from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import pandas as pd


def classify_multiple_tables(
    dfs: List[pd.DataFrame],
    max_workers: Optional[int] = None
) -> List[str]:
    """
    Classifies multiple markdown tables concurrently.

    Args:
        dfs (List[pd.DataFrame]): List of DataFrames to classify.
        max_workers (int, optional): Maximum number of worker threads. Defaults to the number of CPU cores.

    Returns:
        List[str]: A list of classification statements for each table.
    """
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()

    classifications = [None] * len(dfs)  # To maintain order in results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks with indices to keep track of table order
        future_to_index = {
            executor.submit(classify_table, df): i for i, df in enumerate(dfs)
        }

        for future in as_completed(future_to_index):
            index = future_to_index[future]
            statement = future.result()  # Ignore explanation
            classifications[index] = statement

    return classifications


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from app.services.azure_services.openai_service import AzureOpenAIService
from typing import List, Optional

def extract_fiscal_year_end(
    text: List[str], 
    openai_service: AzureOpenAIService,
    top_n: int = 5
) -> Optional[str]:
    """
    Extract the fiscal year end date using a Retrieval-Augmented Generation (RAG) approach.

    Args:
        text (List[str]): List of text strings from the document
        openai_service (AzureOpenAIService): OpenAI service to query
        top_n (int, optional): Number of top similar text segments to retrieve. Defaults to 5.

    Returns:
        Optional[str]: Extracted fiscal year end date or None if not found
    """
    # Create TF-IDF vectorizer
    vectorizer = TfidfVectorizer(stop_words='english')
    
    # Define retrieval query
    query = "fiscal year end date"
    
    # Convert text to TF-IDF vectors
    text_vectors = vectorizer.fit_transform(text)
    
    # Convert query to TF-IDF vector
    query_vector = vectorizer.transform([query])
    
    # Calculate cosine similarities
    similarities = cosine_similarity(query_vector, text_vectors)[0]
    
    # Find the top k most similar text segments
    top_k_indices = np.argsort(similarities)[-top_n:][::-1]
    retrieved_contexts = [text[idx] for idx in top_k_indices]
    
    # Prepare system prompt for extracting fiscal year end
    system_prompt = (
        "You are an expert document analyzer. Your task is to precisely extract "
        "the fiscal year end date from the given context. "
        "Please follow these guidelines:\n"
        "1. Look for phrases like 'Fiscal Year Ended', 'Year End', 'As of'\n"
        "2. Provide ONLY the exact date in the format 'Month Day, Year'\n"
        "3. If no clear fiscal year end date is found, respond with 'Not Found'\n"
        "4. Use ONLY the information from the provided context"
    )
    
    # Combine retrieved contexts
    context = "\n\n".join(retrieved_contexts)
    
    # Query the chatbot to extract fiscal year end
    user_prompt = f"Extract the fiscal year end date from the following context:\n\n{context}"
    
    try:
        response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
        result = response.strip()
        return result if result.lower() != 'not found' else None
    except Exception as e:
        print(f"Error extracting fiscal year end: {e}")
        return None

def extract_company_name(
    text: List[str], 
    openai_service: AzureOpenAIService,
    max_context_lengths: List[int] = [1000, 2000, 5000]
) -> Optional[str]:
    """
    Extract document metadata using progressive context expansion.
    
    Args:
        text (List[str]): List of text strings from the document
        openai_service (AzureOpenAIService): OpenAI service to query
        metadata_type (str): Type of metadata to extract ('fiscal_year_end' or 'company_name')
        max_context_lengths (List[int]): Maximum context lengths to try
    
    Returns:
        Optional[str]: Extracted metadata or None if not found
    """
    # Define system prompts and queries based on metadata type
    system_prompt = (
        "You are an expert document analyzer. Your task is to precisely extract "
        "the official company name from the given context. "
        "Please follow these guidelines:\n"
        "1. Look for the full legal name of the company\n"
        "2. Prioritize names found in headers, title pages, or official statements\n"
        "3. Provide ONLY the exact company name\n"
        "4. If multiple variations exist, choose the most formal, complete version\n"
        "5. If no clear company name is found, respond with 'Not Found'\n"
        "6. Use ONLY the information from the provided context"
    )
    user_prompt_template = "Extract the official company name from the following context:\n\n{context}"

    # Attempt extraction with progressive context expansion
    for max_length in max_context_lengths:
        # Concatenate text segments up to max_length
        full_context = ""
        for segment in text:
            if len(full_context) + len(segment) <= max_length:
                full_context += segment + "\n\n"
            else:
                break
        
        # Skip if context is empty
        if not full_context.strip():
            continue
        
        # Prepare user prompt
        user_prompt = user_prompt_template.format(context=full_context)
        
        try:
            # Query the chatbot
            response = openai_service.query(
                system_prompt=system_prompt, 
                user_prompt=user_prompt
            )
            
            # Check if a meaningful response was found
            if response.strip().lower() not in ['not found', '']:
                return response.strip()
        
        except Exception as e:
            print(f"Error extracting company name: {e}")
    
    # Return None if no metadata found after all attempts
    return ""

def extract_unit_scale(
    combined_dfs: str,
    openai_service: AzureOpenAIService
) -> str:
    """
    Extracts the collective unit scale from the provided tables using Azure OpenAI.

    Args:
        dfs (str): List of pandas DataFrames representing the tables in string markdown form.
        openai_service (AzureOpenAIService): Service to interact with Azure OpenAI.

    Returns:
        str: The collective unit scale (e.g., 'millions', 'thousands', 'billions').

    Raises:
        AssertionError: If the chatbot output is invalid or doesn't match the expected format.
    """
    # Combine all tables into a single prompt
    system_prompt = (
        "You are given all the tables from the 10-K or 10-Q filing of a company. "
        "Please identify the unit scale (e.g., millions, thousands, billions, etc.) used in the tables. "
        "If the tables use different unit scales, choose the one that appears the most frequently. "
        "Provide your answer as a single string, such as 'millions', 'thousands', or 'billions'. "
        "Ensure the output is strictly in this format and does not contain additional text."
    )

    # Send the prompt to the OpenAI service
    response = openai_service.query(system_prompt, combined_dfs)

    # Parse and validate the response
    return _parse_unit_scale_response(response)


def _parse_unit_scale_response(response: str) -> str:
    """
    Helper function to parse and validate the chatbot response.

    Args:
        response (str): The raw response from the chatbot.

    Returns:
        str: The collective unit scale.

    Raises:
        AssertionError: If the chatbot output is not valid or in the correct format.
    """
    try:
        # Ensure the response is a single string
        parsed_response = response.strip().lower()
        
        # Validate expected unit scales
        valid_units = {'millions', 'thousands', 'billions'}
        if parsed_response not in valid_units:
            raise AssertionError("Response contains an invalid unit scale.")
        
        return parsed_response
    except Exception as e:
        raise AssertionError(f"Failed to parse or validate chatbot response: {e}")

def aggregate_income_statements(results: Dict[str, Tuple[List[pd.DataFrame], List[float]]]) -> str:
    """
    Sends multiple fiscal years' income statement data to the Azure OpenAI Service
    and asks the model to aggregate them into a single clean table, relying on the model’s
    best professional judgment as an accountant to merge similar line items.

    Args:
        results (Dict[str, Tuple[List[pd.DataFrame], List[float]]]): Dictionary where keys are `year_ended`
            and values are tuples (dataframes, amounts).

    Returns:
        str: The aggregated table as a string.
    """

    def dataframe_to_text(df) -> str:
        """Converts a DataFrame to a simple text representation (e.g., CSV)."""
        return df.to_csv(index=False)

    # Adjusted system prompt to rely on the model's professional judgment instead of a similarity threshold
    system_prompt = (
        "You are a financial assistant acting as a professional accountant. You are given data from multiple "
        "fiscal years' income statements.\n"
        "Aggregate them into a single clean table, following these rules:\n\n"
        "Formatting:\n"
        "- The table should have a header row with fiscal years as column headers (e.g., 'FY 2023', 'FY 2024').\n"
        "- The first column header (row 1, col 0) should be blank.\n"
        "- Preserve the original order of rows from the first year processed for all identical or equivalent line items.\n"
        "- If new line items appear in subsequent years, place them in the most logically appropriate category "
        "according to standard income statement sequencing.\n"
        "- Use your best professional judgment as an accountant to merge line items that represent the same concept, "
        "even if phrased slightly differently (e.g., adding or removing words like 'and', 'Expenses', etc.).\n"
        "- Do not include unnecessary rows like 'Income Statement' or 'Revenue' as section headers.\n"
        "- Negative values should be displayed in parentheses.\n"
        "- Keep a consistent unit scale.\n"
        "- The final output should be a clean table without bold formatting or extra section headers.\n\n"
        "Logical Arithmetic Flow (Standard Income Statement Ordering):\n"
        "1. All revenue line items (segment revenues) and then Total Revenue.\n"
        "2. Cost of Goods Sold (COGS).\n"
        "3. Gross Profit.\n"
        "4. Operating Expenses (e.g., Selling, General & Administrative).\n"
        "5. Operating Income.\n"
        "6. Interest-related items (e.g., Interest Expense, Other Interest Expense, Interest Income), Other Non-Operating Items.\n"
        "7. Pre-Tax Income.\n"
        "8. Income Tax Expense.\n"
        "9. Net Income.\n\n"
        "Ensure that items like 'Interest Expense on Long-Term Debt' and 'Other Interest Expense' appear after Operating Income "
        "and before Pre-Tax Income. If new items appear in later years that fit into the interest/other non-operating category, "
        "place them in that category.\n\n"
        "Do not provide any explanation outside of the final table. Just provide the aggregated table."
    )

    # Build the user prompt with all the years' data
    user_prompt = "Below are the income statement tables for multiple fiscal years:\n\n"
    for year_ended, (dataframes, amounts) in results.items():
        user_prompt += f"Year: {year_ended}\n"
        metric_names = [
            "Revenue Breakdown",
            "Gross Profit",
            "Operating Income",
            "Pre-Tax Income",
            "Net Income"
        ]
        for metric_name, df, amount in zip(metric_names, dataframes, amounts):
            user_prompt += f"{metric_name} table:\n"
            user_prompt += dataframe_to_text(df) + "\n\n"
            user_prompt += f"Amount associated with {metric_name}: {amount}\n\n"

    user_prompt += (
        "Please aggregate all the years' data into a single table following the rules above.\n"
        "Remember to merge line items representing the same concept, using your best judgment as a professional accountant.\n"
    )

    openai_service = AzureOpenAIService()
    response = openai_service.query(system_prompt=system_prompt, user_prompt=user_prompt)
    return response