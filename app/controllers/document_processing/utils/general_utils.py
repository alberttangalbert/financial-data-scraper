from typing import List, Dict, Tuple, Any, Optional
from collections import defaultdict
import pandas as pd
from io import BytesIO
import re

from app.controllers.document_processing.utils.doc_intel_utils import analyze_result_dict_to_df
from app.services.azure_services.blob_storage_service import AzureBlobStorageService

def extract_table_details(
    table: Dict[str, Any],
    paragraphs: List[Dict[str, Any]],
    dfs: List[Any],
    dfs_sources: List[Any]
) -> Dict[str, Any]:
    """
    Extracts table details, including its content, associated context, and bounding polygons.

    :param table: Dictionary representing a table from the document.
    :param paragraphs: List of all paragraphs in the document.
    :param dfs: List to store extracted dataframes.
    :param dfs_sources: List to store bounding source polygons for tables.
    :return: A dictionary containing table details for merging with the final output.
    """
    df, source = analyze_result_dict_to_df(table)
    dfs.append(df)
    dfs_sources.append(source)
    table_index = len(dfs) - 1

    footnotes = table.get("footnotes", "")
    footnotes_text = footnotes[0]["content"] if footnotes else ""

    table_start = table["spans"][0]["offset"] if "spans" in table else None

    # Extract context paragraphs
    context_content, context_polygons, context_indices = extract_context_for_table(paragraphs, table_start)

    # Combine context, table content, and footnotes into a single string
    context_string = "\n".join(context_content)
    combined_string = f"Context:\n{context_string}\n\nTable:\n{df.to_markdown(index=False)}"
    if footnotes_text:
        combined_string += f"\n\nFootnotes:\n{footnotes_text}"

    # Collect all polygons (context, sources, and footnotes)
    all_polygons = collect_table_polygons(context_polygons, source, footnotes)

    # Generate bounding boxes for each page
    page_bounding_boxes = create_page_bounding_boxes(all_polygons)

    return {
        "offset": table_start,
        "combined_string": combined_string,
        "source_boxes": page_bounding_boxes,
        "context_indices": context_indices,
        "table_index": table_index
    }


def extract_context_for_table(
    paragraphs: List[Dict[str, Any]],
    table_start: Optional[int]
) -> Tuple[List[str], Dict[int, List[List[float]]], List[int]]:
    """
    Extracts up to three paragraphs before a table to serve as its context.

    :param paragraphs: List of all paragraphs in the document.
    :param table_start: The starting offset of the table.
    :return: A tuple containing context content, bounding polygons, and paragraph indices.
    """
    if table_start is None:
        return [], defaultdict(list), []

    # Find the last three paragraphs before the table
    previous_paragraphs = [
        (idx, p) for idx, p in enumerate(paragraphs)
        if p["spans"][0]["offset"] + p["spans"][0]["length"] <= table_start
    ][-3:]

    # Identify the closest section heading or title
    closest_heading_idx = next(
        (idx for idx, (_, paragraph) in enumerate(previous_paragraphs) if paragraph.get("role") in ["sectionHeading", "title"]),
        None
    )

    # Include only paragraphs from the closest heading onward, or fallback to the last paragraph
    to_include = previous_paragraphs[closest_heading_idx:] if closest_heading_idx is not None else previous_paragraphs[-1:]

    context_content = []
    context_indices = []
    context_polygons = defaultdict(list)

    for idx, paragraph in to_include:
        context_content.append(paragraph["content"])
        context_indices.append(idx)
        for region in paragraph["boundingRegions"]:
            context_polygons[region["pageNumber"]].append(region["polygon"])

    return context_content, context_polygons, context_indices


def collect_table_polygons(
    context_polygons: Dict[int, List[List[float]]],
    source: List[Dict[str, Any]],
    footnotes: List[Dict[str, Any]]
) -> Dict[int, List[List[float]]]:
    """
    Collects all bounding polygons associated with a table, including context and footnotes.

    :param context_polygons: Polygons from context paragraphs.
    :param source: Polygons from the table's sources.
    :param footnotes: Polygons from the table's footnotes.
    :return: Dictionary of all polygons grouped by page.
    """
    all_polygons = defaultdict(list)

    # Add context polygons
    for page, polygons in context_polygons.items():
        all_polygons[page].extend(polygons)

    # Add source polygons
    for source_item in source:
        for region in source_item:
            if isinstance(region, dict) and "polygon" in region:
                all_polygons[region["pageNumber"]].append(region["polygon"])

    # Add footnote polygons
    if footnotes:
        for region in footnotes[0]["boundingRegions"]:
            all_polygons[region["pageNumber"]].append(region["polygon"])

    return all_polygons


def create_page_bounding_boxes(all_polygons: Dict[int, List[List[float]]]) -> List[Dict[str, Any]]:
    """
    Creates bounding boxes for each page from a collection of polygons.

    :param all_polygons: Dictionary of polygons grouped by page.
    :return: List of bounding box dictionaries for each page.
    """
    page_bounding_boxes = []
    for page, polygons in all_polygons.items():
        min_x = min(point[0] for polygon in polygons for point in zip(polygon[::2], polygon[1::2]))
        min_y = min(point[1] for polygon in polygons for point in zip(polygon[::2], polygon[1::2]))
        max_x = max(point[0] for polygon in polygons for point in zip(polygon[::2], polygon[1::2]))
        max_y = max(point[1] for polygon in polygons for point in zip(polygon[::2], polygon[1::2]))

        page_bounding_boxes.append({
            "pageNumber": page,
            "boundingBox": [min_x, min_y, max_x, min_y, max_x, max_y, min_x, max_y]
        })
    return page_bounding_boxes


def filter_paragraphs_without_overlap(
    paragraphs: List[Dict[str, Any]],
    table_spans: List[Dict[str, Any]]
) -> List[Tuple[int, Dict[str, Any]]]:
    """
    Filters paragraphs that do not overlap with any table spans.

    :param paragraphs: List of all paragraphs in the document.
    :param table_spans: List of spans corresponding to tables.
    :return: List of non-overlapping paragraphs with their starting offsets.
    """
    non_overlapping_paragraphs = []
    for i, paragraph in enumerate(paragraphs):
        paragraph_span = paragraph.get("spans", [{}])[0]
        paragraph_start = paragraph_span.get("offset")
        paragraph_end = paragraph_start + paragraph_span.get("length", 0)

        if not any(
            t_span["offset"] <= paragraph_end and paragraph_start <= t_span["offset"] + t_span["length"]
            for t_span in table_spans
        ):
            non_overlapping_paragraphs.append((paragraph_start, paragraph))

    return non_overlapping_paragraphs


def convert_analyze_document_to_structured_data(
    result: Dict[str, Any]
) -> Tuple[List[str], List[Any], List[int], List[List[Any]]]:
    """
    Converts the output of an "Analyze Document" operation into structured paragraphs and tables.

    :param result: The result of the document intelligence "Analyze Document" operation.
    :return: A tuple containing final items (text), sources, table indicators, and related sources.
    """
    dfs, dfs_sources = [], []
    tables_info = []

    # Extract table details
    for table in result.get("tables", []):
        tables_info.append(extract_table_details(table, result.get("paragraphs", []), dfs, dfs_sources))

    # Filter paragraphs that do not overlap with tables
    non_overlapping_paragraphs = filter_paragraphs_without_overlap(
        result.get("paragraphs", []), [t["spans"][0] for t in result.get("tables", [])]
    )
    paragraphs_by_offset = {p_off: p_val for p_off, p_val in non_overlapping_paragraphs}

    # Build final output
    return build_final_output(paragraphs_by_offset, tables_info, dfs_sources)


def build_final_output(
    paragraphs_by_offset: Dict[int, Dict[str, Any]],
    tables_info: List[Dict[str, Any]],
    dfs_sources: List[Any]
) -> Tuple[List[str], List[Any], List[int], List[List[Any]]]:
    """
    Builds the final structured output containing paragraphs and tables.

    :param paragraphs_by_offset: Dictionary of non-overlapping paragraphs by offset.
    :param tables_info: List of extracted table details.
    :param dfs_sources: List of sources for tables.
    :return: A tuple containing final items, sources, table indicators, and related sources.
    """
    final_items = []
    final_sources = []
    table_indicator = []
    table_related_sources = []

    all_offsets = [(off, "paragraph", p) for off, p in paragraphs_by_offset.items()]
    for tinfo in tables_info:
        all_offsets.append((tinfo["offset"], "table", tinfo["combined_string"], tinfo["source_boxes"], tinfo["table_index"]))

    all_offsets.sort(key=lambda x: x[0])

    for item in all_offsets:
        if item[1] == "paragraph":
            paragraph_dict = item[2]
            final_items.append(paragraph_dict["content"])
            final_sources.append(paragraph_dict)
            table_indicator.append(0)
            table_related_sources.append([])
        else:
            combined_string = item[2]
            source_boxes = item[3]
            table_idx = item[4]

            final_items.append(combined_string)
            final_sources.append(source_boxes)
            table_indicator.append(1)
            table_related_sources.append(dfs_sources[table_idx])

    return final_items, final_sources, table_indicator, table_related_sources

import pandas as pd
import re

def parse_table_from_response(response: str) -> pd.DataFrame:
    """
    Dynamically parses a table from a text response containing a markdown table 
    and converts it into a pandas DataFrame.
    """
    # Extract lines that look like table rows (start and end with '|')

    lines = [line for line in response.split('\n') if line.strip().startswith('|') and line.strip().endswith('|')]

    if len(lines) < 2:
        raise ValueError("No valid table found in the response.")

    # Parse each line into columns
    rows = []
    for line in lines:
        # Split on '|', ignoring the first and last empty splits due to leading/trailing '|'
        parts = [col.strip() for col in line.strip().split('|')[1:-1]]
        rows.append(parts)

    # The first row is the header
    headers = rows[0]

    # The second row is likely the separator row (dashes). Remove it if present.
    if all(re.match(r'^\-+$', col) for col in rows[1]):
        rows.pop(1)

    # The remaining rows are data
    data = rows[1:]

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Convert numeric columns
    for col in df.columns[1:]:  # Skipping the first column (descriptive)
        df[col] = (
            df[col]
            .str.replace(',', '', regex=False)                  # Remove commas
            .str.replace(r'\(([\d.]+)\)', r'-\1', regex=True)   # Convert (xxx) to -xxx
            .apply(pd.to_numeric, errors='coerce')              # Convert to numeric
        )

    return df
    
def store_dataframe_to_blob(dataframe: pd.DataFrame, blob_service: AzureBlobStorageService) -> str:
    """
    Stores a Pandas DataFrame as an Excel file in Azure Blob Storage.

    Args:
        dataframe (pd.DataFrame): The DataFrame to store.
        blob_service (AzureBlobStorageService): Azure Blob Storage service instance.

    Returns:
        str: The blob name of the uploaded Excel file.
    """
    try:
        # Create a BytesIO stream to save the Excel file
        excel_stream = BytesIO()
        dataframe.to_excel(excel_stream, index=False, engine='xlsxwriter')
        excel_stream.seek(0)

        # Define the blob name and upload the file
        blob_name = "aggregated_income_statement.xlsx"
        blob_service.upload_to_blob_storage(
            blob_name=blob_name,
            data=excel_stream.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print(f"Excel file '{blob_name}' successfully uploaded to Azure Blob Storage.")
        return blob_name
    except Exception as e:
        print(f"Error storing DataFrame to Azure Blob Storage: {e}")
        raise