import unittest
from unittest.mock import MagicMock
from collections import defaultdict
import pandas as pd
from app.controllers.document_processing.utils.doc_intel_utils import analyze_result_dict_to_df
from app.controllers.document_processing.utils.general_utils import (
    extract_table_details,
    extract_context_for_table,
    collect_table_polygons,
    create_page_bounding_boxes,
    filter_paragraphs_without_overlap,
    parse_table_from_response
)

class TestTableProcessing(unittest.TestCase):

    def setUp(self):
        self.mock_table = {
            "spans": [{"offset": 100}],
            "footnotes": [{"content": "This is a footnote.", "boundingRegions": [{"pageNumber": 1, "polygon": [1, 1, 2, 2]}]}]
        }

        self.mock_paragraphs = [
            {"content": "Heading", "spans": [{"offset": 50, "length": 10}], "role": "sectionHeading", "boundingRegions": [{"pageNumber": 1, "polygon": [0, 0, 1, 1]}]},
            {"content": "Paragraph 1", "spans": [{"offset": 60, "length": 20}], "boundingRegions": [{"pageNumber": 1, "polygon": [2, 2, 3, 3]}]},
            {"content": "Paragraph 2", "spans": [{"offset": 80, "length": 20}], "boundingRegions": [{"pageNumber": 1, "polygon": [3, 3, 4, 4]}]},
        ]

    def test_extract_context_for_table(self):
        context_content, context_polygons, context_indices = extract_context_for_table(self.mock_paragraphs, 100)
        self.assertEqual(len(context_content), 3)  # Updated to reflect 3 paragraphs
        self.assertEqual(context_content[0], "Heading")
        self.assertEqual(context_indices, [0, 1, 2])

    def test_collect_table_polygons(self):
        context_polygons = {1: [[0, 0, 1, 1], [2, 2, 3, 3]]}
        source = [{"pageNumber": 1, "polygon": [4, 4, 5, 5]}]
        footnotes = [{"boundingRegions": [{"pageNumber": 1, "polygon": [6, 6, 7, 7]}]}]
        all_polygons = collect_table_polygons(context_polygons, source, footnotes)
        self.assertEqual(len(all_polygons[1]), 3)

    def test_create_page_bounding_boxes(self):
        all_polygons = {1: [[0, 0, 1, 1], [2, 2, 3, 3]]}  # Corrected polygon structure
        bounding_boxes = create_page_bounding_boxes(all_polygons)
        self.assertEqual(len(bounding_boxes), 1)
        self.assertEqual(bounding_boxes[0]["boundingBox"], [0, 0, 3, 0, 3, 3, 0, 3])

    def test_parse_table_from_response(self):
        response = (
            """
            | Column A | Column B |
            |----------|----------|
            | Data 1   | Data 2   |
            | Data 3   | Data 4   |
            """
        )
        df = parse_table_from_response(response)
        self.assertEqual(len(df), 2)
        self.assertListEqual(list(df.columns), ["Column A", "Column B"])
        self.assertEqual(df.iloc[0]["Column A"], "Data 1")

if __name__ == "__main__":
    unittest.main()
