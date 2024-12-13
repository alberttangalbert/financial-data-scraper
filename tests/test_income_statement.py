import unittest
import pandas as pd
from app.core.fs_generators.income_statement_gen import (
    parse_revenue_table, extract_total_revenue,
    parse_gross_profit_table, calculate_gross_profit,
    parse_operating_income_table, calculate_operating_income,
    parse_pre_tax_income_table, calculate_pre_tax_income,
    parse_net_income_table, calculate_net_income
)

class TestIncomeStatementFunctions(unittest.TestCase):
    def setUp(self):
        # Mocked responses for testing
        self.mock_revenue_response = (
            """
            | Segment        | Revenue       |
            |----------------|---------------|
            | Segment A      | 1,000,000     |
            | Segment B      | 2,000,000     |
            | Total Revenue  | 3,000,000     |
            """
        )

        self.mock_gross_profit_response = (
            """
            | Item                  | Value         |
            |-----------------------|---------------|
            | Total Revenue         | 3,000,000     |
            | Cost of Goods Sold    | (1,500,000)   |
            | Gross Profit          | 1,500,000     |
            """
        )

        self.mock_operating_income_response = (
            """
            | Item                  | Value         |
            |-----------------------|---------------|
            | Gross Profit          | 1,500,000     |
            | Operating Expenses    | (500,000)     |
            | Operating Income      | 1,000,000     |
            """
        )

        self.mock_pre_tax_income_response = (
            """
            | Item                  | Value         |
            |-----------------------|---------------|
            | Operating Income      | 1,000,000     |
            | Interest Expense      | (50,000)      |
            | Pre-Tax Income        | 950,000       |
            """
        )

        self.mock_net_income_response = (
            """
            | Item                  | Value         |
            |-----------------------|---------------|
            | Pre-Tax Income        | 950,000       |
            | Income Tax Expense    | (200,000)     |
            | Net Income            | 750,000       |
            """
        )

    def test_parse_revenue_table(self):
        df = parse_revenue_table(self.mock_revenue_response)
        self.assertEqual(len(df), 3)
        self.assertEqual(df.loc[2, "Segment"], "Total Revenue")
        self.assertEqual(df.loc[2, "Revenue"], 3000000.0)

    def test_extract_total_revenue(self):
        df = parse_revenue_table(self.mock_revenue_response)
        total_revenue = extract_total_revenue(df)
        self.assertEqual(total_revenue, 3000000.0)

    def test_parse_gross_profit_table(self):
        df = parse_gross_profit_table(self.mock_gross_profit_response)
        self.assertEqual(len(df), 3)
        self.assertEqual(df.loc[2, "Item"], "Gross Profit")
        self.assertEqual(df.loc[2, "Value"], 1500000.0)

    def test_calculate_gross_profit(self):
        df = parse_gross_profit_table(self.mock_gross_profit_response)
        gross_profit = calculate_gross_profit(df)
        self.assertEqual(gross_profit, 1500000.0)

    def test_parse_operating_income_table(self):
        df = parse_operating_income_table(self.mock_operating_income_response)
        self.assertEqual(len(df), 3)
        self.assertEqual(df.loc[2, "Item"], "Operating Income")
        self.assertEqual(df.loc[2, "Value"], 1000000.0)

    def test_calculate_operating_income(self):
        df = parse_operating_income_table(self.mock_operating_income_response)
        operating_income = calculate_operating_income(df)
        self.assertEqual(operating_income, 1000000.0)

    def test_parse_pre_tax_income_table(self):
        df = parse_pre_tax_income_table(self.mock_pre_tax_income_response)
        self.assertEqual(len(df), 3)
        self.assertEqual(df.loc[2, "Item"], "Pre-Tax Income")
        self.assertEqual(df.loc[2, "Value"], 950000.0)

    def test_calculate_pre_tax_income(self):
        df = parse_pre_tax_income_table(self.mock_pre_tax_income_response)
        pre_tax_income = calculate_pre_tax_income(df)
        self.assertEqual(pre_tax_income, 950000.0)

    def test_parse_net_income_table(self):
        df = parse_net_income_table(self.mock_net_income_response)
        self.assertEqual(len(df), 3)
        self.assertEqual(df.loc[2, "Item"], "Net Income")
        self.assertEqual(df.loc[2, "Value"], 750000.0)

    def test_calculate_net_income(self):
        df = parse_net_income_table(self.mock_net_income_response)
        net_income = calculate_net_income(df)
        self.assertEqual(net_income, 750000.0)

if __name__ == "__main__":
    unittest.main()