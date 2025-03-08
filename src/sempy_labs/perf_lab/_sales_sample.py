
import pandas as pd
import urllib.parse
import sempy.fabric as fabric
from typing import Optional, Union, List, Tuple
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from uuid import UUID

import sempy_labs._icons as icons
from sempy_labs.tom import connect_semantic_model
from sempy_labs.perf_lab._lab_infrastructure import _ensure_table
from sempy_labs.perf_lab._test_suite import TestSuite, TestDefinition
from sempy_labs._helper_functions import (
    _create_spark_session,
    save_as_delta_table,
)
    
class SalesLakehouseConfig:
    def __init__(self, 
                 start_date: Optional[Union[str, date]] = None,
                 years: Optional[int] = 4,
                 fact_rows_in_millions: Optional[int] = 100,
                 num_fact_tables: Optional[int] = 1):
        """
        Initializes a SalesSampleParameters instance.

        Parameters
        ----------
        start_date : str | date, default=None
            The start date for the date table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
            Defaults to None which resolves to the current date minus the specified years.
        years : int, default=4
            The number of years that the date table covers.
            The value must be greater than 0. Defaults to 4.
        fact_rows_in_millions : int, default=100
            The number of transactions in the sales table(s) in millions.
            The value must be greater than 0. Defaults to 100 for 100 million rows.
        num_fact_tables : int, default=1
            The number of fact table(s) to generate for the lakehouse.
            The value must be greater than 0. Defaults to 1.
        """
        self.start_date = start_date
        self.years = years
        self.fact_rows_in_millions = fact_rows_in_millions
        self.num_fact_tables = num_fact_tables

    def to_dict(self) -> dict:
        """
        Generates a property bag for the provision_lakehouse function.

        Returns
        -------
        dict
            A dictionary wrapping the parameters passed into this class.
        """
        return {
            "start_date": self.start_date,
            "years": self.years,
            "fact_rows_in_millions": self.fact_rows_in_millions,
            "num_fact_tables": self.num_fact_tables
        }


def provision_sales_tables(
    workspace_id: UUID,
    lakehouse_id: UUID,
    table_properties: Optional[dict] = None,
):
    """
    Generates the Delta tables for a sales sample lakehouse.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The key/value pairs in the dictionary are specific to the table_generator function.
    """

    start_date = table_properties['start_date']
    years = table_properties['years']
    fact_rows_in_millions = table_properties['fact_rows_in_millions']
    num_fact_tables = table_properties['num_fact_tables']        

        
    # Generate and persist the sample Delta tables in the lakehouse.
    save_as_delta_table(
        dataframe = _get_dates_df(start_date, years),
        delta_table_name = "date",
        write_mode = 'overwrite',
        lakehouse = lakehouse_id,
        workspace = workspace_id,
        )

    save_as_delta_table(
        dataframe = _get_measures_table_df(),
        delta_table_name = "measuregroup",
        write_mode = 'overwrite',
        lakehouse = lakehouse_id,
        workspace = workspace_id,
        )

    save_as_delta_table(
        dataframe = _get_product_categories_df(),
        delta_table_name = "productcategory",
        write_mode = 'overwrite',
        lakehouse = lakehouse_id,
        workspace = workspace_id,
        )
 
    save_as_delta_table(
        dataframe = _get_geography_df(),
        delta_table_name = "geography",
        write_mode = 'overwrite',
        lakehouse = lakehouse_id,
        workspace = workspace_id,
        )


    if not _ensure_table("date", workspace_id, lakehouse_id):
        print(f"{icons.yellow_dot} The Delta table 'date' was created but has not yet appeared in the lakehouse.")
    if not _ensure_table("measuregroup", workspace_id, lakehouse_id):
        print(f"{icons.yellow_dot} The Delta table 'measuregroup' was created but has not yet appeared in the lakehouse.")
    if not _ensure_table("productcategory", workspace_id, lakehouse_id):
        print(f"{icons.yellow_dot} The Delta table 'productcategory' was created but has not yet appeared in the lakehouse.")
    if not _ensure_table("geography", workspace_id, lakehouse_id):
        print(f"{icons.yellow_dot} The Delta table 'geography' was created but has not yet appeared in the lakehouse.")

    for i in range(1, num_fact_tables+1):
        save_as_delta_table(
            dataframe = _get_sales_df(start_date, years, fact_rows_in_millions, i),
            delta_table_name = f"sales_{i}",
            write_mode = 'overwrite',
            lakehouse = lakehouse_id,
            workspace = workspace_id,
            )
        if not _ensure_table(f"sales_{i}", workspace_id, lakehouse_id):
            print(f"{icons.yellow_dot} The Delta table 'sales_{i}' was created but has not yet appeared in the lakehouse.")


def _get_dates_df(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
) -> 'pyspark.sql.DataFrame':
    """
    Generates sample data for a date table.

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the date table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the date table covers.
        The value must be greater than 0. Defaults to 4.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe with the typical columns of a basic date table.
    """   
    from pyspark.sql.functions import col, last_day, dayofweek, year, month, date_format

    # years must be greater than 0.
    if years < 1:
        raise ValueError("Years must be greater than 0.")

    # Make sure the dates are valid
    if not start_date:
       start_date = date.today() - relativedelta(years=years)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

    end_date = start_date + relativedelta(years=years)

    # Generate the date table data.
    spark = _create_spark_session()

    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='Date')
    date_df['Date'] = date_df['Date'].astype(str)

    spark_df  = spark.createDataFrame(date_df)
    spark_df = spark_df.withColumn('Date', col('Date').cast('date'))
    spark_df = spark_df.withColumn('DateID', date_format(col('Date'),"yyyyMMdd").cast('integer'))
    spark_df = spark_df.withColumn('Monthly', date_format(col('Date'),"yyyy-MM-01").cast('date'))
    spark_df = spark_df.withColumn('Month', date_format(col('Date'),"MMM"))
    spark_df = spark_df.withColumn('MonthYear', date_format(col('Date'),"MMM yyyy"))
    spark_df = spark_df.withColumn('MonthOfYear', month(col('Date')))
    spark_df = spark_df.withColumn('Year', year(col('Date')))
    spark_df = spark_df.withColumn('EndOfMonth', last_day(col('Date')).cast('date'))
    spark_df = spark_df.withColumn('DayOfWeekNum', dayofweek(col('Date')))
    spark_df = spark_df.withColumn('DayOfWeek', date_format(col('Date'),"EE"))
    spark_df = spark_df.withColumn('WeeklyStartSun', col('Date')+1-dayofweek(col('Date')))
    spark_df = spark_df.withColumn('WeeklyStartMon', col('Date')+2-dayofweek(col('Date')))

    return spark_df 


def _get_measures_table_df(
) -> 'pyspark.sql.DataFrame':
    """
    Generates a tiny table to store the measures in the model

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe for the measures table.
    """
    spark = _create_spark_session()

    # A table to store the measures in the model
    data = [('Measures only',)]
    columns = ['Col1']

    return spark.createDataFrame(data, columns)


def _get_product_categories_df(
) -> 'pyspark.sql.DataFrame':
    """
    Generates sample data for a productcategory table.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe with a small list of product categories.
    """
    spark = _create_spark_session()

    # A small dimension table
    data = [(1, 'Accessories'), (2, 'Bikes'), (3, 'Clothing')]
    columns = ['ProductCategoryID', 'ProductCategory']

    return spark.createDataFrame(data, columns)


def _get_geography_df(
) -> 'pyspark.sql.DataFrame':
    """
    Generates a geo dimension table with USA and Australia and their states/territories.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe with sample data for a geography table.
    """
    spark = _create_spark_session()

    # A table with geography sample data
    data = [(1, 'Alabama', 'USA'),(2, 'Alaska', 'USA'),(3, 'Arizona', 'USA'),(4, 'Arkansas', 'USA'),(5, 'California', 'USA'),(6, 'Colorado', 'USA'),(7, 'Connecticut', 'USA'),(8, 'Delaware', 'USA'),(9, 'Florida', 'USA'),(10, 'Georgia', 'USA'),(11, 'Hawaii', 'USA'),(12, 'Idaho', 'USA'),(13, 'Illinois', 'USA'),(14, 'Indiana', 'USA'),(15, 'Iowa', 'USA'),(16, 'Kansas', 'USA'),(17, 'Kentucky', 'USA'),(18, 'Louisiana', 'USA'),(19, 'Maine', 'USA'),(20, 'Maryland', 'USA'),(21, 'Massachusetts', 'USA'),(22, 'Michigan', 'USA'),(23, 'Minnesota', 'USA'),(24, 'Mississippi', 'USA'),(25, 'Missouri', 'USA'),(26, 'Montana', 'USA'),(27, 'Nebraska', 'USA'),(28, 'Nevada', 'USA'),(29, 'New Hampshire', 'USA'),(30, 'New Jersey', 'USA'),(31, 'New Mexico', 'USA'),(32, 'New York', 'USA'),(33, 'North Carolina', 'USA'),(34, 'North Dakota', 'USA'),(35, 'Ohio', 'USA'),(36, 'Oklahoma', 'USA'),(37, 'Oregon', 'USA'),(38, 'Pennsylvania', 'USA'),(39, 'Rhode Island', 'USA'),(40, 'South Carolina', 'USA'),(41, 'South Dakota', 'USA'),(42, 'Tennessee', 'USA'),(43, 'Texas', 'USA'),(44, 'Utah', 'USA'),(45, 'Vermont', 'USA'),(46, 'Virginia', 'USA'),(47, 'Washington', 'USA'),(48, 'West Virginia', 'USA'),(49, 'Wisconsin', 'USA'),(50, 'Wyoming', 'USA'),(51, 'New South Wales', 'Australia'),(52, 'Queensland', 'Australia'),(53, 'South Australia', 'Australia'),(54, 'Tasmania', 'Australia'),(55, 'Victoria', 'Australia'),(56, 'Western Australia', 'Australia'),(57, 'Australian Capital Territory', 'Australia'),(58, 'Northern Territory', 'Australia')]
    columns = ['GeoID', 'StateOrTerritory', 'Country']

    return spark.createDataFrame(data, columns)


def _get_sales_df(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
    num_rows_in_millions: Optional[int] = 100,
    seed: Optional[int] = 1,
) -> 'pyspark.sql.DataFrame':
    """
    Generates a fact table with random links to date, product category, and geography dimensions
    and a sales column with random generated numbers (1-1000)
    and a costs column with random generated numbers (1-100)

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the transactions in the sales table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the transactions in the sales table cover.
        The value must be greater than 0. Defaults to 4.
    num_rows_in_millions : int, default=100
        The number of transactions in the sales table in millions.
        The value must be greater than 0. Defaults to 100 for 100 million rows.
    seed: int, default=1
        A seed value for the random numbers generator. Defaults to 1.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe with sample sales data for a fact table.
    """
    from pyspark.sql.functions import rand, expr

    #years must be greater than 0.
    if years < 1:
        raise ValueError("Years must be greater than 0.")

    #num_rows_in_millions must be greater than 0.
    if num_rows_in_millions < 1:
        raise ValueError("The number of rows in millions must be greater than 0.")

    # Make sure the start_date is valid
    if not start_date:
       start_date = date.today() - relativedelta(years=years)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

    spark = _create_spark_session()

    if num_rows_in_millions > 10:
        print(f"{icons.in_progress} Generating {num_rows_in_millions} million rows of random sales data.")

    return spark.range(0, num_rows_in_millions * 1000000).withColumn('ProductCategoryID', (rand(seed=seed)*3+1).cast('int')) \
        .withColumn('GeoID', (rand(seed=seed)*58+1).cast('int')) \
        .withColumn('DateID', expr(f'cast(date_format(date_add("{start_date}", cast(rand(100) * 365 * {years} as int)), "yyyyMMdd") as int)')) \
        .withColumn('Sales', (rand(seed=seed*4)*1000+1).cast('int')) \
        .withColumn('Costs', (rand(seed=seed+45)*100+1).cast('int'))


def apply_sales_metadata(
    semantic_model: str,
    workspace: UUID,
    remove_schema: bool
):
    """
    semantic_model : str
        The name or ID of the semantic model. The semantic model must exit.   
    workspace : str | uuid.UUID
        The Fabric workspace name or ID where the semantic model is located.
        The workspace must be specified and must exist or the function fails with a WorkspaceNotFoundException.
    remove_schema : bool
        Specifies the the schema name must be removed from all the tables in the semantic model.
    """

    with connect_semantic_model(dataset=semantic_model, workspace=workspace, readonly=False) as tom:

        # Clean up table names and source lineage tags
        for t in tom.model.Tables:
            for c in t.Columns:
                if c.Name.startswith("RowNumber") == False:
                    c.SourceLineageTag = c.Name
            
            if remove_schema:
                t.SourceLineageTag = t.SourceLineageTag.replace("[dbo].", "")

            if t.SourceLineageTag == "[dbo].[sales_1]" or t.SourceLineageTag == "[sales_1]":
                t.set_Name("Sales")
            elif t.SourceLineageTag == "[dbo].[measuregroup]" or t.SourceLineageTag == "[measuregroup]": 
                t.set_Name("Pick a measure")
            elif t.SourceLineageTag == "[dbo].[productcategory]" or t.SourceLineageTag == "[productcategory]": 
                t.set_Name("Product")
            else:
                t.set_Name(t.Name.capitalize())            

        print(f"{icons.checked} Table names updated.")

        # Mark as date table and create relationships
        tom.mark_as_date_table(table_name="Date", column_name="Date")

        tom.add_relationship(
            from_table="Sales",
            from_column="DateID",
            to_table="Date",
            to_column="DateID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="GeoID",
            to_table="Geography",
            to_column="GeoID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="ProductCategoryID",
            to_table="Product",
            to_column="ProductCategoryID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )

        print(f"{icons.checked} Table relationships added.")

        # Mark measures
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Orders",
            description = "Counts the total number of rows in the Sales table, representing the total number of orders.",
            expression = "COUNTROWS(Sales)",
            format_string = "#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Sales",
            description = "Calculates the total sales by summing all the sales values.",
            expression = "SUM(Sales[sales])",
            format_string_expression = f"""
                SWITCH(
                    TRUE(),
                    SELECTEDMEASURE() < 1000,"$#,##0",
                    SELECTEDMEASURE() < 1000000, "$#,##0,.0K",
                    SELECTEDMEASURE() < 1000000000, "$#,##0,,.0M",
                    SELECTEDMEASURE() < 1000000000000,"$#,##0,,,.0B",
                    "$#,##0,,,,.0T"
                )
                """ 
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Costs",
            description = "Calculates the total costs from the 'Sales' table.",
            expression = "SUM(Sales[costs])",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit",
            description = "Calculates the profit by subtracting costs from sales.",
            expression = "[sales] - [costs]",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit Margin",
            description = "Calculates the profit margin by dividing the profit by sales, returning a blank value if the sales are zero.",
            expression = "DIVIDE([profit],[sales],BLANK())",
            format_string = "#,0.00%;-#,0.00%;#,0.00%"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Profit Per Order",
            description = "Calculates the average profit per order by dividing the total profit by the total number of orders.",
            expression = "DIVIDE([profit],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Sales Per Order",
            description = "Calculates the average sales per order by dividing the total sales by the total number of orders.",
            expression = "DIVIDE([sales],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Costs Per Order",
            description = "Calculates the average cost per order by dividing the total costs by the number of orders.",
            expression = "DIVIDE([Costs],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )

        print(f"{icons.checked} Measures created.")

        # Update columns
        tom.update_column(
            table_name="Pick a measure",
            column_name="Col1",
            hidden = True
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="Month",
            sort_by_column="MonthOfYear"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="MonthYear",
            sort_by_column="Monthly"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="DayOfWeek",
            sort_by_column="DayOfWeekNum"
        )
        tom.update_column(
            table_name="Date",
            column_name="Date",
            format_string="dd mmm yyyy"
        )
        tom.update_column(
            table_name="Date",
            column_name="Monthly",
            format_string="mmm yyyy"
        )

        print(f"{icons.checked} Table columns updated.")


        # Add calc items and a hierarchy
        tom.add_calculated_table(
            name="xTables",
            expression="INFO.VIEW.TABLES()"
        )
        tom.add_field_parameter(
            table_name="Field parameter",
            objects=["[Orders]", "[Sales]", "[Costs]", "[Profit]"],
            object_names=["Orders","Sales","Costs","Profit"]
        )
        tom.add_calculation_group(
            name="Time intelligence",
            precedence=1
        )
        tom.model.Tables['Time intelligence'].Columns['Name'].set_Name("Time calculation")
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="Current",
            expression="SELECTEDMEASURE()",
            ordinal=1
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="MTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESMTD('Date'[Date]))",
            ordinal=2
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="QTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESQTD('Date'[Date]))",
            ordinal=3
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))",
            ordinal=4
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY",
            expression="CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('Date'[Date]))",
            ordinal=5
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY MTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "MTD"
                )
            """,
            ordinal=6
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY QTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "QTD"
                )
            """,
            ordinal=7
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY",
            expression= f"""
            SELECTEDMEASURE() -
            CALCULATE(
                SELECTEDMEASURE(),
            'Time Intelligence'[Time Calculation] = "PY"
            )
            """,
            ordinal=8
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY%",
            expression= f"""
            DIVIDE(
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="YOY"
                ),
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="PY"
                )
            )
            """,
            format_string_expression = f""" "#,##0.0%" """,
            ordinal=9
        )

        print(f"{icons.checked} Calculation items added.")

        tom.add_hierarchy(
            table_name="Date",
            hierarchy_name="Calendar",
            columns=["Year","Month","Date"]
        )
        print(f"{icons.checked} Calendar hierarchy created.")

        # Clear the _tables_added list to skip any table refreshes.
        tom._tables_added.clear()

class SalesSampleQueries:
    def __init__(self):
        self.sample_queries = [
            ("Total Sales (Card)", """EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", SUM(Sales[Sales])
)"""),
            # ----------------------------
            ("Sales Over Time (Line Chart)", """EVALUATE
SUMMARIZE(
    'Date',
    'Date'[Date],
    "Sales Over Time", CALCULATE(SUM(Sales[Sales]))
)"""),
            # ----------------------------
            ("Sales by Product Category (Column Chart)", """EVALUATE
SUMMARIZE(
    'Product',
    'Product'[ProductCategory],
    "Sales by Product Category", CALCULATE(SUM(Sales[Sales]))
)"""),
            # ----------------------------
            ("Sales by Location (Map)", """EVALUATE
SUMMARIZE(
    Geography,
    Geography[StateOrTerritory],
    "Sales by Location", CALCULATE(SUM(Sales[Sales]))
)"""),
            # ----------------------------
            ("Total Profit (Card)", """EVALUATE
SUMMARIZE(
    Sales,
    Geography[Country],
    "Total Profit", VALUE([Profit])
)"""),
            # ----------------------------
            ("Monthly Sales Trends (Line Chart)", """EVALUATE
SUMMARIZE(
    'Date',
    'Date'[MonthYear],
    "Monthly Sales Trends", CALCULATE(SUM(Sales[Sales]))
)"""),
            # ----------------------------
            ("Period Comparison", """DEFINE
MEASURE Sales[Sum of Quantity] = SUM(Sales[Sales])
MEASURE Sales[Sum of Quantity PM] = CALCULATE([Sum of Quantity],PREVIOUSMONTH('date'[Date]))
MEASURE Sales[Sum of Quantity PM Delta] = [Sum of Quantity] - [Sum of Quantity PM] 
MEASURE Sales[Sum of Quantity PM %] = [Sum of Quantity PM Delta] / [Sum of Quantity]

EVALUATE
SUMMARIZECOLUMNS(
    'date'[Monthly] ,
    TREATAS({DATE(2023,1,1),DATE(2023,2,1),DATE(2023,3,1)} , 'Date'[Monthly] ) ,
    "Sales" , VALUE([Sum of Quantity]),
    "Sales PM" ,  VALUE([Sum of Quantity PM]),
    "Sales PM Delta", VALUE([Sum of Quantity PM Delta]),
    "Sales PM % " , VALUE([Sum of Quantity PM %])
)

ORDER BY [Monthly]"""),
            # ----------------------------
            ("Running Total", """DEFINE
MEASURE Sales[Sum of Sales] =  SUM(Sales[sales])
MEASURE Sales[Sum of Sales YTD] = TOTALYTD([Sum of Sales],'date'[Date])
MEASURE Sales[Sum of Sales QTD] = TOTALQTD([Sum of Sales],'date'[Date]) 

EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Monthly],
    TREATAS({DATE(2023,1,1)} , 'Date'[Monthly] ),
    "Sales" , VALUE([Sum of Sales]),
    "Sales YTD" , VALUE([Sum of Sales YTD]),
    "Sales QTD" , VALUE([Sum of Sales QTD])
)
ORDER BY [Monthly] """),
            # ----------------------------
        ]

    def to_test_suite(
        self,
        target_dataset: str | UUID,
        target_workspace: Optional[str | UUID] = None,
        master_dataset: Optional[str | UUID] = None,
        master_workspace: Optional[str | UUID] = None,
        data_source: Optional[str | UUID] = None,
        data_source_workspace: Optional[str | UUID] = None,
        data_source_type: Optional[str] = "Lakehouse",

    ) -> TestSuite:
        """
        Wraps the predefined sales sample DAX queries into a TestSuite instance
        based on the specified parameters.

        Parameters
        ----------
        target_dataset : str | uuid.UUID
            The semantic model name or ID designating the model that the 
            test cycle should use to run the DAX queries.
        target_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the target dataset is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        master_dataset : str | uuid.UUID, default=None
            The master semantic model name or ID for the target_dataset. If not 
            specified, the test cycle cannot clone the master to create the target_dataset.
            In this case, the target_dataset must already exist.
        master_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the master dataset is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        data_source : str | uuid.UUID, default=None
            The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
            Defaults to None which resolves to the lakehouse or warehouse referenced in the data source shared expression.
        data_source_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the data source is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        data_source_type : str, default=Lakehouse
            The type of the data source. Currently, the only supported type is Lakehouse.

        Returns
        -------
        TestSuite
            A TestSuite object with the test definitions based on the specified DAX queries.
        """
        return _get_test_definitions(
            dax_queries = self.sample_queries,
            target_dataset = target_dataset,
            target_workspace = target_workspace,
            master_dataset = master_dataset,
            master_workspace = master_workspace,
            data_source = data_source,
            data_source_workspace = data_source_workspace,
            data_source_type = data_source_type,
        )


def _get_workspace_name_and_id(
    workspace: Optional[str | UUID] = None,
) -> Tuple[str, UUID]:
    """
    Resolves a workspace name or ID into a Tuple of workspace name and id.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID to resolve.

    Returns
    -------
    Tuple[str, uuid.UUID]
        A tuple of workspace name and workspace id.
    """
    if workspace:
        filter_condition = urllib.parse.quote(workspace)
        dfW = fabric.list_workspaces(
            filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'"
        )
        if dfW.empty:
            workspace_name = workspace
            workspace_id = None
        else:
            workspace_name = dfW.iloc[0]["Name"]
            workspace_id = dfW.iloc[0]["Id"]
        
        return (workspace_name, workspace_id)
    return (None, None)


def _get_dataset_name_and_id(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> Tuple[str, UUID]:
    """
    Resolves a dataset name or ID into a Tuple of dataset name and id.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        The dataset name or ID to resolve.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the dataset is located.

    Returns
    -------
    Tuple[str, uuid.UUID]
        A tuple of dataset name and dataset id.
    """
    if dataset:
        dfSM = fabric.list_datasets(workspace=workspace, mode="rest")
        dfSM = dfSM[
            (dfSM["Dataset Name"] == dataset)
            | (dfSM["Dataset Id"] == dataset)
        ]
        if dfSM.empty:
            dataset_name = dataset
            dataset_id = None
        else:
            dataset_name = dfSM.iloc[0]["Dataset Name"]
            dataset_id = dfSM.iloc[0]["Dataset Id"]
        
        return (dataset_name, dataset_id)
    return (None, None)


def _get_lakehouse_name_and_id(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> Tuple[str, UUID]:
    """
    Resolves a lakehouse name or ID into a Tuple of lakehouse name and id.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The lakehouse name or ID to resolve.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the lakehouse is located.

    Returns
    -------
    Tuple[str, uuid.UUID]
        A tuple of lakehouse name and dataset id.
    """
    if lakehouse:
        dfLH = fabric.list_items(workspace=workspace, type = "Lakehouse")
        dfLH = dfLH[
            (dfLH["Display Name"] == lakehouse)
            | (dfLH["Id"] == lakehouse)
        ]
        if dfLH.empty:
            lakehouse_name = lakehouse
            lakehouse_id = None
        else:
            lakehouse_name = dfLH.iloc[0]["Display Name"]
            lakehouse_id = dfLH.iloc[0]["Id"]
        
        return (lakehouse_name, lakehouse_id)
    return (None, None)


def _get_test_definitions(
    dax_queries: list[str] | list[(str, str)],
    target_dataset: str | UUID,
    target_workspace: Optional[str | UUID] = None,
    master_dataset: Optional[str | UUID] = None,
    master_workspace: Optional[str | UUID] = None,
    data_source: Optional[str | UUID] = None,
    data_source_workspace: Optional[str | UUID] = None,
    data_source_type: Optional[str] = "Lakehouse",

) -> TestSuite:
    """
    Generates a TestSuite instance with test definitions based on a list of DAX queries.

    Parameters
    ----------
    dax_queries: list[str]
        A predefined list of DAX queries.
        This can be a simple list of query expressions,
        or a list of (Query_Id, Query_Text) tuples.
    target_dataset : str | uuid.UUID
        The semantic model name or ID designating the model that the 
        test cycle should use to run the DAX queries.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the target dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    master_dataset : str | uuid.UUID, default=None
        The master semantic model name or ID for the target_dataset. If not 
        specified, the test cycle cannot clone the master to create the target_dataset.
        In this case, the target_dataset must already exist.
    master_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the master dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source : str | uuid.UUID, default=None
        The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
        Defaults to None which resolves to the lakehouse or warehouse referenced in the data source shared expression.
    data_source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the data source is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source_type : str, default=Lakehouse
        The type of the data source. Currently, the only supported type is Lakehouse.

    Returns
    -------
    TestSuite
        A TestSuite object with the test definitions based on the specified DAX queries.
    """
    # Parameter validation
    if data_source_type != "Lakehouse":
        raise ValueError("Unrecognized data source type specified. The only valid option for now is 'Lakehouse'.")

    (target_workspace_name, target_workspace_id) = _get_workspace_name_and_id(target_workspace)
    (master_workspace_name, master_workspace_id) = _get_workspace_name_and_id(master_workspace)
    (data_source_workspace_name, data_source_workspace_id) = _get_workspace_name_and_id(data_source_workspace)

    (target_dataset_name, target_dataset_id) = _get_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
    (master_dataset_name, master_dataset_id) = _get_dataset_name_and_id(dataset=master_dataset, workspace=master_workspace_id)

    (data_source_name, data_source_id) = _get_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)

    test_suite = TestSuite()
    for i in range(len(dax_queries)):
        q = dax_queries[i]
        if isinstance(q, str):          
            test_suite.add_test_definition(
                TestDefinition(
                    QueryId=f"Q{i}", 
                    QueryText=q, 
                    MasterWorkspace = master_workspace_name,
                    MasterDataset = master_dataset_name,
                    TargetWorkspace = target_workspace_name,
                    TargetDataset= target_dataset_name,
                    DatasourceName = data_source_name,
                    DatasourceWorkspace = data_source_workspace_name,
                    DatasourceType = data_source_type))
        elif isinstance(q, tuple):
            test_suite.add_test_definition(
                TestDefinition(
                    QueryId=q[0], 
                    QueryText=q[1], 
                    MasterWorkspace = master_workspace_name,
                    MasterDataset = master_dataset_name,
                    TargetWorkspace = target_workspace_name,
                    TargetDataset= target_dataset_name,
                    DatasourceName = data_source_name,
                    DatasourceWorkspace = data_source_workspace_name,
                    DatasourceType = data_source_type))
            
    return test_suite

