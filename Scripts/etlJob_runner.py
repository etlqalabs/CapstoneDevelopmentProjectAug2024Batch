import Scripts.extract as etraction
import Scripts.transform as transformation
import Scripts.load as loading
import logging

# Configure the logging
logging.basicConfig(
    filename='logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Data Extraction started ...... ")
    etraction.load_csv_mysql('data/sales_data.csv', 'staging_sales')
    etraction.load_csv_mysql('data/product_data.csv', 'staging_product')
    etraction.load_xml_mysql('data/inventory_data.xml', 'staging_inventory')
    etraction.load_json_mysql('data/supplier_data.json', 'staging_supplier')
    etraction.load_oracle_mysql("select * from stores", 'staging_stores')
    logger.info("Data Extraction completed successfully...... ")

    transformation.filter_sales_data()
    transformation.router_sales_data()
    transformation.aggregate_sales_data()
    transformation.join_sales_data()
    transformation.aggregate_inventory_levels()

    loading.load_sales_fact()
    loading.load_inventory_fact()
    loading.load_monthly_sales_summary()
    loading.load_inventory_levels_by_store()
