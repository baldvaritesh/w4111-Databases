from src.CSVDataTable import CSVDataTable
from tests.unit_tests import data_dir

# Connection data
connect_info = [{
    "directory": data_dir("../../../Data/ClassicModels"),
    "file_name": "customers.csv",
    "delimiter": ";",
    }, {
    "directory": data_dir("../../../Data/ClassicModels"),
    "file_name": "orders.csv",
    "delimiter": ";",
    }, {
    "directory": data_dir("../../../Data/ClassicModels"),
    "file_name": "orderdetails.csv",
    "delimiter": ";",
}]
key_columns = [["customerNumber"], ["orderNumber"], ["orderNumber", "orderLineNumber"]]

# Load everything
csv_tbl_customers = CSVDataTable("customers", connect_info[0], key_columns[0])
csv_tbl_orders = CSVDataTable("orders", connect_info[1], key_columns[1])
csv_tbl_order_details = CSVDataTable("orderdetails", connect_info[2], key_columns[2])


def revenue_report_csv(customer_number=None, order_number=None, order_status=None, country=None):
    # Obtain the data for given customer_number and country
    t1 = dict(zip(["customerNumber", "country"], [customer_number, country]))
    data1 = csv_tbl_customers.find_by_template({k: t1[k] for k in t1 if t1[k] is not None},
                                               ["customerNumber", "country"])

    # Given the data for customer_number obtain order_number and order_status
    t2 = dict(zip(["customerNumber", "orderNumber", "status"], [customer_number, order_number, order_status]))
    data2 = csv_tbl_orders.find_by_template({k: t2[k] for k in t2 if t2[k] is not None},
                                            ["customerNumber", "orderNumber", "status"])

    # Now obtain for the order number, get the corresponding details of prices and quantities
    data3 = csv_tbl_order_details.find_by_template({"orderNumber": order_number}, ["priceEach", "quantityOrdered"])
    print(data1)
    print("----------------------\n")
    print(data2)
    print("----------------------\n")
    print(data3)


revenue_report_csv(customer_number="112", order_number="10124", order_status="Shipped", country="USA")