from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    '''
     self.item_id = item_id
        self.product_name = product_name
        self.brand = brand
        self.category = category
        self.manufact = manufact
        self.current_price = current_price
        self.start_year = start_year
        self.num_owned = num_owned
    '''
    cur.execute("SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item")
    new_sk_id = cur.fetchone()[0]
    cur.execute("INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_category, i_manufact, i_current_price, i_num_owned) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (new_sk_id, new_item.item_id, f"{new_item.start_year}-01-01", new_item.product_name, new_item.brand, new_item.category, new_item.manufact, new_item.current_price, new_item.num_owned))
    # raise NotImplementedError("you must implement this function")


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    '''
    self.customer_id = customer_id
        self.name = name
        self.address = address
        self.email = email
    '''
    # get a new sk_id for customer
    cur.execute("SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer")
    new_sk_id = cur.fetchone()[0]
    # insert the address and get the id for the address
    cur.execute("SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address")
    new_address_id = cur.fetchone()[0]
    address = new_customer.address.split(", ")
    cur.execute("INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) VALUES (?, ?, ?, ?, ?, ?)",
                (new_address_id, address[0].split(" ", 1)[0], address[0].split(" ", 1)[1], address[1], address[2].split()[0], address[2].split()[1]))
    # insert the customer with the new address id
    name = new_customer.name.split()
    cur.execute("INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) VALUES (?, ?, ?, ?, ?, ?)",
                (new_sk_id, new_customer.customer_id, name[0], name[1], new_customer.email, new_address_id))
    # raise NotImplementedError("you must implement this function")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    # check name field
    if new_customer.name is not None:
        name = new_customer.name.split()
        cur.execute("UPDATE customer SET c_first_name = ?, c_last_name = ? WHERE c_customer_id = ?",
                    (name[0], name[1], original_customer_id))
        
    # check email field
    if new_customer.email is not None:
        cur.execute("UPDATE customer SET c_email_address = ? WHERE c_customer_id = ?",
                    (new_customer.email, original_customer_id))

    # check address field
    if new_customer.address is not None:
        # get the customer's current address id
        cur.execute("SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?", (original_customer_id,))
        address_id = cur.fetchone()[0]
        # update the address
        address = new_customer.address.split(", ")
        cur.execute("UPDATE customer_address SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ? WHERE ca_address_sk = ?",
                    (address[0].split(" ", 1)[0], address[0].split(" ", 1)[1], address[1], address[2].split()[0], address[2].split()[1], address_id))
    # raise NotImplementedError("you must implement this function")


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    '''
     self.item_id = item_id
        self.product_name = product_name
        self.brand = brand
        self.category = category
        self.manufact = manufact
        self.current_price = current_price
        self.start_year = start_year
        self.num_owned = num_owned
    '''
    cur.execute("SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned FROM item")
    items = []
    for (item_id, product_name, brand, category, manufact, current_price, start_year, num_owned) in cur:
        item = Item()
        if filter_attributes.item_id is not None:
            if use_patterns:
                if filter_attributes.item_id not in item_id:
                    continue
            else:
                if filter_attributes.item_id != item_id:
                    continue
        item.item_id = item_id
        if filter_attributes.product_name is not None:
            if use_patterns:
                if filter_attributes.product_name not in product_name:
                    continue
            else:
                if filter_attributes.product_name != product_name:
                    continue
        item.product_name = product_name
        if filter_attributes.brand is not None:
            if use_patterns:
                if filter_attributes.brand not in brand:
                    continue
            else:
                if filter_attributes.brand != brand:
                    continue
        item.brand = brand
        if filter_attributes.category is not None:
            if use_patterns:
                if filter_attributes.category not in category:
                    continue
            else:
                if filter_attributes.category != category:
                    continue
        item.category = category
        if filter_attributes.manufact is not None:
            if use_patterns:
                if filter_attributes.manufact not in manufact:
                    continue
            else:
                if filter_attributes.manufact != manufact:
                    continue
        item.manufact = manufact
        if filter_attributes.current_price is not None:
            if min_price != -1 and current_price < min_price:
                continue
            if max_price != -1 and current_price > max_price:
                continue
        item.current_price = current_price
        if filter_attributes.start_year is not None:
            if min_start_year != -1 and start_year < min_start_year:
                continue
            if max_start_year != -1 and start_year > max_start_year:
                continue
        item.start_year = start_year
        if filter_attributes.num_owned is not None:
            if filter_attributes.num_owned != num_owned:
                continue
        item.num_owned = num_owned
        items.append(item)
    return items
    # raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    raise NotImplementedError("you must implement this function")


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    raise NotImplementedError("you must implement this function")


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    raise NotImplementedError("you must implement this function")


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()
    # raise NotImplementedError("you must implement this function")


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

    # raise NotImplementedError("you must implement this function")

