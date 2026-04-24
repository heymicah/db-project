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

    # check customer_id field
    if new_customer.customer_id is not None:
        cur.execute("UPDATE customer SET c_customer_id = ? WHERE c_customer_id = ?",
                    (new_customer.customer_id, original_customer_id))
        original_customer_id = new_customer.customer_id

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
        if address_id is None:
            # if the customer doesn't have an address, insert a new one and update the customer record to point to it
            cur.execute("SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address")
            new_address_id = cur.fetchone()[0]
            address = new_customer.address.split(", ")
            cur.execute("INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) VALUES (?, ?, ?, ?, ?, ?)",
                        (new_address_id, address[0].split(" ", 1)[0], address[0].split(" ", 1)[1], address[1], address[2].split()[0], address[2].split()[1]))
            cur.execute("UPDATE customer SET c_current_addr_sk = ? WHERE c_customer_id = ?",
                        (new_address_id, original_customer_id))
        else:
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
    cur.execute("INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)",
                (item_id, customer_id, date.today(), date.today() + timedelta(days=14)))
    # raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    cur.execute("INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, COALESCE((SELECT MAX(place_in_line) + 1 FROM waitlist WHERE item_id = ?), 1))", 
                (item_id, customer_id, item_id))
    cur.execute("SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    return cur.fetchone()[0]
    # raise NotImplementedError("you must implement this function")

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    cur.execute("DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1", (item_id,))
    cur.execute("UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?", (item_id,))
    # raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    cur.execute("SELECT rental_date, due_date FROM rental WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    rental_info = cur.fetchone()
    cur.execute("INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) VALUES (?, ?, ?, ?, ?)",
                (item_id, customer_id, rental_info[0], rental_info[1], date.today()))
    cur.execute("DELETE FROM rental WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    # update the waitlist for the item
    update_waitlist(item_id)
    # raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    cur.execute("UPDATE rental SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY) WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    # raise NotImplementedError("you must implement this function")


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
    query = "SELECT i_item_id, i_rec_start_date, i_product_name, i_brand, i_category, i_manufact, i_current_price, i_num_owned FROM item"
    conditions = []
    params = []
    if filter_attributes.item_id is not None:
        if use_patterns:
            conditions.append("i_item_id LIKE ?")
        else:
            conditions.append("i_item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.product_name is not None:
        if use_patterns:
            conditions.append("i_product_name LIKE ?")
        else:
            conditions.append("i_product_name = ?")
        params.append(filter_attributes.product_name)
    if filter_attributes.brand is not None:
        if use_patterns:
            conditions.append("i_brand LIKE ?")
        else:
            conditions.append("i_brand = ?")
        params.append(filter_attributes.brand)
    if filter_attributes.category is not None:
        if use_patterns:
            conditions.append("i_category LIKE ?")
        else:
            conditions.append("i_category = ?")
        params.append(filter_attributes.category)
    if filter_attributes.manufact is not None:
        if use_patterns:
            conditions.append("i_manufact LIKE ?")
        else:
            conditions.append("i_manufact = ?")
        params.append(filter_attributes.manufact)
    if filter_attributes.current_price != -1:
        if min_price != -1:
            conditions.append("i_current_price >= ?")
            params.append(min_price)
        if max_price != -1:
            conditions.append("i_current_price <= ?")
            params.append(max_price)
        conditions.append("i_current_price = ?")
        params.append(filter_attributes.current_price)
    if filter_attributes.start_year != -1:
        if min_start_year != -1:
            conditions.append("YEAR(i_rec_start_date) >= ?")
            params.append(min_start_year)
        if max_start_year != -1:
            conditions.append("YEAR(i_rec_start_date) <= ?")
            params.append(max_start_year)
        conditions.append("YEAR(i_rec_start_date) = ?")
        params.append(filter_attributes.start_year)
    if filter_attributes.num_owned != -1:
        conditions.append("i_num_owned = ?")
        params.append(filter_attributes.num_owned)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    cur.execute(query, params)
    items = []
    for (item_id, rec_start_date, product_name, brand, category, manufact, current_price, num_owned) in cur:
        item = Item()
        item.item_id = item_id
        item.product_name = product_name
        item.brand = brand
        item.category = category
        item.manufact = manufact
        item.current_price = current_price
        item.start_year = rec_start_date.year
        item.num_owned = num_owned
        items.append(item)
    return items
    # raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    '''
    self.customer_id = customer_id
        self.name = name
        self.address = address
        self.email = email
    '''
    query = "SELECT c_customer_id, c_first_name, c_last_name, c_email_address, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip FROM customer LEFT JOIN customer_address ON c_current_addr_sk = ca_address_sk"
    conditions = []
    params = []
    if filter_attributes.customer_id is not None:
        if use_patterns:
            conditions.append("c_customer_id LIKE ?")
        else:
            conditions.append("c_customer_id = ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.name is not None:
        name = filter_attributes.name.split(" ", 1)
        if use_patterns:
            conditions.append("c_first_name LIKE ? AND c_last_name LIKE ?")
        else:
            conditions.append("c_first_name = ? AND c_last_name = ?")
        params.append(name[0])
        params.append(name[1])
    if filter_attributes.email is not None:
        if use_patterns:
            conditions.append("c_email_address LIKE ?")
        else:
            conditions.append("c_email_address = ?")
        params.append(filter_attributes.email)
    if filter_attributes.address is not None:
        address = filter_attributes.address.split(", ")
        if use_patterns:
            conditions.append("ca_street_number LIKE ? AND ca_street_name LIKE ? AND ca_city LIKE ? AND ca_state LIKE ? AND ca_zip LIKE ?")
        else:
            conditions.append("ca_street_number = ? AND ca_street_name = ? AND ca_city = ? AND ca_state = ? AND ca_zip = ?")
        params.append(address[0].split(" ", 1)[0])
        params.append(address[0].split(" ", 1)[1])
        params.append(address[1])
        params.append(address[2].split()[0])
        params.append(address[2].split()[1])
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    cur.execute(query, params)
    customers = []
    for (customer_id, first_name, last_name, email, street_number, street_name, city, state, zip_code) in cur:
        customer = Customer()
        customer.customer_id = customer_id
        customer.name = f"{first_name} {last_name}"
        customer.email = email
        customer.address = f"{street_number} {street_name}, {city}, {state} {zip_code}"
        customers.append(customer)
    return customers
    # raise NotImplementedError("you must implement this function")


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
    cur.execute("SELECT i_num_owned FROM item WHERE i_item_id = ?", (item_id,))
    result = cur.fetchone()
    if result is None:
        return -1
    num_owned = result[0]
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id = ?", (item_id,))
    num_rented = cur.fetchone()[0]
    return num_owned - num_rented
    # raise NotImplementedError("you must implement this function")


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    cur.execute("SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    result = cur.fetchone()
    if result is None:
        return -1
    return result[0]
    # raise NotImplementedError("you must implement this function")


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item_id,))
    return cur.fetchone()[0]
    # raise NotImplementedError("you must implement this function")


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

