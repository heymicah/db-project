from unittest import TestCase, main
from datetime import date, timedelta
from importlib import reload

import db_handler as db
from models.Item import Item
from models.Customer import Customer
from models.Rental import Rental
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist


# Fixed test IDs (16 chars each)
TEST_ITEM_ID      = "PRVTEST_ITEM0000"
TEST_ITEM_ID_2    = "PRVTEST_ITEM0001"
TEST_CUSTOMER_ID  = "PRVTEST_CUST0000"
TEST_CUSTOMER_ID2 = "PRVTEST_CUST0001"
TEST_CUSTOMER_ID3 = "PRVTEST_CUST0002"


class PrivateTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = reload(db)

    @classmethod
    def tearDownClass(cls):
        try:
            cls._reset(cls)
            cls.db.cur.close()
            cls.db.conn.close()
        except Exception:
            pass

    def _reset(self):
        self.db.cur.execute("DELETE FROM waitlist WHERE item_id IN (?, ?)", (TEST_ITEM_ID, TEST_ITEM_ID_2))
        self.db.cur.execute("DELETE FROM rental WHERE item_id IN (?, ?)", (TEST_ITEM_ID, TEST_ITEM_ID_2))
        self.db.cur.execute("DELETE FROM rental_history WHERE item_id IN (?, ?)", (TEST_ITEM_ID, TEST_ITEM_ID_2))
        self.db.cur.execute("DELETE FROM item WHERE i_item_id IN (?, ?)", (TEST_ITEM_ID, TEST_ITEM_ID_2))
        for cid in (TEST_CUSTOMER_ID, TEST_CUSTOMER_ID2, TEST_CUSTOMER_ID3, "PRVTEST_EDIT0000"):
            self.db.cur.execute("DELETE FROM customer WHERE c_customer_id = ?", (cid,))
        self.db.conn.commit()

    def setUp(self):
        self._reset()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def get_item(item_id=TEST_ITEM_ID, product_name="Private Test Item",
                 brand="PrivBrand", category="PrivCategory",
                 manufact="PrivManufact", price=29.99, year=2022, num_owned=3):
        return Item(
            item_id=item_id,
            product_name=product_name,
            brand=brand,
            category=category,
            manufact=manufact,
            current_price=price,
            start_year=year,
            num_owned=num_owned,
        )

    @staticmethod
    def get_customer(customer_id=TEST_CUSTOMER_ID, name="Private Tester",
                     email="private.tester@test.com",
                     address="1234 Test Blvd, Orlando, FL 32801"):
        return Customer(
            customer_id=customer_id,
            name=name,
            email=email,
            address=address,
        )

    def _insert_item(self, item_id=TEST_ITEM_ID, **kwargs):
        item = self.get_item(item_id=item_id, **kwargs)
        self.db.cur.execute(
            "INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, "
            "i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) "
            "VALUES ((SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item AS tmp), "
            "?, ?, ?, ?, NULL, ?, ?, ?, ?)",
            (item.item_id, f"{item.start_year}-01-01", item.product_name,
             item.brand, item.category, item.manufact, item.current_price, item.num_owned)
        )
        self.db.conn.commit()
        return item

    def _insert_customer(self, customer_id=TEST_CUSTOMER_ID, name="Private Tester",
                         email="private.tester@test.com",
                         address="1234 Test Blvd, Orlando, FL 32801"):
        customer = self.get_customer(customer_id=customer_id, name=name, email=email, address=address)
        parts = customer.address.split(", ")
        street_num = parts[0].split(" ", 1)[0]
        street_name = parts[0].split(" ", 1)[1]
        city = parts[1]
        state = parts[2].split()[0]
        zip_code = parts[2].split()[1]
        first_name, last_name = customer.name.split()

        self.db.cur.execute(
            "INSERT INTO customer_address "
            "(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) "
            "VALUES ((SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address AS tmp), "
            "?, ?, ?, ?, ?)",
            (street_num, street_name, city, state, zip_code)
        )
        self.db.cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
        addr_sk = self.db.cur.fetchone()[0]
        self.db.cur.execute(
            "INSERT INTO customer "
            "(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) "
            "VALUES ((SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer AS tmp), "
            "?, ?, ?, ?, ?)",
            (customer.customer_id, first_name, last_name, customer.email, addr_sk)
        )
        self.db.conn.commit()
        return customer

    def _insert_rental(self, item_id, customer_id, rental_date=None, due_date=None):
        if rental_date is None:
            rental_date = date.today().isoformat()
        if due_date is None:
            due_date = (date.today() + timedelta(days=14)).isoformat()
        self.db.cur.execute(
            "INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)",
            (item_id, customer_id, rental_date, due_date)
        )
        self.db.conn.commit()

    # =========================================================================
    # add_item tests
    # =========================================================================

    def test_add_item_all_fields_stored(self):
        """Verify every field of a newly added item is persisted correctly."""
        item = self.get_item()
        self.db.add_item(new_item=item)

        self.db.cur.execute(
            "SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, "
            "i_current_price, YEAR(i_rec_start_date), i_num_owned "
            "FROM item WHERE i_item_id = ?",
            (item.item_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(item.item_id, row[0].strip())
        self.assertEqual(item.product_name, row[1].strip())
        self.assertEqual(item.brand, row[2].strip())
        self.assertEqual(item.category, row[3].strip())
        self.assertEqual(item.manufact, row[4].strip())
        self.assertAlmostEqual(item.current_price, float(row[5]), places=2)
        self.assertEqual(item.start_year, row[6])
        self.assertEqual(item.num_owned, row[7])

    def test_add_item_zero_price(self):
        """Items with price 0 should be insertable."""
        item = self.get_item(price=0.00)
        self.db.add_item(new_item=item)

        self.db.cur.execute(
            "SELECT i_current_price FROM item WHERE i_item_id = ?", (item.item_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(0.00, float(row[0]), places=2)

    def test_add_item_large_num_owned(self):
        """Items with a large num_owned value should work."""
        item = self.get_item(num_owned=9999)
        self.db.add_item(new_item=item)

        self.db.cur.execute(
            "SELECT i_num_owned FROM item WHERE i_item_id = ?", (item.item_id,)
        )
        self.assertEqual(9999, self.db.cur.fetchone()[0])

    def test_add_two_different_items(self):
        """Adding two items with different IDs should both be present."""
        item1 = self.get_item(item_id=TEST_ITEM_ID)
        item2 = self.get_item(item_id=TEST_ITEM_ID_2, product_name="Second Item")
        self.db.add_item(new_item=item1)
        self.db.add_item(new_item=item2)

        self.db.cur.execute(
            "SELECT COUNT(*) FROM item WHERE i_item_id IN (?, ?)",
            (TEST_ITEM_ID, TEST_ITEM_ID_2)
        )
        self.assertEqual(2, self.db.cur.fetchone()[0])

    # =========================================================================
    # add_customer tests
    # =========================================================================

    def test_add_customer_all_fields_stored(self):
        """Verify all customer fields including address are persisted."""
        cust = self.get_customer()
        self.db.add_customer(new_customer=cust)

        self.db.cur.execute(
            "SELECT c_customer_id, TRIM(c_first_name), TRIM(c_last_name), "
            "TRIM(c_email_address) FROM customer WHERE c_customer_id = ?",
            (cust.customer_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(cust.customer_id, row[0].strip())
        self.assertEqual("Private", row[1])
        self.assertEqual("Tester", row[2])
        self.assertEqual(cust.email, row[3])

    def test_add_customer_address_persisted(self):
        """Verify the address record is created and linked correctly."""
        cust = self.get_customer()
        self.db.add_customer(new_customer=cust)

        self.db.cur.execute(
            "SELECT ca_street_number, ca_street_name, ca_city, ca_state, ca_zip "
            "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk "
            "WHERE c_customer_id = ?",
            (cust.customer_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("1234", row[0].strip())
        self.assertEqual("Test Blvd", row[1].strip())
        self.assertEqual("Orlando", row[2].strip())
        self.assertEqual("FL", row[3].strip())
        self.assertEqual("32801", row[4].strip())

    def test_add_two_customers(self):
        """Adding two customers with different IDs should both exist."""
        c1 = self.get_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self.get_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                               email="second@test.com")
        self.db.add_customer(new_customer=c1)
        self.db.add_customer(new_customer=c2)

        self.db.cur.execute(
            "SELECT COUNT(*) FROM customer WHERE c_customer_id IN (?, ?)",
            (TEST_CUSTOMER_ID, TEST_CUSTOMER_ID2)
        )
        self.assertEqual(2, self.db.cur.fetchone()[0])

    # =========================================================================
    # edit_customer tests
    # =========================================================================

    def test_edit_customer_only_email(self):
        """Editing only the email should leave other fields untouched."""
        self._insert_customer()
        updated = Customer(email="newemail@test.com")

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT TRIM(c_first_name), TRIM(c_last_name), TRIM(c_email_address) "
            "FROM customer WHERE c_customer_id = ?",
            (TEST_CUSTOMER_ID,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("Private", row[0])
        self.assertEqual("Tester", row[1])
        self.assertEqual("newemail@test.com", row[2])

    def test_edit_customer_only_name(self):
        """Editing only the name should leave email and address untouched."""
        self._insert_customer()
        updated = Customer(name="New Name")

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT TRIM(c_first_name), TRIM(c_last_name), TRIM(c_email_address) "
            "FROM customer WHERE c_customer_id = ?",
            (TEST_CUSTOMER_ID,)
        )
        row = self.db.cur.fetchone()
        self.assertEqual("New", row[0])
        self.assertEqual("Name", row[1])
        self.assertEqual("private.tester@test.com", row[2])

    def test_edit_customer_only_address(self):
        """Editing only the address should update the address record."""
        self._insert_customer()
        updated = Customer(address="9999 New Rd, Tampa, FL 33601")

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT ca_street_number, ca_street_name, ca_city, ca_state, ca_zip "
            "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk "
            "WHERE c_customer_id = ?",
            (TEST_CUSTOMER_ID,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("9999", row[0].strip())
        self.assertEqual("New Rd", row[1].strip())
        self.assertEqual("Tampa", row[2].strip())
        self.assertEqual("FL", row[3].strip())
        self.assertEqual("33601", row[4].strip())

    def test_edit_customer_change_id(self):
        """Changing customer_id should remove the old ID and create the new one."""
        self._insert_customer()
        updated = Customer(customer_id="PRVTEST_EDIT0000")

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT c_customer_id FROM customer WHERE c_customer_id = ?",
            (TEST_CUSTOMER_ID,)
        )
        self.assertIsNone(self.db.cur.fetchone())

        self.db.cur.execute(
            "SELECT c_customer_id FROM customer WHERE c_customer_id = ?",
            ("PRVTEST_EDIT0000",)
        )
        self.assertIsNotNone(self.db.cur.fetchone())

        # cleanup
        self.db.cur.execute("DELETE FROM customer WHERE c_customer_id = ?", ("PRVTEST_EDIT0000",))
        self.db.conn.commit()

    def test_edit_customer_all_fields(self):
        """Editing all fields at once should update everything."""
        self._insert_customer()
        updated = Customer(
            customer_id="PRVTEST_EDIT0000",
            name="Completely Different",
            email="different@test.com",
            address="5555 Oak St, Miami, FL 33101",
        )

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT TRIM(c_first_name), TRIM(c_last_name), TRIM(c_email_address) "
            "FROM customer WHERE c_customer_id = ?",
            ("PRVTEST_EDIT0000",)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("Completely", row[0])
        self.assertEqual("Different", row[1])
        self.assertEqual("different@test.com", row[2])

        # cleanup
        self.db.cur.execute("DELETE FROM customer WHERE c_customer_id = ?", ("PRVTEST_EDIT0000",))
        self.db.conn.commit()

    def test_edit_customer_no_changes(self):
        """Passing all None fields should leave the customer unchanged."""
        self._insert_customer()
        updated = Customer()

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        self.db.cur.execute(
            "SELECT TRIM(c_first_name), TRIM(c_last_name), TRIM(c_email_address) "
            "FROM customer WHERE c_customer_id = ?",
            (TEST_CUSTOMER_ID,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("Private", row[0])
        self.assertEqual("Tester", row[1])
        self.assertEqual("private.tester@test.com", row[2])

    # =========================================================================
    # rent_item tests
    # =========================================================================

    def test_rent_item_creates_rental_record(self):
        """Renting an item should insert a row in the rental table."""
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.rent_item(item.item_id, customer.customer_id)

        self.db.cur.execute(
            "SELECT item_id, customer_id, rental_date, due_date FROM rental "
            "WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(date.today().isoformat(), str(row[2]))
        self.assertEqual((date.today() + timedelta(days=14)).isoformat(), str(row[3]))

    def test_rent_item_decreases_stock(self):
        """Renting an item should decrease the number_in_stock by 1."""
        item = self._insert_item(num_owned=3)
        customer = self._insert_customer()

        stock_before = self.db.number_in_stock(item.item_id)
        self.db.rent_item(item.item_id, customer.customer_id)
        stock_after = self.db.number_in_stock(item.item_id)

        self.assertEqual(stock_before - 1, stock_after)

    def test_rent_multiple_copies(self):
        """Multiple customers renting the same item should each get a rental."""
        item = self._insert_item(num_owned=5)
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self.db.rent_item(item.item_id, c1.customer_id)
        self.db.rent_item(item.item_id, c2.customer_id)

        self.assertEqual(3, self.db.number_in_stock(item.item_id))

        self.db.cur.execute(
            "SELECT COUNT(*) FROM rental WHERE item_id = ?", (item.item_id,)
        )
        self.assertEqual(2, self.db.cur.fetchone()[0])

    # =========================================================================
    # return_item tests
    # =========================================================================

    def test_return_item_removes_rental(self):
        """Returning an item should remove it from the rental table."""
        item = self._insert_item()
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)

        self.db.cur.execute(
            "SELECT * FROM rental WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        self.assertIsNone(self.db.cur.fetchone())

    def test_return_item_creates_history(self):
        """Returning an item should insert a record into rental_history."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today, due)

        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)

        self.db.cur.execute(
            "SELECT rental_date, due_date, return_date FROM rental_history "
            "WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(today, str(row[0]))
        self.assertEqual(due, str(row[1]))
        self.assertEqual(today, str(row[2]))

    def test_return_item_restores_stock(self):
        """Stock should increase by 1 after returning an item."""
        item = self._insert_item(num_owned=3)
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        stock_before = self.db.number_in_stock(item.item_id)
        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)
        stock_after = self.db.number_in_stock(item.item_id)

        self.assertEqual(stock_before + 1, stock_after)

    def test_return_item_calls_update_waitlist(self):
        """Returning an item should pop the first person off the waitlist."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")
        self._insert_rental(item.item_id, c1.customer_id)

        # Put c2 on the waitlist at position 1
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c2.customer_id, 1)
        )
        self.db.conn.commit()

        self.db.return_item(item_id=item.item_id, customer_id=c1.customer_id)

        # Waitlist should be empty now (c2 was at position 1, gets removed)
        self.db.cur.execute(
            "SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item.item_id,)
        )
        self.assertEqual(0, self.db.cur.fetchone()[0])

    # =========================================================================
    # grant_extension tests
    # =========================================================================

    def test_grant_extension_adds_14_days(self):
        """Extension should add exactly 14 days to the due date."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        original_due = (date.today() + timedelta(days=14)).isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today, original_due)

        self.db.grant_extension(item_id=item.item_id, customer_id=customer.customer_id)

        self.db.cur.execute(
            "SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        new_due = str(self.db.cur.fetchone()[0])
        expected = (date.today() + timedelta(days=28)).isoformat()
        self.assertEqual(expected, new_due)

    def test_grant_extension_does_not_change_rental_date(self):
        """Extension should not alter the rental_date."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today)

        self.db.grant_extension(item_id=item.item_id, customer_id=customer.customer_id)

        self.db.cur.execute(
            "SELECT rental_date FROM rental WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        self.assertEqual(today, str(self.db.cur.fetchone()[0]))

    # =========================================================================
    # waitlist_customer tests
    # =========================================================================

    def test_waitlist_first_customer_gets_position_1(self):
        """First customer on waitlist should be at position 1."""
        item = self._insert_item()
        customer = self._insert_customer()

        place = self.db.waitlist_customer(item_id=item.item_id, customer_id=customer.customer_id)
        self.assertEqual(1, place)

    def test_waitlist_second_customer_gets_position_2(self):
        """Second customer on waitlist should be at position 2."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self.db.waitlist_customer(item_id=item.item_id, customer_id=c1.customer_id)
        place = self.db.waitlist_customer(item_id=item.item_id, customer_id=c2.customer_id)
        self.assertEqual(2, place)

    def test_waitlist_three_customers_sequential(self):
        """Three customers should get positions 1, 2, 3."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")
        c3 = self._insert_customer(customer_id=TEST_CUSTOMER_ID3, name="Third Customer",
                                   email="c3@test.com", address="3333 Pine St, Miami, FL 33101")

        p1 = self.db.waitlist_customer(item_id=item.item_id, customer_id=c1.customer_id)
        p2 = self.db.waitlist_customer(item_id=item.item_id, customer_id=c2.customer_id)
        p3 = self.db.waitlist_customer(item_id=item.item_id, customer_id=c3.customer_id)

        self.assertEqual(1, p1)
        self.assertEqual(2, p2)
        self.assertEqual(3, p3)

    def test_waitlist_different_items_independent(self):
        """Waitlist positions should be independent per item."""
        item1 = self._insert_item(item_id=TEST_ITEM_ID)
        item2 = self._insert_item(item_id=TEST_ITEM_ID_2, product_name="Other Item")
        customer = self._insert_customer()

        p1 = self.db.waitlist_customer(item_id=item1.item_id, customer_id=customer.customer_id)
        p2 = self.db.waitlist_customer(item_id=item2.item_id, customer_id=customer.customer_id)

        self.assertEqual(1, p1)
        self.assertEqual(1, p2)

    # =========================================================================
    # update_waitlist tests
    # =========================================================================

    def test_update_waitlist_removes_first(self):
        """Position 1 should be removed after update_waitlist."""
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.conn.commit()

        self.db.update_waitlist(item_id=item.item_id)

        self.db.cur.execute(
            "SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item.item_id,)
        )
        self.assertEqual(0, self.db.cur.fetchone()[0])

    def test_update_waitlist_shifts_positions(self):
        """After removing position 1, positions 2 and 3 should become 1 and 2."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")
        c3 = self._insert_customer(customer_id=TEST_CUSTOMER_ID3, name="Third Customer",
                                   email="c3@test.com", address="3333 Pine St, Miami, FL 33101")

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c1.customer_id, 1)
        )
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c2.customer_id, 2)
        )
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c3.customer_id, 3)
        )
        self.db.conn.commit()

        self.db.update_waitlist(item_id=item.item_id)

        # c1 should be gone
        self.assertEqual(-1, self.db.place_in_line(item.item_id, c1.customer_id))
        # c2 should now be 1, c3 should now be 2
        self.assertEqual(1, self.db.place_in_line(item.item_id, c2.customer_id))
        self.assertEqual(2, self.db.place_in_line(item.item_id, c3.customer_id))

    def test_update_waitlist_empty_is_noop(self):
        """Calling update_waitlist on an empty waitlist should not error."""
        item = self._insert_item()
        self.db.update_waitlist(item_id=item.item_id)

        self.db.cur.execute(
            "SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item.item_id,)
        )
        self.assertEqual(0, self.db.cur.fetchone()[0])

    # =========================================================================
    # number_in_stock tests
    # =========================================================================

    def test_number_in_stock_no_rentals(self):
        """With no rentals, stock should equal num_owned."""
        item = self._insert_item(num_owned=5)
        self.assertEqual(5, self.db.number_in_stock(item.item_id))

    def test_number_in_stock_with_rentals(self):
        """Stock should decrease by the number of active rentals."""
        item = self._insert_item(num_owned=5)
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self._insert_rental(item.item_id, c1.customer_id)
        self._insert_rental(item.item_id, c2.customer_id)

        self.assertEqual(3, self.db.number_in_stock(item.item_id))

    def test_number_in_stock_all_rented(self):
        """When all copies are rented, stock should be 0."""
        item = self._insert_item(num_owned=1)
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        self.assertEqual(0, self.db.number_in_stock(item.item_id))

    def test_number_in_stock_nonexistent_item(self):
        """Non-existent item should return -1."""
        self.assertEqual(-1, self.db.number_in_stock("NONEXISTENT_ITEM"))

    # =========================================================================
    # place_in_line tests
    # =========================================================================

    def test_place_in_line_not_on_waitlist(self):
        """Customer not on waitlist should return -1."""
        item = self._insert_item()
        customer = self._insert_customer()
        self.assertEqual(-1, self.db.place_in_line(item.item_id, customer.customer_id))

    def test_place_in_line_on_waitlist(self):
        """Customer on waitlist should return their position."""
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 3)
        )
        self.db.conn.commit()

        self.assertEqual(3, self.db.place_in_line(item.item_id, customer.customer_id))

    # =========================================================================
    # line_length tests
    # =========================================================================

    def test_line_length_empty(self):
        """Empty waitlist should return 0."""
        item = self._insert_item()
        self.assertEqual(0, self.db.line_length(item.item_id))

    def test_line_length_multiple(self):
        """Line length should equal number of waitlist entries for item."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c1.customer_id, 1)
        )
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c2.customer_id, 2)
        )
        self.db.conn.commit()

        self.assertEqual(2, self.db.line_length(item.item_id))

    # =========================================================================
    # get_filtered_items tests
    # =========================================================================

    def test_get_filtered_items_by_brand(self):
        """Filter by brand should return matching items."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(brand="PrivBrand"), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_by_category(self):
        """Filter by category should return matching items."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(category="PrivCategory"), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_by_manufact(self):
        """Filter by manufacturer should return matching items."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(manufact="PrivManufact"), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_by_price(self):
        """Filter by exact current_price should return matching items."""
        self._insert_item(price=29.99)
        results = self.db.get_filtered_items(
            filter_attributes=Item(current_price=29.99), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_by_start_year(self):
        """Filter by start_year should return matching items."""
        self._insert_item(year=2022)
        results = self.db.get_filtered_items(
            filter_attributes=Item(start_year=2022), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_by_num_owned(self):
        """Filter by num_owned should return matching items."""
        self._insert_item(num_owned=3)
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=TEST_ITEM_ID, num_owned=3), use_patterns=False
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_no_match(self):
        """Filter with non-matching values should return empty list."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id="NONEXISTENT_ITEM"), use_patterns=False
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_items_pattern_brand(self):
        """LIKE pattern on brand should match partial strings."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(brand="Priv%"), use_patterns=True
        )
        ids = [r.item_id for r in results]
        self.assertIn(TEST_ITEM_ID, ids)

    def test_get_filtered_items_min_start_year(self):
        """Filter with min_start_year should only return items >= that year."""
        self._insert_item(year=2022)
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=TEST_ITEM_ID),
            use_patterns=False, min_start_year=2020
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_items_max_start_year(self):
        """Filter with max_start_year should only return items <= that year."""
        self._insert_item(year=2022)
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=TEST_ITEM_ID),
            use_patterns=False, max_start_year=2025
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_items_year_range_excludes(self):
        """Item outside the year range should not be returned."""
        self._insert_item(year=2022)
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=TEST_ITEM_ID),
            use_patterns=False, min_start_year=2023
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_items_multiple_filters(self):
        """Multiple filters should be ANDed together."""
        self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=TEST_ITEM_ID, brand="PrivBrand"),
            use_patterns=False
        )
        self.assertEqual(1, len(results))
        self.assertEqual(TEST_ITEM_ID, results[0].item_id)

    def test_get_filtered_items_returns_correct_model(self):
        """Returned Items should have all fields populated correctly."""
        item = self._insert_item()
        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=item.item_id), use_patterns=False
        )
        self.assertEqual(1, len(results))
        r = results[0]
        self.assertEqual(item.product_name, r.product_name)
        self.assertEqual(item.brand, r.brand)
        self.assertEqual(item.category, r.category)
        self.assertEqual(item.manufact, r.manufact)
        self.assertAlmostEqual(item.current_price, float(r.current_price), places=2)
        self.assertEqual(item.start_year, r.start_year)
        self.assertEqual(item.num_owned, r.num_owned)

    # =========================================================================
    # get_filtered_customers tests
    # =========================================================================

    def test_get_filtered_customers_by_name(self):
        """Filter by name should return matching customers."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(name="Private Tester")
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_get_filtered_customers_by_email(self):
        """Filter by email should return matching customers."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(email="private.tester@test.com")
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_get_filtered_customers_by_address(self):
        """Filter by address should return matching customers."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(address="1234 Test Blvd, Orlando, FL 32801")
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_get_filtered_customers_no_match(self):
        """Non-matching filter should return empty list."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(customer_id="NONEXISTENT_CUST")
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_customers_pattern_name(self):
        """LIKE pattern on name should match partial strings."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(name="Priv% Test%"),
            use_patterns=True
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_get_filtered_customers_pattern_id(self):
        """LIKE pattern on customer_id should match partial strings."""
        self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(customer_id="PRVTEST%"),
            use_patterns=True
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_get_filtered_customers_returns_correct_model(self):
        """Returned Customer should have all fields populated."""
        cust = self._insert_customer()
        results = self.db.get_filtered_customers(
            filter_attributes=Customer(customer_id=cust.customer_id)
        )
        self.assertEqual(1, len(results))
        r = results[0]
        self.assertEqual(cust.email, r.email)
        self.assertIn("Orlando", r.address)
        self.assertIn("Private", r.name)

    # =========================================================================
    # get_filtered_rentals tests
    # =========================================================================

    def test_get_filtered_rentals_by_item_id(self):
        """Filter rentals by item_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(item.item_id, results[0].item_id.strip())

    def test_get_filtered_rentals_by_customer_id(self):
        """Filter rentals by customer_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(customer.customer_id, results[0].customer_id.strip())

    def test_get_filtered_rentals_by_both_ids(self):
        """Filter rentals by both item_id and customer_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        self._insert_rental(item.item_id, customer.customer_id)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_rentals_no_match(self):
        """Non-matching filter returns empty."""
        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id="NONEXISTENT_ITEM")
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_rentals_min_rental_date(self):
        """Filter by min_rental_date should exclude earlier rentals."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id),
            min_rental_date=today
        )
        self.assertEqual(1, len(results))

        # Future date should exclude it
        future = (date.today() + timedelta(days=1)).isoformat()
        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id),
            min_rental_date=future
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_rentals_max_rental_date(self):
        """Filter by max_rental_date should exclude later rentals."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id),
            max_rental_date=today
        )
        self.assertEqual(1, len(results))

        # Past date should exclude it
        past = (date.today() - timedelta(days=1)).isoformat()
        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id),
            max_rental_date=past
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_rentals_due_date_range(self):
        """Filter by min/max due_date should work."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today, due)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id),
            min_due_date=today, max_due_date=due
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_rentals_returns_correct_model(self):
        """Returned Rental should have all fields populated."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()
        self._insert_rental(item.item_id, customer.customer_id, today, due)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))
        r = results[0]
        self.assertEqual(today, str(r.rental_date))
        self.assertEqual(due, str(r.due_date))

    # =========================================================================
    # get_filtered_rental_histories tests
    # =========================================================================

    def test_get_filtered_rental_histories_basic(self):
        """Should return history entries matching item_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, due, today)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id)
        )
        self.assertGreaterEqual(len(results), 1)
        ids = [r.item_id.strip() for r in results]
        self.assertIn(item.item_id, ids)

    def test_get_filtered_rental_histories_by_customer(self):
        """Filter rental history by customer_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, due, today)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(customer_id=customer.customer_id)
        )
        self.assertGreaterEqual(len(results), 1)

    def test_get_filtered_rental_histories_return_date_range(self):
        """Filter rental history by min/max return_date."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, due, today)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id),
            min_return_date=today, max_return_date=today
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_rental_histories_no_match(self):
        """Non-matching filter returns empty."""
        results = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id="NONEXISTENT_ITEM")
        )
        self.assertEqual(0, len(results))

    def test_get_filtered_rental_histories_returns_correct_model(self):
        """Returned RentalHistory should have all fields."""
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, due, today)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))
        r = results[0]
        self.assertEqual(today, str(r.rental_date))
        self.assertEqual(due, str(r.due_date))
        self.assertEqual(today, str(r.return_date))

    # =========================================================================
    # get_filtered_waitlist tests
    # =========================================================================

    def test_get_filtered_waitlist_by_item(self):
        """Filter waitlist by item_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(item_id=item.item_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(item.item_id, results[0].item_id.strip())

    def test_get_filtered_waitlist_by_customer(self):
        """Filter waitlist by customer_id."""
        item = self._insert_item()
        customer = self._insert_customer()
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))

    def test_get_filtered_waitlist_by_place(self):
        """Filter waitlist by exact place_in_line."""
        item = self._insert_item()
        customer = self._insert_customer()
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 5)
        )
        self.db.conn.commit()

        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(place_in_line=5)
        )
        ids = [r.customer_id.strip() for r in results]
        self.assertIn(customer.customer_id, ids)

    def test_get_filtered_waitlist_min_max_place(self):
        """Filter waitlist by min/max place_in_line range."""
        item = self._insert_item()
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c1.customer_id, 1)
        )
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, c2.customer_id, 5)
        )
        self.db.conn.commit()

        # Only position 1 should match
        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(item_id=item.item_id),
            min_place_in_line=1, max_place_in_line=3
        )
        self.assertEqual(1, len(results))
        self.assertEqual(c1.customer_id, results[0].customer_id.strip())

    def test_get_filtered_waitlist_no_match(self):
        """Non-matching filter returns empty."""
        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(item_id="NONEXISTENT_ITEM")
        )
        self.assertEqual(0, len(results))

    # =========================================================================
    # save_changes / close_connection tests
    # =========================================================================

    def test_save_changes_persists_data(self):
        """Data inserted then saved should survive a reconnection."""
        item = self.get_item()
        self.db.add_item(new_item=item)
        self.db.save_changes()

        self.db.cur.close()
        self.db.conn.close()
        self.db = reload(db)

        self.db.cur.execute(
            "SELECT i_item_id FROM item WHERE i_item_id = ?", (item.item_id,)
        )
        result = self.db.cur.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(item.item_id, result[0].strip())

    def test_close_connection_reduces_process_count(self):
        """Closing should reduce the number of DB connections by 1."""
        from MARIADB_CREDS import DB_CONFIG
        from mariadb import connect
        conn2 = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"],
                        host=DB_CONFIG["host"], database=DB_CONFIG["database"],
                        port=DB_CONFIG["port"])
        cur2 = conn2.cursor()

        cur2.execute("SHOW PROCESSLIST")
        count_before = len(cur2.fetchall())

        self.db.close_connection()

        cur2.execute("SHOW PROCESSLIST")
        count_after = len(cur2.fetchall())

        self.assertEqual(count_before - 1, count_after)

        cur2.close()
        conn2.close()
        self.db = reload(db)

    # =========================================================================
    # Integration tests: functions working together
    # =========================================================================

    def test_full_rent_return_cycle(self):
        """Complete rent -> verify rental -> return -> verify history flow."""
        item = self._insert_item(num_owned=2)
        customer = self._insert_customer()

        # Rent
        self.db.rent_item(item.item_id, customer.customer_id)
        self.assertEqual(1, self.db.number_in_stock(item.item_id))

        # Verify rental exists via get_filtered_rentals
        rentals = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(rentals))

        # Return
        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)
        self.assertEqual(2, self.db.number_in_stock(item.item_id))

        # Verify rental gone
        rentals = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(0, len(rentals))

        # Verify history
        histories = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(histories))
        self.assertEqual(date.today().isoformat(), str(histories[0].return_date))

    def test_rent_with_extension_then_return(self):
        """Rent -> extend -> verify extended due date -> return."""
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.rent_item(item.item_id, customer.customer_id)
        self.db.grant_extension(item_id=item.item_id, customer_id=customer.customer_id)

        # Verify extended due date
        rentals = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(rentals))
        expected_due = (date.today() + timedelta(days=28)).isoformat()
        self.assertEqual(expected_due, str(rentals[0].due_date))

        # Return
        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)

        # History should have the extended due date
        histories = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(histories))
        self.assertEqual(expected_due, str(histories[0].due_date))

    def test_waitlist_then_return_shifts_queue(self):
        """Waitlist 3 customers, return item, verify queue shift."""
        item = self._insert_item(num_owned=1)
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")
        c3 = self._insert_customer(customer_id=TEST_CUSTOMER_ID3, name="Third Customer",
                                   email="c3@test.com", address="3333 Pine St, Miami, FL 33101")

        # c1 rents the only copy
        self.db.rent_item(item.item_id, c1.customer_id)
        self.assertEqual(0, self.db.number_in_stock(item.item_id))

        # c2 and c3 join waitlist
        self.db.waitlist_customer(item_id=item.item_id, customer_id=c2.customer_id)
        self.db.waitlist_customer(item_id=item.item_id, customer_id=c3.customer_id)
        self.assertEqual(2, self.db.line_length(item.item_id))

        # c1 returns the item (triggers update_waitlist: removes position 1 = c2)
        self.db.return_item(item_id=item.item_id, customer_id=c1.customer_id)

        # c2 should be off the waitlist, c3 should now be position 1
        self.assertEqual(-1, self.db.place_in_line(item.item_id, c2.customer_id))
        self.assertEqual(1, self.db.place_in_line(item.item_id, c3.customer_id))
        self.assertEqual(1, self.db.line_length(item.item_id))

    def test_add_item_then_filter_finds_it(self):
        """Add an item via add_item, then find it via get_filtered_items."""
        item = self.get_item()
        self.db.add_item(new_item=item)

        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=item.item_id), use_patterns=False
        )
        self.assertEqual(1, len(results))
        self.assertEqual(item.product_name, results[0].product_name)

    def test_add_customer_then_filter_finds_it(self):
        """Add a customer via add_customer, then find it via get_filtered_customers."""
        cust = self.get_customer()
        self.db.add_customer(new_customer=cust)

        results = self.db.get_filtered_customers(
            filter_attributes=Customer(customer_id=cust.customer_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(cust.email, results[0].email)

    def test_edit_customer_then_filter_by_new_values(self):
        """Edit customer email, then filter by the new email."""
        self._insert_customer()
        self.db.edit_customer(
            original_customer_id=TEST_CUSTOMER_ID,
            new_customer=Customer(email="updated@test.com")
        )

        results = self.db.get_filtered_customers(
            filter_attributes=Customer(email="updated@test.com")
        )
        ids = [r.customer_id for r in results]
        self.assertIn(TEST_CUSTOMER_ID, ids)

    def test_multiple_rentals_filtered_correctly(self):
        """Two different customers renting the same item should both show up."""
        item = self._insert_item(num_owned=5)
        c1 = self._insert_customer(customer_id=TEST_CUSTOMER_ID)
        c2 = self._insert_customer(customer_id=TEST_CUSTOMER_ID2, name="Second Customer",
                                   email="c2@test.com", address="2222 Elm St, Tampa, FL 33601")

        self.db.rent_item(item.item_id, c1.customer_id)
        self.db.rent_item(item.item_id, c2.customer_id)

        results = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id)
        )
        self.assertEqual(2, len(results))
        cust_ids = [r.customer_id.strip() for r in results]
        self.assertIn(c1.customer_id, cust_ids)
        self.assertIn(c2.customer_id, cust_ids)

    def test_rent_return_rent_again(self):
        """A customer can rent, return, and re-rent the same item."""
        item = self._insert_item(num_owned=1)
        customer = self._insert_customer()

        # First rental
        self.db.rent_item(item.item_id, customer.customer_id)
        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)

        # Second rental
        self.db.rent_item(item.item_id, customer.customer_id)

        # Should have 1 active rental and 1 history entry
        rentals = self.db.get_filtered_rentals(
            filter_attributes=Rental(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(rentals))

        histories = self.db.get_filtered_rental_histories(
            filter_attributes=RentalHistory(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertGreaterEqual(len(histories), 1)

    def test_waitlist_customer_via_api_then_query_via_filtered(self):
        """Add to waitlist via waitlist_customer, query via get_filtered_waitlist."""
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.waitlist_customer(item_id=item.item_id, customer_id=customer.customer_id)

        results = self.db.get_filtered_waitlist(
            filter_attributes=Waitlist(item_id=item.item_id, customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(1, results[0].place_in_line)


if __name__ == '__main__':
    main()
