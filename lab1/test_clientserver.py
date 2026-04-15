"""
Simple client server unit test
"""

import logging
import threading
import unittest

import clientserver
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


class TestEchoService(unittest.TestCase):
    """The test"""
    _server = clientserver.Server()  # create single server in class variable
    _server_thread = threading.Thread(target=_server.serve)  # define thread for running server

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()  # start server loop in a thread (called only once)

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client()  # create new client for each test

    def test_srv_get(self):  # each test_* function is a test
        """Test simple call"""
        msg = self.client.call("Hello VS2Lab")
        self.assertEqual(msg, 'Hello VS2Lab*')

    def tearDown(self):
        self.client.close()  # terminate client after each test

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = False  # break out of server loop. pylint: disable=protected-access
        cls._server_thread.join()  # wait for server thread to terminate


class TestPhoneNumberService(unittest.TestCase):
    """Test the phone number lookup service"""
    _server = clientserver.Server()
    _server_thread = threading.Thread(target=_server.serve)

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client()

    def test_get_existing_name(self):
        """Test getting phone number for existing person"""
        msg = self.client.get("Jamie")
        self.assertIn("42", msg)
        self.assertIn("Jamie", msg)

    def test_get_another_existing_name(self):
        """Test getting phone number for another existing person"""
        msg = self.client.get("Hannah")
        self.assertIn("37", msg)
        self.assertIn("Hannah", msg)

    def test_get_nonexistent_name(self):
        """Test getting phone number for non-existent person"""
        msg = self.client.get("Unknown")
        self.assertIn("No entry", msg)
        self.assertIn("Unknown", msg)

    def test_getAll(self):
        """Test getting all phone numbers"""
        msg = self.client.getAll()
        self.assertIn("Jamie", msg)
        self.assertIn("Hannah", msg)
        self.assertIn("Bec", msg)
        self.assertIn("42", msg)
        self.assertIn("37", msg)
        self.assertIn("55", msg)

    def tearDown(self):
        self.client.close()

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = False
        cls._server_thread.join()


if __name__ == '__main__':
    unittest.main()

