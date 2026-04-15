"""
Client and server using classes
"""

import logging
import socket

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True
    _database = {"Jamie": 42, "Hannah": 37, "Bec": 55}

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def serve(self):
        """ Serve echo """
        self.sock.listen(1)
        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                # pylint: disable=unused-variable
                (connection, address) = self.sock.accept()  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024)  # receive data from client
                    if not data:
                        break  # stop if client stopped

                    message = data.decode('ascii')
                    
                    if message.startswith("GET "):
                        name = message[4:]  # Extract name after "GET "
                        self._logger.info("Requested Phone Number for " + name)
                        if name in self._database:
                            response = str(self._database[name])  # Get the number
                        else:
                            response = "No entry for " + name 
                        connection.send(response.encode('ascii'))
                    elif message == "GETALL":
                        self._logger.info("Requested All Phone Numbers")
                        response = str(self._database)
                        connection.send(response.encode('ascii'))
                    else:
                        self._logger.info("Defaulting to echo for message: " + message)
                        connection.send(data + "*".encode('ascii'))  # Default echo

                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")


class Client:
    """ The client """
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def call(self, msg_in):
        """ Call server """
        self.sock.send(msg_in.encode('ascii'))  # send encoded string as data
        data = self.sock.recv(1024)  # receive the response
        msg_out = data.decode('ascii')
        print(msg_out)  # print the result
        return msg_out
    
    def get(self, name):
        """Get a specific item by name"""
        request = f"GET {name}"  # Create request message
        self.sock.send(request.encode('ascii'))  # Send to server
        self.logger.info("Requested Phone Number for " + name)
        data = self.sock.recv(1024)  # Receive response
        response = data.decode('ascii')
        print(response)
        return response

    def getAll(self):
        """Get all items"""
        request = "GETALL"  # Simple request
        self.sock.send(request.encode('ascii'))  # Send to server
        self.logger.info("Requested All Phone Numbers")
        data = self.sock.recv(1024)  # Receive response
        response = data.decode('ascii')
        print(response)
        return response

    def close(self):
        """ Close socket """
        self.sock.close()
        self.logger.info("Client down.")
