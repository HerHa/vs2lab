"""
Client and server using classes
"""

import logging
import socket

import const_cs
from context import lab_logging

# init loging channels for the lab
if not logging.getLogger().hasHandlers():
    lab_logging.setup(stream_level=logging.INFO)

# pylint: disable=logging-not-lazy, line-too-long

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever

        self.tel_db = {"Alice": 123, "Bob": 456, "Charlie": 789}  # lab1: example telephone number database

        self._logger.info("Server bound to socket " + str(self.sock))

    def get(self, name): # lab 1: search name in database and return number if found, else NOTFOUND
        """ Get number for name from database """
        if name in self.tel_db:
            return "OK " + name + " " + str(self.tel_db[name])
        else:
            return "NOTFOUND " + name
        
    def getAll(self): # lab 1:return all entries in database, one per line, starting with OK and ending with END
        """ Get all entries from database """
        response = "OK\n"
        for name, number in self.tel_db.items():
            response += name + " " + str(number) + "\n"
        response += "END"
        return response  

    def call(self): # lab 1: return "Hello, world" as response
        """ Call echo """
        return "Hello, world" + "*"  

    def handle_request(self, request): # lab 1:differentiate between GET and GETALL requests, call the appropriate method and return the result
        parts = request.strip().split()

        if parts[0] == "GET":
            if len(parts) != 2:
                return "ERROR GET requires exactly one argument"
            return self.get(parts[1])

        elif parts[0] == "GETALL":
            return self.getAll()
        
        else:
            return "ERROR Unknown command"        

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
                    # lab 1
                    request = data.decode("ascii") # decode data to string
                    self._logger.info("Received: " + request) # log the request

                    response = self.handle_request(request) # handle the request and get the response
                    if response.startswith("ERROR"):
                        self._logger.error("Initiate Default echo:") # log errors
                        connection.send(data + "*".encode('ascii'))  # return sent data plus an "*"
                    else:
                        connection.send(response.encode("ascii")) # send the response back to the client

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

    def call(self, msg_in="Hello, world"):
        """ Call server """
        self.sock.send(msg_in.encode('ascii'))  # send encoded string as data
        data = self.sock.recv(1024)  # receive the response
        msg_out = data.decode('ascii')
        print(msg_out)  # print the result
        self.sock.close()  # close the connection
        self.logger.info("Client down.")
        return msg_out
    
    def get(self, name): # lab 1: send GET request for name and print the response
        request = "GET " + name
        self.sock.send(request.encode("ascii"))
        self.logger.info("Sent: " + request)

        response = self.sock.recv(1024).decode("ascii")
        print(response)
        self.logger.info("Received: " + response)
        self.sock.close()
        self.logger.info("Client down.")
        return response

    def getall(self): #lab 1: send GETALL request and print the response
        request = "GETALL"
        self.sock.send(request.encode("ascii"))
        self.logger.info("Sent: " + request)

        response = self.sock.recv(1024).decode("ascii")
        print(response)
        self.logger.info("Received: " + response)
        self.sock.close()
        self.logger.info("Client down.")
        return response

    def close(self):
        """ Close socket """
        self.sock.close()
