import socket
import threading
import time
import random
import sys
import json
from operator import truediv
from typing import final

PEERS = {
    1: ('process1', 5001),
    2: ('process1', 5002),
    3: ('process1', 5003)
}

class DistribuitedProcess:

    def __init__(self, process_id: int):
        if process_id not in PEERS:
            raise ValueError(f"Process id invalid: {process_id}")

        self.id = process_id
        self.host, self.port = PEERS[process_id]

        self.others_peers = {pid:addr for pid, addr in PEERS.items() if pid != self.id}

        self.lamport_clock = 0
        self.local_state = 0
        self.lock = threading.Lock()

        self.is_capturing = False
        self.snapshot_state = None
        self.received_markers_from = {peer_id:False for peer_id in self.others_peers}
        self.in_transit_messages = {}

    def _log(self, message: str):
        print(f"[PEER{self.id} | Clock: {self.lamport_clock} | State: {self.local_state}] {message}", flush=True)

    def start(self):
        self._log("Process start")

        server_thread = threading.Thread(target=self._listen_for_messages)
        server_thread.daemon = True
        server_thread.start()

        time.sleep(5)

    def _listen_for_messages(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(len(PEERS))
        self._log(f"Listening {self.host}:{self.port}")

        while True:
            conn, addr = server_socket.accept()
            handler_thread = threading.Thread(target=self._handle_connection, args=(conn,))
            handler_thread.daemon = True
            handler_thread.start()

    def _handle_connection(self, connection : socket.socket):
        try:
            while True:
                data = connection.recv(1024)
                if not data:
                    break

                for messages in data.decode('utf-8').strip().split('\n'):
                    if messages:
                        self._process_received_message(messages)

        except ConnectionResetError:
            self._log("Connection Lost")
        finally:
            connection.close()

    def _send_message(self, target_id : int, message_type : str, content : str = ""):
        with self.lock:
            self.lamport_clock +=1
            self._log(f"Sending '{message_type}' for {target_id}")

            full_message = f"{self.lamport_clock}:{self.id}:{message_type}:{content}\n"

            try:
                target_host, target_port = PEERS[target_id]
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((target_host, target_port))
                client_socket.sendall(full_message.encode('utf-8'))

            except ConnectionRefusedError:
                self._log(f"Erro conecting with p{target_id}")
            except Exception as e:
                self._log(f"Erro sending message: {str(e)}")
            finally:
                client_socket.close()

    def _process_received_message(self, message : str):
        parts = message.strip().split(':',3)

        ts_received, origin_id, mesage_type, content = int(parts[0]), int(parts[1]), parts[2], parts[3]

        with self.lock:
            self.lamport_clock = max(self.lamport_clock, ts_received) + 1
            self._log(f"Received '{mesage_type}' from P{origin_id} [TS: {ts_received}]")

            if mesage_type == 'MARKER':
                if not self.is_capturing:
                    self._start_snapshot_procedure(initiated_by_marker=True)
                self.received_markers_from[origin_id] = True
            else:
                if self.is_capturing and not self.received_markers_from.get(origin_id, True):
                    if origin_id not in self.in_transit_messages:
                        self.in_transit_messages[origin_id] = []

                    self.in_transit_messages[origin_id].append(message)
                    self._log(f"Message from P{origin_id} in transit")

                    if self.is_capturing and all(self.received_markers_from.values()):
                        self._finalize_snapshot()

    def _start_snapshot_procedure(self, initiated_by_marker=False):
        self.is_capturing = True
        self.snapshot_state = self.local_state
        self.in_transit_messages = {}

        for peer_id in self.others_peers:
            self.received_markers_from[peer_id] = False

        if initiated_by_marker:
            self._log("SNAPSHOT INITIATED FROM MARKER")
        else:
            self._log("SNAPSHOT INITIATED AS MARKER")

        threading.Thread(target=self._broadcast_markers).start()

    def _internal_event(self):
        with self.lock:
            self.lamport_clock += 1
            self.local_state += 1
            self._log("Execute internal event")

    def _broadcast_marker(self):
        for peer_id in self.others_peers:
            self._send_message(peer_id, 'MARKER')

    def _finalize_snapshot(self):