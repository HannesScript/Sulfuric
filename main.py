import re
import asyncio
import os
import json
from threading import Lock

class Request:
    __slots__ = ('method', 'path', 'headers', 'body')
    def __init__(self, method, path, headers, body):
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body
        self.bodyJson = json.loads(body) if body else {}

class Response:
    __slots__ = ('writer',)
    def __init__(self, writer):
        self.writer = writer

    async def send(self, body, status_code=200, content_type="text/plain"):
        response_line = f"HTTP/1.1 {status_code} OK\r\n"
        headers = f"Content-Type: {content_type}\r\nContent-Length: {len(body.encode('utf-8'))}\r\n\r\n"
        self.writer.write((response_line + headers + body).encode('utf-8'))
        await self.writer.drain()
        self.writer.close()

    async def sendFile(self, file_path, content_type="text/plain"):
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            response_line = f"HTTP/1.1 200 OK\r\n"
            headers = f"Content-Type: {content_type}\r\nContent-Length: {len(content)}\r\n\r\n"
            self.writer.write(response_line.encode('utf-8'))
            self.writer.write(headers.encode('utf-8'))
            self.writer.write(content)
            await self.writer.drain()
        except FileNotFoundError:
            await self.send("File not found", status_code=404)
        finally:
            self.writer.close()
            
    async def sendJson(self, data):
        await self.send(json.dumps(data), content_type="application/json")

class Server:
    def __init__(self):
        self.port = 8080
        self.routes = []

    def on(self, path="/", method="GET"):
        def decorator(func):
            pattern = re.sub(r"\[(\w+)\]", r"(?P<\1>[^/]+)", path)
            pattern = "^" + pattern + "$"
            regex = re.compile(pattern)
            self.routes.append((method, regex, func))
            return func
        return decorator

    async def __handle_client(self, reader, writer):
        data = await reader.read(65536)
        if not data:
            writer.close()
            return
        request_line, headers, body = self.__parse_request(data.decode('utf-8', 'ignore'))
        parts = request_line.split(" ")
        if len(parts) < 3:
            writer.close()
            return
        method, path = parts[0], parts[1]
        req = Request(method, path, headers, body)
        res = Response(writer)

        for m, regex, handler in self.routes:
            if m == method:
                match = regex.match(path)
                if match:
                    await handler(req, res, **match.groupdict())
                    return

        await res.send("404 Not Found", status_code=404)

    def __parse_request(self, request_text):
        lines = request_text.split("\r\n")
        request_line = lines[0] if lines else ""
        headers = {}
        i = 1
        while i < len(lines) and lines[i]:
            if ": " in lines[i]:
                key, value = lines[i].split(": ", 1)
                headers[key] = value
            i += 1
        body = "\r\n".join(lines[i+1:])
        return request_line, headers, body

    def listen(self, port=8080):
        self.port = port
        print(f"Server running on port {self.port}")
        async def start_server():
            server = await asyncio.start_server(self.__handle_client, '0.0.0.0', self.port)
            async with server:
                await server.serve_forever()
        asyncio.run(start_server())

class Database:
    def __init__(self, db_path="./db/"):
        self.base_path = db_path
        os.makedirs(self.base_path, exist_ok=True)
        self.tables = {}
        self.lock = Lock()
        self.__load_tables()

    def __load_tables(self):
        for table_name in os.listdir(self.base_path):
            table_path = os.path.join(self.base_path, table_name)
            if os.path.isdir(table_path):
                index_file = os.path.join(table_path, 'index.json')
                if os.path.exists(index_file):
                    with open(index_file, 'r') as f:
                        self.tables[table_name] = json.load(f)
                else:
                    self.tables[table_name] = {}
                    with open(index_file, 'w') as f:
                        json.dump({}, f)

    def __save_index(self, table):
        index_file = os.path.join(self.base_path, table, 'index.json')
        with open(index_file, 'w') as f:
            json.dump(self.tables[table], f)

    def set(self, table, key, value):
        with self.lock:
            table_path = os.path.join(self.base_path, table)
            keys_path = os.path.join(table_path, 'keys')
            os.makedirs(keys_path, exist_ok=True)
            file_path = os.path.join(keys_path, f"{key}.json")
            with open(file_path, 'w') as f:
                json.dump(value, f)
            if table not in self.tables:
                self.tables[table] = {}
            self.tables[table][key] = file_path
            self.__save_index(table)

    def get(self, table, key):
        with self.lock:
            if table not in self.tables:
                return None
            file_path = self.tables[table].get(key)
            if not file_path or not os.path.exists(file_path):
                return None
            with open(file_path, 'r') as f:
                return json.load(f)

    def delete(self, table, key):
        with self.lock:
            if table in self.tables:
                file_path = self.tables[table].pop(key, None)
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    self.__save_index(table)

    def all_keys(self, table):
        if table in self.tables:
            return list(self.tables[table].keys())
        return []