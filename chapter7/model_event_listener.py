import asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer

import os, json, httpx

url = os.getenv("TARGET_SERVER_URL") or "http://127.0.0.1:8080"
loop = asyncio.get_event_loop()


async def outlier_call(payload, request_headers):
    print("asdadasd")
    async with httpx.AsyncClient() as client:
        print("Calling")
        response = await client.post(url, json=payload)
        print(f"response for {payload} is {response} with data {response.text}")


class SeldonCoreHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            request_headers = self.headers
            length = int(request_headers['content-length'])
            print(f"length is {length}")

            payload = json.loads(self.rfile.read(length))
            print(f"payload is {payload}")

            future = asyncio.ensure_future(outlier_call(payload, request_headers), loop=loop)
            loop.run_until_complete(future)

        except Exception as ex:
            print(ex)
        finally:
            self.send_response(200)
            self.wfile.flush()

        return


def main():
    server_address = ('127.0.0.1', 8081)
    httpd = HTTPServer(server_address, SeldonCoreHTTPRequestHandler)
    print('http forwarding server is running')
    httpd.serve_forever()


if __name__ == '__main__':
    main()
