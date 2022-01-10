import asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer

import os, json, httpx

url = os.getenv("TARGET_SERVER_URL") or "http://127.0.0.1:8080"
loop = asyncio.get_event_loop()


async def call_outlier_model(payload, request_headers):
    """ This method makes a call to another model, in our case the outlier detector, and prints the response
        In a production scenario, instead of printing the response, if an outlier has been detected this method
        can trigger an event which can then be handled as needed
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        print(f"response for {payload} is {response} with data {response.text}")


class SeldonCoreHTTPRequestHandler(BaseHTTPRequestHandler):
    """ A simple HTTP server which will receive the input payload from Seldon core. The server will respond immediately as HTTP.200
        It will then call the outlier model to record if there is an outlier payload is detected. Right now the async calls are managed in memory,
        however, in a production scenario, an event bus would be a better choice.
    """
    def do_POST(self):
        try:
            request_headers = self.headers
            length = int(request_headers['content-length'])
            print(f"length is {length}")

            payload = json.loads(self.rfile.read(length))
            print(f"payload is {payload}")

            future = asyncio.ensure_future(call_outlier_model(payload, request_headers), loop=loop)
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
