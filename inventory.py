import sys
from api import api
from waitress import serve
if __name__ == "__main__":
    host = sys.argv[1]
    print(host)
    print("\nCTRL+C to close.")
    serve(api, listen=host)