from vanity_address.vanity_address import VanityAddressGenerator
from flask import Flask, request, make_response, jsonify
from threading import Thread
import requests
import time

app = Flask(__name__, subdomain_matching=True)
words = []

def callback(address):
    return address.startswith(bytes(words[0][0],'UTF-8'))

@app.route("/generate", methods=['GET'])
async def generateAddress():
    if not "word" in request.args.keys(): return make_response(jsonify({'error': 'Undefined word'}), 400)
    words.append(request.args.get("word"))
    return jsonify({"status": "generate"})

def generate():
    while True:
        if words:
            address = VanityAddressGenerator.generate_one(callback=callback)
            words.remove(words[0][0])
            response = ''
            while response.lower() != 'ok':
                response = requests.get(f"https://vanitygen.net/handler/key.php?word={words[0][0]}&address={address.address}&key={address.private_key}").text
                print(response)
                time.sleep(0.5)

if __name__ == "__main__":
    Thread(target=generate).start()
    app.run(host="0.0.0.0", port=80)